#include "mapreduce.h"
#include "mrtypes.h"
#include <assert.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

#define _POSIX_C_SOURCE 200809L

static Store store;
static Mapper mapper;
static Mapper_Thread_Pool *pool;
static Partitioner p;
static Reducer reducer;

static unsigned int Hash_Func(char *key) {
  unsigned int hash = 5381;
  unsigned int c;
  while ((c = *key++) != '\0') {
    c *= 0x5bd1e995;
    hash *= 0x5bd1e995;
    hash ^= c;
  }

  hash ^= hash >> 13;
  hash *= 0x5bd1e995;
  hash ^= hash >> 15;

  return hash;
}

static Collection *Create_Collection(bool list) {
  Collection *c = malloc(sizeof(Collection));
  assert(c != NULL);
  c->len = 0;
  c->capacity = DEFAULT_LIST_CAPACITY;
  if (list) {
    c->datas = malloc(sizeof(void *) * DEFAULT_LIST_CAPACITY);
  } else {
    // HashMap 操作需要判断数据是否为空
    c->datas = calloc(DEFAULT_LIST_CAPACITY, sizeof(void *));
  }
  assert(c->datas != NULL);
  return c;
}

static void Init_Part(Part *part) {
  part->key_with_values_map = Create_Collection(false);
  part->string_pool = Create_Collection(false);
  assert(pthread_mutex_init(&part->lock, NULL) == 0);
}

static void Init_Store(int num_partitions) {
  store.partition_count = num_partitions;
  store.parts = malloc(sizeof(Part) * num_partitions);
  assert(store.parts != NULL);

  for (int i = 0; i < num_partitions; i++) {
    // init each partition
    Init_Part(&store.parts[i]);
  }
}

static char *Key_With_Values_Selector(void *data) {
  Key_With_Values *kvs = (Key_With_Values *)data;
  return kvs->key;
}

static char *String_Pool_Selector(void *data) { return (char *)data; }

static int Get_Index(char *key, Collection *map, Key_Selector selector) {
  unsigned int index = Hash_Func(key) % map->capacity;

  // 线性探测，不考虑删除
  while (map->datas[index] != NULL) {
    if (strcmp(key, selector(map->datas[index])) == 0) {
      return index;
    }
    // 环形
    index = (index + 1) % map->capacity;
  }

  return index;
}

static inline double Get_Load_Factor(Collection *map) {
  return (double)map->len / (double)map->capacity;
}

static void Insert_To_Map(Collection *map, void *value, int pos) {
  map->datas[pos] = value;
  map->len++;
}

static void Extend(Collection *map, Key_Selector selector) {
  void **origin = map->datas;
  int old_capacity = map->capacity;
  // 扩容 4 倍
  map->capacity *= 4;
  map->len = 0;
  map->datas = calloc(map->capacity, sizeof(void *));

  for (int i = 0; i < old_capacity; i++) {
    if (origin[i] != NULL) {
      int index = Get_Index(selector(origin[i]), map, selector);
      assert(map->datas[index] == NULL);
      Insert_To_Map(map, origin[i], index);
    }
  }
  free(origin);
}

// true presents extended, otherwise false
static bool Ensure_Map_Capacity(Collection *map, Key_Selector pred) {
  if (Get_Load_Factor(map) > LOAD_FACTOR) {
    Extend(map, pred);
    return true;
  }
  return false;
}

static inline void Ensure_List_Capacity(Collection *list) {
  if (list->len == list->capacity) {
    list->capacity *= 2;
    list->datas = realloc(list->datas, sizeof(void *) * list->capacity);
    assert(list->datas != NULL);
  }
}

static void Insert_To_List(Collection *list, void *value) {
  Ensure_List_Capacity(list);
  int pos_v = list->len++;
  list->datas[pos_v] = value;
}

static Key_With_Values *Create_Key_With_Values(char *key) {
  char *key_copy = strdup(key);
  assert(key_copy != NULL);

  Key_With_Values *key_with_values = malloc(sizeof(Key_With_Values));
  assert(key_with_values != NULL);
  key_with_values->key = key_copy;
  key_with_values->values_array = Create_Collection(true);

  return key_with_values;
}

static void *KVProd(char *key) { return (void *)Create_Key_With_Values(key); }

static void *Get_Exist_Otherwise_New(char *key, Collection *map,
                                     Producer producer, Key_Selector selector) {
  int index = Get_Index(key, map, selector);
  if (map->datas[index] == NULL) {
    if (Ensure_Map_Capacity(map, selector) == true) {
      index = Get_Index(key, map, selector);
    }
    void *new_value = producer(key);
    Insert_To_Map(map, new_value, index);
  }
  return map->datas[index];
}

static void *String_Pool_Prod(char *value) {
  char *value_copy = strdup(value);
  assert(value_copy != NULL);
  return (void *)value_copy;
}

void MR_Emit(char *key, char *value) {
  assert(key != NULL);
  assert(value != NULL);
  unsigned long pos_p = p(key, store.partition_count);
  Part *part = &store.parts[pos_p];
  assert(pthread_mutex_lock(&part->lock) == 0);

  Key_With_Values *kv = Get_Exist_Otherwise_New(
      key, part->key_with_values_map, KVProd, Key_With_Values_Selector);

  char *value_in_pool = Get_Exist_Otherwise_New(
      value, part->string_pool, String_Pool_Prod, String_Pool_Selector);

  Insert_To_List(kv->values_array, value_in_pool);

  assert(pthread_mutex_unlock(&store.parts[pos_p].lock) == 0);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
  unsigned long hash = 5381;
  int c;
  while ((c = *key++) != '\0')
    hash = hash * 33 + c;
  return hash % num_partitions;
}

static void Wait_And_Destory_Pool() {
  assert(pthread_mutex_lock(&pool->queue.mutex) == 0);
  pool->queue.exit_flag = 1;
  assert(pthread_cond_broadcast(&pool->queue.fill) == 0);

  assert(pthread_mutex_unlock(&pool->queue.mutex) == 0);
  for (int i = 0; i < pool->created; i++) {
    assert(pthread_join(pool->threads[i], NULL) == 0);
  }

  assert(pthread_cond_destroy(&pool->queue.fill) == 0);
  assert(pthread_cond_destroy(&pool->queue.empty) == 0);
  assert(pthread_mutex_destroy(&pool->queue.mutex) == 0);
  free(pool->threads);
  free(pool);
}

static void *Mapper_Thread_Loop(void *args) {

  mapper(args);

  while (1) {
    char *file_name;
    assert(pthread_mutex_lock(&pool->queue.mutex) == 0);
    // queue is empty
    while (pool->queue.count == 0) {
      // no more element
      if (pool->queue.exit_flag == 1) {
        assert(pthread_mutex_unlock(&pool->queue.mutex) == 0);
        pthread_exit(NULL);
      }
      assert(pthread_cond_wait(&pool->queue.fill, &pool->queue.mutex) == 0);
    }
    file_name = pool->queue.buffer[pool->queue.use];
    pool->queue.use = (pool->queue.use + 1) % QUEUE_MAX;
    pool->queue.count--;
    assert(pthread_cond_signal(&pool->queue.empty) == 0);
    assert(pthread_mutex_unlock(&pool->queue.mutex) == 0);

    mapper(file_name);
  }
}

static void Run_Task(char *file_name) {
  if (pool->created == pool->capacity) {

    // add file name to queue
    assert(pthread_mutex_lock(&pool->queue.mutex) == 0);
    while (pool->queue.count == QUEUE_MAX) {
      assert(pthread_cond_wait(&pool->queue.empty, &pool->queue.mutex) == 0);
    }
    pool->queue.buffer[pool->queue.insert] = file_name;
    pool->queue.insert = (pool->queue.insert + 1) % QUEUE_MAX;
    pool->queue.count++;
    assert(pthread_cond_signal(&pool->queue.fill) == 0);
    assert(pthread_mutex_unlock(&pool->queue.mutex) == 0);

  } else {
    // lazy create threads
    assert(pthread_create(&pool->threads[pool->created++], NULL,
                          Mapper_Thread_Loop, file_name) == 0);
  }
}

static inline void Init_Pool_Queue() {
  pool->queue.count = 0;
  pool->queue.use = 0;
  pool->queue.insert = 0;
  pool->queue.exit_flag = 0;
  assert(pthread_cond_init(&pool->queue.empty, NULL) == 0);
  assert(pthread_cond_init(&pool->queue.fill, NULL) == 0);
  assert(pthread_mutex_init(&pool->queue.mutex, NULL) == 0);
}

static void Init_Thread_Pool(int capacity) {
  pool = malloc(sizeof(Mapper_Thread_Pool));
  assert(pool != NULL);

  pool->threads = malloc(capacity * sizeof(pthread_t));
  assert(pool->threads != NULL);
  pool->capacity = capacity;
  pool->created = 0;

  // consumer and producer
  Init_Pool_Queue(pool);
}

/* 元素交换 */
static inline void swap(Key_With_Values *keys[], int i, int j) {
  Key_With_Values *tmp = keys[i];
  keys[i] = keys[j];
  keys[j] = tmp;
}

/* 哨兵划分 */
static int partition(Key_With_Values *keys[], int left, int right) {
  // 以 keys[left] 为基准数
  int i = left, j = right;
  while (i < j) {
    while (i < j && strcmp(keys[j]->key, keys[left]->key) >= 0) {
      j--; // 从右向左找首个小于基准数的元素
    }
    while (i < j && strcmp(keys[i]->key, keys[left]->key) <= 0) {
      i++; // 从左向右找首个大于基准数的元素
    }
    // 交换这两个元素
    swap(keys, i, j);
  }
  // 将基准数交换至两子数组的分界线
  swap(keys, i, left);
  // 返回基准数的索引
  return i;
}

/* 快速排序 */
static void quickSort(Key_With_Values *keys[], int left, int right) {
  // 子数组长度为 1 时终止递归
  if (left >= right) {
    return;
  }
  // 哨兵划分
  int pivot = partition(keys, left, right);
  // 递归左子数组、右子数组
  quickSort(keys, left, pivot - 1);
  quickSort(keys, pivot + 1, right);
}

static char *Get_Func(char *key, int partition_number) {
  int iter_k = store.parts[partition_number].iter;
  Key_With_Values *k =
      store.parts[partition_number].key_with_values_map->datas[iter_k];
  if (k->iter == k->values_array->len) {
    return NULL;
  }
  return k->values_array->datas[k->iter++];
}

static void Compact(Collection *map) {
  int now = 0;
  for (int i = 0; i < map->capacity; i++) {
    if (map->datas[i] != NULL) {
      map->datas[now] = map->datas[i];
      now++;
    }
    if (now == map->len) {
      break;
    }
  }
}

static void Destory_String_Pool_List(Collection *list) {
  for (int i = 0; i < list->capacity; i++) {
    if (list->datas[i] != NULL) {
      free(list->datas[i]);
    }
  }
  free(list->datas);
  free(list);
}

static void Destory_Key_With_Values(Key_With_Values *kv) {
  free(kv->key);
  // don't need to do, each value string is in string pool
  // Destory_Simple_List(kv->values_array);
  free(kv->values_array->datas);
  free(kv->values_array);
  free(kv);
}

static void *Reducer_Thread(void *p_n) {
  unsigned long partition_number = (unsigned long)p_n;
  Part *part = &store.parts[partition_number];
  Compact(part->key_with_values_map);
  // sort in parallel
  quickSort((Key_With_Values **)part->key_with_values_map->datas, 0,
            part->key_with_values_map->len - 1);

  part->iter = 0;
  for (int i = 0; i < part->key_with_values_map->len; i++) {
    // init iter used in Get_Func
    Key_With_Values *now =
        (Key_With_Values *)part->key_with_values_map->datas[i];
    now->iter = 0;

    reducer(now->key, Get_Func, partition_number);
    part->iter++;
  }

  // release resources in part
  // Destory key_with_values_map
  for (int i = 0; i < part->key_with_values_map->len; i++) {
    Destory_Key_With_Values(part->key_with_values_map->datas[i]);
  }
  free(part->key_with_values_map->datas);
  free(part->key_with_values_map);

  // Destory string_pool
  Destory_String_Pool_List(part->string_pool);

  assert(pthread_mutex_destroy(&part->lock) == 0);

  return NULL;
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce,
            int num_reducers, Partitioner partition) {
  // 全局变量
  mapper = map;
  reducer = reduce;
  p = partition;
  // 1.1 init thread pool
  Init_Thread_Pool(num_mappers);
  // 1.2 init store
  Init_Store(num_reducers);
  // 1.2 run mapper in threads
  for (int i = 1; i < argc; i++) {
    Run_Task(argv[i]);
  }
  // wait all threads finished
  // 1.3 destory thread pool
  Wait_And_Destory_Pool();
  // 2. sort (now in Reducer_Thread)

  // 3. reduce
  pthread_t *threads = malloc(sizeof(pthread_t) * num_reducers);
  assert(threads != NULL);
  for (int i = 0; i < num_reducers; i++) {
    assert(pthread_create(&threads[i], NULL, Reducer_Thread,
                          (void *)(unsigned long)i) == 0);
  }

  for (int i = 0; i < num_reducers; i++) {
    assert(pthread_join(threads[i], NULL) == 0);
  }
  free(threads);
  free(store.parts);
}