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

static void Init_Part(Part *part) {
  part->len = 0;
  part->capacity = DEFAULT_LIST_CAPACITY;
  Key_With_Values **keys;
  // Find_Place 需要判空，使用 calloc
  keys = calloc(DEFAULT_LIST_CAPACITY, sizeof(Key_With_Values *));
  assert(keys != NULL);
  part->keys = keys;
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

static int Find_Place(char *key, Part *part) {
  unsigned int index = Hash_Func(key) % part->capacity;

  // 线性探测，不考虑删除
  while (part->keys[index] != NULL) {
    if (strcmp(key, part->keys[index]->key) == 0) {
      return index;
    }
    // 环形
    index = (index + 1) % part->capacity;
  }

  return index;
}

static inline double lf(Part *part) {
  return (double)part->len / (double)part->capacity;
}

static void Insert_Key_To_Partition(Part *part,
                                    Key_With_Values *key_with_values,
                                    int pos_k) {
  part->keys[pos_k] = key_with_values;
  part->len++;
}

static void Extend(Part *part) {
  Key_With_Values **origin = part->keys;
  int old_capacity = part->capacity;
  // 扩容 4 倍
  part->capacity *= 4;
  part->len = 0;
  part->keys = calloc(part->capacity, sizeof(Key_With_Values *));

  for (int i = 0; i < old_capacity; i++) {
    if (origin[i] != NULL) {
      int pos_k = Find_Place(origin[i]->key, part);
      assert(part->keys[pos_k] == NULL);
      Insert_Key_To_Partition(part, origin[i], pos_k);
    }
  }
  free(origin);
}

static int Ensure_Keys_Capacity(Part *part) {
  if (lf(part) > LOAD_FACTOR) {
    Extend(part);
    return 1;
  }
  return 0;
}

static inline void Ensure_Values_Capacity(Key_With_Values *list) {
  if (list->len == list->capacity) {
    list->capacity *= 2;
    list->values = realloc(list->values, sizeof(char *) * list->capacity);
    assert(list->values != NULL);
  }
}

// 按定义顺序填充
static void Key_With_Values_Init(Key_With_Values *key_with_values, char *key) {
  key_with_values->key = key;
  key_with_values->values = malloc(sizeof(char *) * DEFAULT_LIST_CAPACITY);
  assert(key_with_values->values != NULL);
  key_with_values->len = 0;
  key_with_values->capacity = DEFAULT_LIST_CAPACITY;
}

static void Insert_Value(Key_With_Values *list, char *value) {
  int pos_v = list->len++;
  list->values[pos_v] = strdup(value);
  assert(list->values[pos_v] != NULL);
}

Key_With_Values *Create_Key_With_Values(char *key) {
  char *key_copy = strdup(key);
  assert(key_copy != NULL);

  Key_With_Values *key_with_values = malloc(sizeof(Key_With_Values));
  assert(key_with_values != NULL);
  Key_With_Values_Init(key_with_values, key_copy);

  return key_with_values;
}

void MR_Emit(char *key, char *value) {
  assert(key != NULL);
  assert(value != NULL);
  unsigned long pos_p = p(key, store.partition_count);

  assert(pthread_mutex_lock(&store.parts[pos_p].lock) == 0);

  int pos_k = Find_Place(key, &store.parts[pos_p]);
  if (store.parts[pos_p].keys[pos_k] == NULL) {
    // insert new key
    if (Ensure_Keys_Capacity(&store.parts[pos_p]) == 1) {
      // 扩容后 pos_k 改变
      pos_k = Find_Place(key, &store.parts[pos_p]);
    }
    Key_With_Values *key_with_values = Create_Key_With_Values(key);

    Insert_Key_To_Partition(&store.parts[pos_p], key_with_values, pos_k);
  }

  Ensure_Values_Capacity(store.parts[pos_p].keys[pos_k]);
  Insert_Value(store.parts[pos_p].keys[pos_k], value);

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
  Key_With_Values *k = store.parts[partition_number].keys[iter_k];
  if (k->iter == k->len) {
    return NULL;
  }
  return k->values[k->iter++];
}

static void Compact(Part *part) {
  int now = 0;
  for (int i = 0; i < part->capacity; i++) {
    if (part->keys[i] != NULL) {
      part->keys[now] = part->keys[i];
      now++;
    }
    if (now == part->len) {
      break;
    }
  }
}

static void *Reducer_Thread(void *p_n) {
  unsigned long partition_number = (unsigned long)p_n;
  Part *part = &store.parts[partition_number];
  Compact(part);
  // sort in parallel
  quickSort(part->keys, 0, part->len - 1);

  part->iter = 0;
  for (int i = 0; i < part->len; i++) {
    // init iter used in Get_Func
    part->keys[i]->iter = 0;

    reducer(part->keys[i]->key, Get_Func, partition_number);
    part->iter++;
  }

  // release resources in part
  for (int i = 0; i < part->len; i++) {
    for (int j = 0; j < part->keys[i]->len; j++) {
      // free each saved value
      free(part->keys[i]->values[j]);
    }
    // free key
    free(part->keys[i]->key);
    // free values array
    free(part->keys[i]->values);
    // free Key_With_Values struct
    free(part->keys[i]);
  }
  // free Key_With_Value_List struct array
  free(part->keys);

  assert(pthread_mutex_destroy(&part->lock) == 0);

  return NULL;
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce,
            int num_reducers, Partitioner partition) {
  // 1. map
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
  // 2. sort (now in parallel)

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