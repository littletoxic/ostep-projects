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

static void Init_Store(int num_partitions) {
  store.partition_count = num_partitions;
  store.parts = malloc(sizeof(Part) * num_partitions);
  assert(store.parts != NULL);

  for (int i = 0; i < num_partitions; i++) {
    // init each partition
    assert(pthread_mutex_init(&store.parts[i].lock, NULL) == 0);
    store.parts[i].capacity = DEFAULT_LIST_CAPACITY;
    store.parts[i].len = 0;
    Key_With_Value_List *keys;
    keys = malloc(sizeof(Key_With_Value_List) * DEFAULT_LIST_CAPACITY);
    assert(keys != NULL);
    store.parts[i].keys = keys;
  }
}

static int Found_Key(char *key, unsigned long pos_p) {
  for (int i = 0; i < store.parts[pos_p].len; i++) {
    if (strcmp(key, store.parts[pos_p].keys[i].key) == 0) {
      return i;
    }
  }
  return -1;
}

static inline void Ensure_Keys_Capacity(Part *part) {
  if (part->len == part->capacity) {
    part->capacity *= 2;
    part->keys =
        realloc(part->keys, sizeof(Key_With_Value_List) * part->capacity);
    assert(part->keys != NULL);
  }
}

static inline void Ensure_Values_Capacity(Key_With_Value_List *list) {
  if (list->len == list->capacity) {
    list->capacity *= 2;
    list->values = realloc(list->values, sizeof(char *) * list->capacity);
    assert(list->values != NULL);
  }
}

static inline void Insert_Key_To_Partition(Part *part, char *key, int pos_k) {
  Key_With_Value_List *key_with_list = &part->keys[pos_k];
  key_with_list->key = strdup(key);
  assert(key_with_list->key != NULL);
  key_with_list->capacity = DEFAULT_LIST_CAPACITY;
  key_with_list->len = 0;
  key_with_list->values = malloc(sizeof(char *) * DEFAULT_LIST_CAPACITY);
  assert(key_with_list->values != NULL);
}

static inline void Insert_Value(Key_With_Value_List *list, char *value) {
  int pos_v = list->len++;
  list->values[pos_v] = strdup(value);
  assert(list->values[pos_v] != NULL);
}

void MR_Emit(char *key, char *value) {
  assert(key != NULL);
  assert(value != NULL);
  unsigned long pos_p = p(key, store.partition_count);
  int pos_k;

  assert(pthread_mutex_lock(&store.parts[pos_p].lock) == 0);

  if ((pos_k = Found_Key(key, pos_p)) == -1) {
    Ensure_Keys_Capacity(&store.parts[pos_p]);

    pos_k = store.parts[pos_p].len++;
    Insert_Key_To_Partition(&store.parts[pos_p], key, pos_k);
  }

  Ensure_Values_Capacity(&store.parts[pos_p].keys[pos_k]);
  Insert_Value(&store.parts[pos_p].keys[pos_k], value);

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
  pool->queue.insert = 0;
  pool->queue.use = 0;
  pool->queue.exit_flag = 0;
  assert(pthread_mutex_init(&pool->queue.mutex, NULL) == 0);
  assert(pthread_cond_init(&pool->queue.fill, NULL) == 0);
  assert(pthread_cond_init(&pool->queue.empty, NULL) == 0);
}

static void Init_Thread_Pool(int capacity) {
  pool = malloc(sizeof(Mapper_Thread_Pool));
  assert(pool != NULL);
  pool->capacity = capacity;
  pool->created = 0;
  pool->threads = malloc(capacity * sizeof(pthread_t));
  assert(pool->threads != NULL);

  // consumer and producer
  Init_Pool_Queue(pool);
}

/* 元素交换 */
static inline void swap(Key_With_Value_List keys[], int i, int j) {
  Key_With_Value_List tmp = keys[i];
  keys[i] = keys[j];
  keys[j] = tmp;
}

/* 哨兵划分 */
static int partition(Key_With_Value_List keys[], int left, int right) {
  // 以 keys[left] 为基准数
  int i = left, j = right;
  while (i < j) {
    while (i < j && strcmp(keys[j].key, keys[left].key) >= 0) {
      j--; // 从右向左找首个小于基准数的元素
    }
    while (i < j && strcmp(keys[j].key, keys[left].key) <= 0) {
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
static void quickSort(Key_With_Value_List keys[], int left, int right) {
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

static void Sort_Keys(Part *part) { quickSort(part->keys, 0, part->len - 1); }

static char *Get_Func(char *key, int partition_number) {
  int iter_k = store.parts[partition_number].iter;
  Key_With_Value_List *k = &store.parts[partition_number].keys[iter_k];
  if (k->iter == k->len) {
    return NULL;
  }
  return k->values[k->iter++];
}

static void *Reducer_Thread(void *p_n) {
  unsigned long partition_number = (unsigned long)p_n;
  Part *part = &store.parts[partition_number];

  part->iter = 0;
  for (int i = 0; i < part->len; i++) {

    // init iter used in Get_Func
    part->keys[i].iter = 0;

    reducer(part->keys[i].key, Get_Func, partition_number);
    part->iter++;
  }

  // release resources in part
  for (int i = 0; i < part->len; i++) {
    for (int j = 0; j < part->keys[i].len; j++) {
      // free each saved value
      free(part->keys[i].values[j]);
    }
    // free key
    free(part->keys[i].key);
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
  // 2. sort
  for (int i = 0; i < num_reducers; i++) {
    Sort_Keys(&store.parts[i]);
  }
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