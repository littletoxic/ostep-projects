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

static void Init_Store(int num_partitions) {
  store.partition_count = num_partitions;
  store.parts = malloc(sizeof(Part) * num_partitions);
  assert(store.parts != NULL);

  for (int i = 0; i < num_partitions; i++) {
    assert(pthread_mutex_init(&store.parts[i].lock, NULL) == 0);
    store.parts[i].capacity = DEFAULT_LIST_CAPACITY;
    store.parts[i].len = 0;
    Key_With_Value_List *values;
    values = malloc(sizeof(Key_With_Value_List) * store.parts[i].capacity);
    assert(values != NULL);
    store.parts[i].keys = values;
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
  part->keys[pos_k].key = strdup(key);
  assert(part->keys[pos_k].key != NULL);
  part->keys[pos_k].values =
      malloc(sizeof(char *) * part->keys[pos_k].capacity);
  assert(part->keys[pos_k].values != NULL);
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

    pos_k = store.parts[pos_p].keys[pos_k].len++;
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

void *Thread_Loop(void *args) {

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

void Run_Task(char *file_name) {
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
    pthread_create(&pool->threads[pool->created++], NULL, Thread_Loop,
                   file_name);
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

void Init_Thread_Pool(int capacity) {
  pool = malloc(sizeof(Mapper_Thread_Pool));
  assert(pool != NULL);
  pool->capacity = capacity;
  pool->created = 0;
  pool->threads = malloc(capacity * sizeof(pthread_t));
  assert(pool->threads != NULL);

  // consumer and producer
  Init_Pool_Queue(pool);
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce,
            int num_reducers, Partitioner partition) {
  // 1. map
  mapper = map;
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

  // 3. reduce
}