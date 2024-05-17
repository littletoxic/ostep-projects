#ifndef __mrtypes_h__
#define __mrtypes_h__

#include <pthread.h>

#define QUEUE_MAX (1 << 7)
#define DEFAULT_LIST_CAPACITY (1 << 15)
#define LOAD_FACTOR 0.4

typedef struct Wait_Queue {
  char *buffer[QUEUE_MAX];
  int count, use, insert, exit_flag;
  pthread_cond_t empty, fill;
  pthread_mutex_t mutex;
} Wait_Queue;

typedef struct Mapper_Thread_Pool {
  pthread_t *threads;
  int capacity;
  int created;
  Wait_Queue queue;
} Mapper_Thread_Pool;

// 作为 HashMap 时不可删除
typedef struct Collection {
  int len;
  int capacity;
  void **datas;
} Collection;

typedef struct Key_With_Values {
  char *key;
  // values 数组
  Collection *values_array;
  // 遍历时初始化
  int iter;
} Key_With_Values;

typedef struct Part {
  // Key_With_Values HashMap
  Collection *key_with_values_map;
  // 字符串池
  Collection *string_pool;
  // each part needs a lock
  pthread_mutex_t lock;
  // 遍历时初始化
  int iter;
} Part;

typedef struct Store {
  int partition_count;
  Part *parts;
} Store;

typedef enum { false, true } bool;

typedef char * (*Key_Selector)(void *data);

typedef void *(*Producer)(char *key);

#endif // __mrtypes_h__