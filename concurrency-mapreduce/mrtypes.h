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

// 使用开放寻址线性探测，不需要考虑删除问题
typedef struct Key_With_Values {
  char *key;
  char **values;
  int len;
  int capacity;
  // 遍历时初始化
  int iter;
} Key_With_Values;

typedef struct Part {
  int len;
  int capacity;
  Key_With_Values **keys;
  // each part needs a lock
  pthread_mutex_t lock;
  // 遍历时初始化
  int iter;
} Part;

typedef struct Store {
  int partition_count;
  Part *parts;
} Store;

#endif // __mrtypes_h__