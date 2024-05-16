#ifndef __mrtypes_h__
#define __mrtypes_h__

#include <pthread.h>

#define QUEUE_MAX 32
#define DEFAULT_LIST_CAPACITY 64

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

typedef struct Key_With_Value_List {
  char *key;
  char **values;
  int len;
  int capacity;
  int iter;
} Key_With_Value_List;

typedef struct Part {
  int len;
  int capacity;
  int iter;
  Key_With_Value_List *keys;
  // each part needs a lock
  pthread_mutex_t lock;
} Part;

typedef struct Store {
  int partition_count;
  Part *parts;
} Store;

#endif // __mrtypes_h__