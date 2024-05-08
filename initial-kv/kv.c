#define _DEFAULT_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct pair {
  long k;
  char *v;
} pair;

typedef struct list {
  pair *l;
  int len;
  int capacity;
} list;

typedef struct tokens {
  char *argv[3];
  int len;
} tokens;

list *kvlist;

void put(tokens *tokens) {
  if (tokens->len != 3) {
    // error
    printf("bad command\n");
    return;
  }
  char *reminder = NULL;
  long key = strtol(tokens->argv[1], &reminder, 10);
  if (reminder[0] != 0) {
    // error
    printf("bad command\n");
    return;
  }

  int position = -1;
  for (int i = 0; i < kvlist->len; i++) {
    if (kvlist->l[i].k == key) {
      position = i;
      break;
    }
  }

  if (position == -1) {
    position = kvlist->len;
    kvlist->len++;
    if (kvlist->len == kvlist->capacity) {
      kvlist->capacity *= 2;
      kvlist->l = realloc(kvlist->l, kvlist->capacity * sizeof(pair));
    }
  }

  kvlist->l[position].k = key;
  kvlist->l[position].v = tokens->argv[2];
}

void get(tokens *tokens) {
  if (tokens->len != 2) {
    // error
    printf("bad command\n");
    return;
  }
  char *reminder = NULL;
  long key = strtol(tokens->argv[1], &reminder, 10);
  if (reminder[0] != 0) {
    // error
    printf("bad command\n");
    return;
  }

  char *value;
  int find = 0;
  for (int i = 0; i < kvlist->len; i++) {
    if (kvlist->l[i].k == key) {
      find = 1;
      value = kvlist->l[i].v;
      // print value
      printf("%ld,%s\n", key, value);
      break;
    }
  }

  if (!find) {
    // not found
    printf("%ld not found\n", key);
  }
}

void delete(tokens *tokens) {
  if (tokens->len != 2) {
    // error
    printf("bad command\n");
    return;
  }
  char *reminder = NULL;
  long key = strtol(tokens->argv[1], &reminder, 10);
  if (reminder[0] != 0) {
    // error
    printf("bad command\n");
    return;
  }

  int find = 0;
  for (int i = 0; i < kvlist->len; i++) {
    if (kvlist->l[i].k == key) {
      find = 1;

      // delete
      for (int j = i + 1; j < kvlist->len; j++) {
        kvlist->l[j - 1] = kvlist->l[j];
      }
      kvlist->len--;
      break;
    }
  }

  if (!find) {
    // not found
    printf("%ld not found\n", key);
  }
}

void clear(tokens *tokens) {
  if (tokens->len != 1) {
    // error
    printf("bad command\n");
    return;
  }

  kvlist->len = 0;
}

void all(tokens *tokens) {
  if (tokens->len != 1) {
    // error
    printf("bad command\n");
    return;
  }
}

void init() {
  kvlist = malloc(sizeof(list));
  kvlist->len = 0;
  kvlist->capacity = 32;
  kvlist->l = malloc(sizeof(pair) * kvlist->capacity);

  FILE *file = fopen("database.txt", "r");

  if (file != NULL) {
    char *line = NULL;
    size_t len = 0;
    ssize_t nread;
    tokens tokens;

    while ((nread = getline(&line, &len, file)) != -1) {
      line[strlen(line) - 1] = 0;

      tokens.len = 1;
      char *token = NULL;
      while ((token = strsep(&line, ",")) != NULL) {
        tokens.argv[tokens.len++] = token;
      }
      put(&tokens);
    }

    fclose(file);
  }
}

void save() {
  FILE *file = fopen("database.txt", "w");

  if (file == NULL) {
    printf("fail to open database.txt\n");
    exit(EXIT_FAILURE);
  }

  for (int i = 0; i < kvlist->len; i++) {
    fprintf(file, "%ld,%s\n", kvlist->l[i].k, kvlist->l[i].v);
  }

  fclose(file);
}

int main(int argc, char *argv[]) {
  if (argc == 1) {
    return 0;
  }

  init();
  tokens tokens;

  for (int i = 1; i < argc; i++) {
    tokens.len = 0;
    char *token = NULL;
    while ((token = strsep(&argv[i], ",")) != NULL) {
      if (tokens.len == 3) {
        // error
        printf("bad command\n");
        continue;
      }
      tokens.argv[tokens.len++] = token;
    }

    if (strlen(tokens.argv[0]) != 1) {
      // error
      printf("bad command\n");
      continue;
    }

    switch (tokens.argv[0][0]) {
    case 'p':
      put(&tokens);
      break;
    case 'g':
      get(&tokens);
      break;
    case 'd':
      delete (&tokens);
      break;
    case 'c':
      clear(&tokens);
      break;
    case 'a':
      all(&tokens);
      break;
    default:
      // print error
      printf("bad command\n");
      break;
    }
  }

  save();
}