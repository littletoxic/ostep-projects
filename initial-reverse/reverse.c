#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

typedef struct node {
  struct node *pre;
  struct node *next;
  char *s;
} node;

typedef enum bool { false, true } bool;

bool same(char *input, char *output) {
  if (access(output, F_OK) == -1) {
    // output file not exists
    return false;
  }

  struct stat stat1, stat2;
  if (stat(input, &stat1) == -1) {
    fprintf(stderr, "reverse: cannot open file '%s'\n", input);
    exit(EXIT_FAILURE);
  }
  if (stat(output, &stat2) == -1) {
    fprintf(stderr, "reverse: cannot open file '%s'\n", output);
    exit(EXIT_FAILURE);
  }
  return stat1.st_dev == stat2.st_dev && stat1.st_ino == stat2.st_ino;
}

int main(int argc, char *argv[]) {
  FILE *in = stdin;
  FILE *out = stdout;
  switch (argc) {
  case 3:
    if (same(argv[1], argv[2])) {
      fprintf(stderr, "reverse: input and output file must differ\n");
      exit(EXIT_FAILURE);
    }
    out = fopen(argv[2], "w");
    if (out == NULL) {
      fprintf(stderr, "reverse: cannot open file '%s'\n", argv[2]);
      exit(EXIT_FAILURE);
    }
  case 2:
    in = fopen(argv[1], "r");
    if (in == NULL) {
      fprintf(stderr, "reverse: cannot open file '%s'\n", argv[1]);
      exit(EXIT_FAILURE);
    }
  case 1:
    break;
  default:
    fprintf(stderr, "usage: reverse <input> <output>\n");
    exit(EXIT_FAILURE);
  }

  node *head = calloc(1, sizeof(node));
  node *now = head;

  char *line = NULL;
  size_t bufsize = 0;
  ssize_t nread = 0;

  while ((nread = getline(&line, &bufsize, in)) != -1) {
    now->next = calloc(1, sizeof(node));
    now->next->pre = now;
    now = now->next;
    now->s = strdup(line);
    if (now->s == NULL) {
      fprintf(stderr, "malloc failed\n");
      exit(EXIT_FAILURE);
    }
  }

  while (now != head) {
    fprintf(out, "%s", now->s);
    now = now->pre;
  }

  return 0;
}