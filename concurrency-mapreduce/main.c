#include "mapreduce.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void Map(char *file_name) {
  FILE *fp = fopen(file_name, "r");
  assert(fp != NULL);

  char *line = NULL;
  size_t size = 0;
  while (getline(&line, &size, fp) != -1) {
    char *token, *dummy = line;
    while ((token = strsep(&dummy, " \t\n\r")) != NULL) {
      if (token[0] == 0) {
        continue;
      }
      MR_Emit(token, "1");
    }
  }
  free(line);
  fclose(fp);
}

void Reduce(char *key, Getter get_next, int partition_number) {
  int count = 0;
  char *value;
  while ((value = get_next(key, partition_number)) != NULL)
    count++;
  printf("%s %d\n", key, count);
}

int main(int argc, char *argv[]) {
  MR_Run(argc, argv, Map, 8, Reduce, 8, MR_DefaultHashPartition);
}