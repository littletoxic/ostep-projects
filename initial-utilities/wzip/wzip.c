#include <stdio.h>

int main(int argc, char *argv[]) {
  if (argc == 1) {
    printf("wzip: file1 [file2 ...]\n");
    return 1;
  }
  char buffer[1024];
  size_t size;

  int count = 1;
  char last;
  int inited = 0;

  for (int i = 1; i < argc; i++) {
    FILE *f = fopen(argv[i], "r");
    if (f == NULL) {
      return 1;
    }

    if (!inited) {
      if (fread(&last, 1, 1, f) == 0) {
        continue;
      }
      inited = 1;
    }

    while ((size = fread(buffer, 1, 1024, f)) > 0) {
      for (int j = 0; j < size; j++) {
        if (buffer[j] == last) {
          count++;
        } else {
          fwrite(&count, 4, 1, stdout);
          fwrite(&last, 1, 1, stdout);
          count = 1;
        }
        last = buffer[j];
      }
    }
  }
  fwrite(&count, 4, 1, stdout);
  fwrite(&last, 1, 1, stdout);
  return 0;
}