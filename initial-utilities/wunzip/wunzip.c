#include <stdio.h>

int main(int argc, char *argv[]) {
  if (argc == 1) {
    printf("wunzip: file1 [file2 ...]\n");
    return 1;
  }
  int count;
  char c;
  for (int i = 1; i < argc; i++) {
    FILE *f = fopen(argv[i], "r");
    if (f == NULL) {
      return 1;
    }

    while (fread(&count, sizeof(int), 1, f) > 0) {
      fread(&c, sizeof(char), 1, f);
      for(int j = 0; j < count; j++) {
        putchar(c);
      }
    }
  }

  return 0;
}