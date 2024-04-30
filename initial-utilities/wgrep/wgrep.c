#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

const char *needle;

int findNewLine(int start, int size, char *buffer) {
  for (int j = start; j < size; j++) {
    if (buffer[j] == '\n') {
      buffer[j] = 0;
      return j;
    }
  }
  return -1;
}

void processFile(int fd) {
  int len = 2048;
  char *buffer = (char *)malloc(len);

  ssize_t size = read(fd, buffer, len);
  if (size < len && buffer[size - 1] != '\n') {
    buffer[size] = '\n';
  }

  int pos; // position of \n
  while (size > 0) {

    int start = 0;
    while ((pos = findNewLine(start, size, buffer)) == -1) {
      start = size;
      // 防止 len 无限增长
      if (size == len) {
        len <<= 1;
        buffer = (char *)realloc(buffer, len);
      }
      size = size + read(fd, buffer + size, len - size);
      if (size < len && buffer[size - 1] != '\n') {
        buffer[size] = '\n';
      }
    }

    int lastPos = -1;
    do {
      if (strstr(buffer + lastPos + 1, needle) != NULL) {
        write(STDOUT_FILENO, buffer + lastPos + 1, pos - lastPos - 1);
        write(STDOUT_FILENO, "\n", 1);
      }
      lastPos = pos;
    } while ((pos = findNewLine(lastPos, size, buffer)) != -1);

    size = size - lastPos - 1;
    for (int i = 0; i < size; i++) {
      buffer[i] = buffer[lastPos + i + 1];
    }
  }
  free(buffer);
}

int main(int argc, char *argv[]) {
  needle = argv[1];
  if (argc < 2) {
    printf("wgrep: searchterm [file ...]\n");
    return 1;
  } else if (argc == 2) {
    processFile(STDIN_FILENO);
  } else {
    for (int i = 2; i < argc; i++) {
      int fd = open(argv[i], O_RDONLY);
      if (fd == -1) {
        printf("wgrep: cannot open file\n");
        return 1;
      }
      processFile(fd);
    }
  }
}
