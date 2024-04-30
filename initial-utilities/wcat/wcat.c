#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

int main(int argc, char *argv[]) {

  if (argc == 1) {
    return 0;
  }

  char buffer[1024];
  ssize_t size;

  for (int i = 1; i < argc; i++) {
    int fd = open(argv[i], O_RDONLY);
    if (fd == -1) {
      printf("wcat: cannot open file\n");
      return 1;
    }
    while ((size = read(fd, buffer, 1024)) > 0) {
      write(STDOUT_FILENO, buffer, size);
    }
  }
}
