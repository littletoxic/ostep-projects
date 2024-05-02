#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

typedef struct plist {
  int len;
  int capacity;
  void **list;
} plist;

typedef enum bool { false, true } bool;

plist *pathlist;

plist *create_list() {
  plist *list = malloc(sizeof(plist));
  list->len = 0;
  list->capacity = 10;
  list->list = malloc(10 * sizeof(void *));
  list->list[0] = NULL;
  return list;
}

void add_item(plist *list, void *item) {
  if (list->len + 1 == list->capacity) {
    list->capacity *= 2;
    list->list = realloc(list->list, list->capacity * sizeof(void *));
  }

  list->list[list->len++] = item;
  list->list[list->len] = NULL;
}

void clear_list(plist *list) {
  list->len = 0;
  list->list[0] = NULL;
}

void clear_and_free_list_element(plist *list) {
  for (int i = 0; i < list->len; i++) {
    free(list->list[i]);
  }
  clear_list(list);
}

bool check_empty(const char *str) {
  if (str[0] == '\0') {
    return true;
  } else {
    return false;
  }
}

void print_error() {
  static char error_message[30] = "An error has occurred\n";
  write(STDERR_FILENO, error_message, strlen(error_message));
}

void destroy_list(plist *list) {
  free(list->list);
  free(list);
}

bool run_internal_commmand(char *command, plist *arglist) {
  if (strcmp(command, "exit") == 0) {
    // exit
    if (arglist->len != 1) {
      print_error();
      return true;
    }
    exit(0);
  } else if (strcmp(command, "cd") == 0) {
    // cd
    if (arglist->len != 2) {
      print_error();
      return true;
    }
    if (chdir(arglist->list[1]) == -1) {
      print_error();
    }
    return true;
  } else if (strcmp(command, "path") == 0) {
    // path
    clear_and_free_list_element(pathlist);
    for (int i = 1; i < arglist->len; i++) {
      add_item(pathlist, strdup(arglist->list[i]));
    }
    return true;
  }
  return false;
}

pid_t run_command(char *sub) {
  char *token = NULL;
  char *redirect = sub;
  sub = strsep(&redirect, ">");
  // parse redirect
  char *redirectfilename = NULL;
  bool r = redirect != NULL;
  while ((token = strsep(&redirect, "\r\n ")) != NULL) {

    if (check_empty(token)) {
      continue;
    }
    if (redirectfilename == NULL) {
      redirectfilename = token;
    } else {
      print_error();
      return 0;
    }
  }
  if (r == true && redirectfilename == NULL) {
    print_error();
    return 0;
  }

  token = NULL;
  plist *arglist = create_list();
  char *command = NULL;
  // parse command and arg
  while ((token = strsep(&sub, "\r\n ")) != NULL) {
    if (check_empty(token)) {
      continue;
    }
    if (command == NULL) {
      command = token;
      add_item(arglist, token);
    } else {
      add_item(arglist, token);
    }
  }

  if (command == NULL) {
    if (r == true) {
      print_error();
    }
    return 0;
  }

  if (run_internal_commmand(command, arglist)) {
    destroy_list(arglist);
    return 0;
  } else {
    // isn't bulit-in command
    int len = 50;
    char *fullpath = malloc(len);

    bool find = false;
    for (int i = 0; i < pathlist->len; i++) {
      if (len < strlen(command) + strlen(pathlist->list[i]) + 2) {
        len = strlen(command) + strlen(pathlist->list[i]) + 2;
        fullpath = realloc(fullpath, len);
      }
      sprintf(fullpath, "%s/%s", (char *)pathlist->list[i], command);
      if (!access(fullpath, X_OK)) {
        find = true;
        break;
      }
    }

    if (!find) {
      print_error();
      return 0;
    }

    pid_t pid = fork();

    if (pid == -1) {
      print_error();
      return 0;
    }
    if (pid == 0) {
      // child
      // set redirect
      if (redirectfilename != NULL) {
        int fd = open(redirectfilename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd == -1) {
          print_error();
          return 0;
        }
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
      }

      if (execv(fullpath, (char **)arglist->list) == -1) {
        print_error();
        exit(1);
      }
    } else {
      // parent
      destroy_list(arglist);
      return pid;
    }
  }
  return 0;
}

void main_loop(bool interactive) {
  char *line = NULL;
  size_t buflen = 0;
  ssize_t nread = 0;
  plist *pidlist = create_list();
  while (1) {
    if (interactive) {
      printf("wish> ");
    }

    if ((nread = getline(&line, &buflen, stdin)) != -1) {
      char *sub = NULL;
      while ((sub = strsep(&line, "&")) != NULL) {
        if (!check_empty(sub)) {
          pid_t pid = run_command(sub);
          if (pid > 0) {
            add_item(pidlist, (void *)(long)pid);
          }
        }
      }

      // wait
      for (int i = 0; i < pidlist->len; i++) {
        int wstatus = 0;
        waitpid((pid_t)(long)pidlist->list[i], &wstatus, 0);
      }

      // clear list
      clear_list(pidlist);
    } else {
      // Maybe EOF
      exit(0);
    }
  }
}

int main(int argc, char *argv[]) {
  pathlist = create_list();
  char *initpath = strdup("/bin");
  add_item(pathlist, initpath);
  if (argc == 1) {
    // interactive
    main_loop(true);
  } else if (argc == 2) {
    // batch
    close(STDIN_FILENO);
    int fd = open(argv[1], O_RDONLY);
    if (fd == -1) {
      print_error();
      exit(1);
    }

    main_loop(false);
  } else {
    // error
    print_error();
    return 1;
  }
}