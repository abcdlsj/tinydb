#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>

typedef struct {
  char *buf;
  size_t buf_len;
  size_t in_len;
} Ibuffer;

Ibuffer* new_in_buffer() {
  Ibuffer* in_buffer = malloc(sizeof(Ibuffer));
  in_buffer->buf_len = 0;
  in_buffer->buf = NULL;
  in_buffer->in_len = 0;

  return in_buffer;
}

void print_prompt() {
  printf("[tinydb]\n>> ");
}

void read_input(Ibuffer* buffer) {
  ssize_t bytes = getline(&(buffer->buf), &(buffer->buf_len), stdin);

  if (bytes < 0) {
    printf("Error [Reading Input]\n");
    exit(EXIT_FAILURE);
  }


  buffer->in_len = bytes - 1;
  buffer->buf[bytes - 1] = 0;
}


void close_buffer(Ibuffer* buffer) {
  free(buffer->buf);
  free(buffer);
}

int main () {
  Ibuffer* buffer = new_in_buffer();

  for (;;) {
    print_prompt();
    read_input(buffer);

    if (strcmp(buffer->buf, ".exit") == 0) {
      close_buffer(buffer);
      exit(EXIT_FAILURE);
    } else {
      printf("Unrecognized command '%s'.\n", buffer->buf);
    }
  }
}