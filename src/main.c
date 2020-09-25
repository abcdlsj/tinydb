#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef enum {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum { PREPARE_SUCCESS, PREPARE_UNRECOGNIZED_STATEMENT } PrepareResult;
typedef enum { STATEMENT_INSERT, STATEMENT_SELECT } StatementType;

typedef struct {
  StatementType type;
} Statement;

typedef struct {
  char *buf;
  size_t buf_len;
  size_t in_len;
} Ibuffer;

Ibuffer *new_in_buffer() {
  Ibuffer *in_buffer = malloc(sizeof(Ibuffer));
  in_buffer->buf_len = 0;
  in_buffer->buf = NULL;
  in_buffer->in_len = 0;

  return in_buffer;
}

void print_prompt() { printf("[tinydb]\n>> "); }

void read_input(Ibuffer *buffer) {
  ssize_t bytes = getline(&(buffer->buf), &(buffer->buf_len), stdin);

  if (bytes < 0) {
    printf("Error [Reading Input]\n");
    exit(EXIT_FAILURE);
  }

  buffer->in_len = bytes - 1;
  buffer->buf[bytes - 1] = 0;
}

void close_buffer(Ibuffer *buffer) {
  free(buffer->buf);
  free(buffer);
}

MetaCommandResult do_meta_command(Ibuffer *buffer) {
  if (strcmp(buffer->buf, ".exit") == 0) {
    close_buffer(buffer);
    exit(EXIT_FAILURE);
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

PrepareResult prepare_statement(Ibuffer *buffer, Statement statement) {
  if (strncmp(buffer->buf, "insert", 6) == 0) {
    statement.type = STATEMENT_INSERT;
    return PREPARE_SUCCESS;
  }
  if (strncmp(buffer->buf, "select", 6) == 0) {
    statement.type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}

void execute_statement(Statement *statement) {
  switch (statement->type) {
  case STATEMENT_INSERT:
    printf("insert.\n");
    break;
  case STATEMENT_SELECT:
    printf("select.\n");
    break;
  }
}

int main() {
  Ibuffer *buffer = new_in_buffer();
  for (;;) {
    print_prompt();
    read_input(buffer);

    if (buffer->buf[0] == '.') {
      switch (do_meta_command(buffer)) {
      case META_COMMAND_SUCCESS:
        continue;
      case META_COMMAND_UNRECOGNIZED_COMMAND:
        printf("Unrecognized command '%s'\n", buffer->buf);
        continue;
      }
    }

    Statement statement;
    switch (prepare_statement(buffer, statement)) {
    case PREPARE_SUCCESS:
      break;
    case PREPARE_UNRECOGNIZED_STATEMENT:
      printf("Unrecognized keyword at start of '%s'.\n", buffer->buf);
      continue;
    }

    execute_statement(&statement);
    printf("Executed.\n");
  }
}