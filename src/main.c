#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef enum {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
  PREPARE_SUCCESS,
  PREPARE_UNRECOGNIZED_STATEMENT,
  PREPARE_SYNTAX_ERROR
} PrepareResult;
typedef enum { STATEMENT_INSERT, STATEMENT_SELECT } StatementType;
typedef enum { EXECUTE_SUCCESS, EXECUTE_TABLE_FULL } ExecuteResult;

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
typedef struct {
  uint32_t id;
  char username[COLUMN_USERNAME_SIZE];
  char email[COLUMN_EMAIL_SIZE];
} Row;

typedef struct {
  StatementType type;
  Row row_to_insert;
} Statement;

#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_SIZE + USERNAME_OFFSET;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const uint32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 100
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

//
void serialize_row(Row *source, void *destination) {
  memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
  memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
  memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void *source, Row *destination) {
  memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
  memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

typedef struct {
  uint32_t num_rows;
  void *pages[TABLE_MAX_PAGES];
} Table;

Table *new_table() {
  Table *table = malloc(sizeof(Table));
  table->num_rows = 0;
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    table->pages[i] = NULL;
  return table;
}

void free_table(Table *table) {
  for (uint32_t i = 0; table->pages[i]; i++) {
    free(table->pages[i]);
  }
  free(table);
}

void print_row(Row *row) {
  printf("(%d %s %s)\n", row->id, row->username, row->email);
}

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

PrepareResult prepare_statement(Ibuffer *buffer, Statement *statement) {
  if (strncmp(buffer->buf, "insert", 6) == 0) {
    statement->type = STATEMENT_INSERT;

    int args_check = sscanf(
        buffer->buf, "insert %d %s %s", &(statement->row_to_insert.id),
        statement->row_to_insert.username, statement->row_to_insert.email);

    if (args_check < 3) {
      return PREPARE_SYNTAX_ERROR;
    }

    return PREPARE_SUCCESS;
  }

  if (strncmp(buffer->buf, "select", 6) == 0) {
    statement->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}

void *row_slot(Table *table, uint32_t row_num) {
  uint32_t page_num = row_num / ROWS_PER_PAGE;
  void *page = table->pages[page_num];
  if (page == NULL) {
    page = table->pages[page_num] = malloc(PAGE_SIZE);
  }
  uint32_t row_offset = row_num % ROWS_PER_PAGE;
  uint32_t byte_offset = row_offset * ROW_SIZE;

  return page + byte_offset;
}

ExecuteResult execute_insert(Statement *statement, Table *table) {
  if (table->num_rows >= TABLE_MAX_ROWS) {
    return EXECUTE_TABLE_FULL;
  }
  Row *row_to_insert = &(statement->row_to_insert);
  serialize_row(row_to_insert, row_slot(table, table->num_rows));
  table->num_rows += 1;
  return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement *statement, Table *table) {
  Row row;
  for (uint32_t i = 0; i < table->num_rows; i++) {
    deserialize_row(row_slot(table, i), &row);
    print_row(&row);
  }

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement *statement, Table *table) {
  switch (statement->type) {
  case STATEMENT_INSERT:
    return execute_insert(statement, table);
  case STATEMENT_SELECT:
    return execute_select(statement, table);
  }
}

int main() {
  Table *table = new_table();
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
    switch (prepare_statement(buffer, &statement)) {
    case PREPARE_SUCCESS:
      break;
    case PREPARE_SYNTAX_ERROR:
      printf("Syntax error. Could not parse statement.\n");
      continue;
    case PREPARE_UNRECOGNIZED_STATEMENT:
      printf("Unrecognized keyword at start of '%s'.\n", buffer->buf);
      continue;
    }

    switch (execute_statement(&statement, table)) {
    case EXECUTE_SUCCESS:
      printf("Executed.\n");
      break;
    case EXECUTE_TABLE_FULL:
      printf("Error: Table full.\n");
      break;
    }
  }
}