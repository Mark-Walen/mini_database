#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)
#define TABLE_MAX_PAGES 100

typedef struct InputBuffer InputBuffer;
typedef struct Statement Statement;
typedef struct Row Row;
typedef struct Table Table;
typedef struct Pager Pager;

typedef enum
{
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
} ExecuteResult;

typedef enum
{
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum
{
    PREPARE_SUCCESS,
    PREPARE_NEGATIVE_ID,
    PREPARE_SYNTAX_ERROR,
    PREPARE_STRING_TOO_LONG,
    PREPARE_UNRECOGNIZED_STATEMENT,
} PrepareResult;

typedef enum
{
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

struct InputBuffer
{
    char *buffer;
    size_t bufferLength;
    ssize_t inputLength;
};

struct Row
{
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
};

struct Statement
{
    StatementType type;
    Row rowToInsert;
};

struct Pager
{
    int fd;        // file descriptor
    uint32_t fLen; // file length
    void *pages[TABLE_MAX_PAGES];
};

struct Table
{
    uint32_t numRows;
    Pager *pager;
};

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const uint32_t PAGE_SIZE = 4096;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

static InputBuffer *newInputBuffer(void);
static void readInput(InputBuffer *inputBuffer);
static void closeInputBuffer(InputBuffer *inputBuffer);
static void printPrompt();
static MetaCommandResult doMetaCommand(InputBuffer *inputBuffer, Table *table);
static PrepareResult prepareInsertStatement(InputBuffer *input_buffer, Statement *statement);
static PrepareResult prepareStatement(InputBuffer *inputBuffer,
                                      Statement *statement);
static ExecuteResult executeInsertStatement(Statement *statement, Table *table);
static ExecuteResult executeSelectStatement(Statement *statement, Table *table);
static ExecuteResult executeStatement(Statement *statement, Table *table);
static void serializeRow(Row *source, void *dest);
static void deserializeRow(void *source, Row *dest);
static void *rowSlot(Table *table, uint32_t rowNum);
static Pager *openPager(const char *fn);
static void *getPage(Pager *pager, uint32_t pageNum);
static void flushPager(Pager *pager, uint32_t pageNum, uint32_t size);
static Table *openDatabase(const char *fn);
static void closeDatabase(Table *table);
static void printRow(Row *row);

static void printRow(Row *row)
{
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

static Pager *openPager(const char *fn)
{
    int fd = open(fn,
                  O_RDWR |     // Read/Write mode
                      O_CREAT, // Create file if it does not exist
                  S_IWUSR |    // User write permission
                      S_IRUSR  // User read permission
    );
    if (fd == -1)
    {
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }
    off_t fLen = lseek(fd, 0, SEEK_END);

    Pager *pager = malloc(sizeof(Pager));
    pager->fd = fd;
    pager->fLen = fLen;
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    {
        pager->pages[i] = NULL;
    }
    return pager;
}

static void *getPage(Pager *pager, uint32_t pageNum)
{
    if (pageNum > TABLE_MAX_PAGES)
    {
        printf("Tried to fetch page number out of bounds. %d > %d\n", pageNum,
               TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    if (pager->pages[pageNum] == NULL)
    {
        // Cache miss. Allocate memory and load from file.
        void *page = malloc(PAGE_SIZE);
        uint32_t numPages = pager->fLen / PAGE_SIZE;

        // We might save a partial page at the end of the file
        if (pager->fLen % PAGE_SIZE)
        {
            numPages++;
        }

        if (pageNum <= numPages)
        {
            lseek(pager->fd, pageNum * PAGE_SIZE, SEEK_SET);
            ssize_t bytesRead = read(pager->fd, page, PAGE_SIZE);
            if (bytesRead == -1)
            {
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[pageNum] = page;
    }
    return pager->pages[pageNum];
}

void flushPager(Pager *pager, uint32_t pageNum, uint32_t size)
{
    if (pager->pages[pageNum] == NULL)
    {
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->fd, pageNum * PAGE_SIZE, SEEK_SET);
    if (offset == -1)
    {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    ssize_t bytesWritten = write(pager->fd, pager->pages[pageNum], size);
    if (bytesWritten == -1)
    {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

/**
 * @brief open database
 * @param fn database filename
 */
static Table *openDatabase(const char *fn)
{
    Pager *pager = openPager(fn);
    uint32_t numRows = pager->fLen / ROW_SIZE;

    Table *table = (Table *)malloc(sizeof(Table));
    table->pager = pager;
    table->numRows = numRows;

    return table;
}

/**
 * @brief close database
 */
static void closeDatabase(Table *table)
{
    Pager *pager = table->pager;
    uint32_t numFullPages = table->numRows / ROWS_PER_PAGE;

    for (uint32_t i = 0; i < numFullPages; i++)
    {
        if (pager->pages[i] == NULL)
        {
            continue;
        }
        flushPager(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    // There may be a partial page to write to the end of the file
    // This should not be needed after we switch to a B-tree
    uint32_t numAdditionalRows = table->numRows % ROWS_PER_PAGE;
    if (numAdditionalRows > 0)
    {
        uint32_t pageNum = numFullPages;
        if (pager->pages[pageNum] != NULL)
        {
            Row row;
            flushPager(pager, pageNum, numAdditionalRows * ROW_SIZE);
            free(pager->pages[pageNum]);
            pager->pages[pageNum] = NULL;
        }
    }

    int result = close(pager->fd);
    if (result == -1)
    {
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    {
        void *page = pager->pages[i];
        if (page)
        {
            free(page);
            pager->pages[i] = NULL;
        }
    }

    free(pager);
    free(table);
}

static void *rowSlot(Table *table, uint32_t rowNum)
{
    uint32_t pageNum = rowNum / ROWS_PER_PAGE;
    void *page = getPage(table->pager, pageNum);
    uint32_t rowOffset = rowNum % ROWS_PER_PAGE;
    uint32_t byteOffset = rowOffset * ROW_SIZE;
    return page + byteOffset;
}

static void serializeRow(Row *source, void *dest)
{
    memcpy(dest + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(dest + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(dest + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

static void deserializeRow(void *source, Row *dest)
{
    memcpy(&(dest->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(dest->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(dest->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

static InputBuffer *newInputBuffer(void)
{
    InputBuffer *inputBuffer = (InputBuffer *)malloc(sizeof(inputBuffer));
    inputBuffer->buffer = NULL;
    inputBuffer->bufferLength = 0;
    inputBuffer->inputLength = 0;
    return inputBuffer;
}

static void readInput(InputBuffer *inputBuffer)
{
    ssize_t bytesRead = getline(&(inputBuffer->buffer), &(inputBuffer->bufferLength), stdin);

    if (bytesRead <= 0)
    {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }
    // Ignore trailing newline
    inputBuffer->inputLength = bytesRead - 1;
    inputBuffer->buffer[bytesRead - 1] = 0;
}

static void closeInputBuffer(InputBuffer *inputBuffer)
{
    free(inputBuffer->buffer);
    free(inputBuffer);
}

static void printPrompt()
{
    printf("db > ");
}

static MetaCommandResult doMetaCommand(InputBuffer *inputBuffer, Table *table)
{
    if (strcmp(inputBuffer->buffer, ".exit") == 0)
    {
        closeInputBuffer(inputBuffer);
        closeDatabase(table);
        exit(EXIT_SUCCESS);
    }
    else
    {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

static PrepareResult prepareInsertStatement(InputBuffer *input_buffer, Statement *statement)
{
    char *keyword = strtok(input_buffer->buffer, " ");
    char *id_string = strtok(NULL, " ");
    char *username = strtok(NULL, " ");
    char *email = strtok(NULL, " ");

    if (id_string == NULL || username == NULL || email == NULL)
    {
        return PREPARE_SYNTAX_ERROR;
    }
    int id = atoi(id_string);
    if (id < 0)
    {
        return PREPARE_NEGATIVE_ID;
    }

    if (strlen(username) > COLUMN_USERNAME_SIZE)
    {
        return PREPARE_STRING_TOO_LONG;
    }

    if (strlen(email) > COLUMN_EMAIL_SIZE)
    {
        return PREPARE_STRING_TOO_LONG;
    }

    statement->rowToInsert.id = id;
    strcpy(statement->rowToInsert.username, username);
    strcpy(statement->rowToInsert.email, email);

    return PREPARE_SUCCESS;
}

static PrepareResult prepareStatement(InputBuffer *inputBuffer,
                                      Statement *statement)
{
    if (strncmp(inputBuffer->buffer, "insert", 6) == 0)
    {
        statement->type = STATEMENT_INSERT;

        return prepareInsertStatement(inputBuffer, statement);
    }
    if (strcmp(inputBuffer->buffer, "select") == 0)
    {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

static ExecuteResult executeInsertStatement(Statement *statement, Table *table)
{
    if (table->numRows >= TABLE_MAX_ROWS)
    {
        return EXECUTE_TABLE_FULL;
    }
    Row *rowToInsert = &(statement->rowToInsert);
    serializeRow(rowToInsert, rowSlot(table, table->numRows));
    table->numRows += 1;
    return EXECUTE_SUCCESS;
}

static ExecuteResult executeSelectStatement(Statement *statement, Table *table)
{
    Row row;
    for (uint32_t i = 0; i < table->numRows; i++)
    {
        deserializeRow(rowSlot(table, i), &row);
        printRow(&row);
    }
    return EXECUTE_SUCCESS;
}

static ExecuteResult executeStatement(Statement *statement, Table *table)
{
    switch (statement->type)
    {
    case (STATEMENT_INSERT):
        return executeInsertStatement(statement, table);
    case (STATEMENT_SELECT):
        return executeSelectStatement(statement, table);
    }
}

int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        printf("Must supply a database filename.\n");
        exit(EXIT_FAILURE);
    }

    char *filename = argv[1];
    Table *table = openDatabase(filename);
    InputBuffer *inputBuffer = newInputBuffer();

    while (true)
    {
        printPrompt();
        readInput(inputBuffer);
        if (inputBuffer->buffer[0] == '.')
        {
            switch (doMetaCommand(inputBuffer, table))
            {
            case (META_COMMAND_SUCCESS):
                continue;
            case (META_COMMAND_UNRECOGNIZED_COMMAND):
                printf("Unrecognized command '%s'\n", inputBuffer->buffer);
                continue;
            }
        }
        Statement statement;
        switch (prepareStatement(inputBuffer, &statement))
        {
        case (PREPARE_SUCCESS):
            /* code */
            break;

        case (PREPARE_NEGATIVE_ID):
            printf("ID must be positive.\n");
            continue;

        case (PREPARE_STRING_TOO_LONG):
            printf("String is too long.\n");
            continue;
        case (PREPARE_SYNTAX_ERROR):
            printf("Syntax error. Could not parse statement.\n");
            continue;

        case (PREPARE_UNRECOGNIZED_STATEMENT):
            printf("Unrecognized keyword at start of '%s'.\n",
                   inputBuffer->buffer);
            continue;
        default:
            break;
        }

        switch (executeStatement(&statement, table))
        {
        case (EXECUTE_SUCCESS):
            printf("Executed.\n");
            break;
        case (EXECUTE_TABLE_FULL):
            printf("Error: Table full.\n");
            break;
        default:
            break;
        }
    }
    return 0;
}
