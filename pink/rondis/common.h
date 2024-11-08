#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#ifndef RONDIS_COMMON_H
#define RONDIS_COMMON_H

#define MAX_CONNECTIONS 2

#define REDIS_DB_NAME "redis"

#define FOREIGN_KEY_RESTRICT_ERROR 256

#define RONDB_INTERNAL_ERROR 2
#define READ_ERROR 626

int write_formatted(char *buffer, int bufferSize, const char *format, ...);
void assign_ndb_err_to_response(std::string *response, const char *app_str, NdbError error);
void assign_generic_err_to_response(std::string *response, const char *app_str);

// NDB API error messages
#define FAILED_GET_DICT "Failed to get NdbDict"
#define FAILED_CREATE_TABLE_OBJECT "Failed to create table object"
#define FAILED_CREATE_TXN_OBJECT "Failed to create transaction object"
#define FAILED_EXEC_TXN "Failed to execute transaction"
#define FAILED_READ_KEY "Failed to read key"
#define FAILED_INCR_KEY "Failed to increment key"
#define FAILED_INCR_KEY_MULTI_ROW "Failed to increment key, multi-row value"
#define FAILED_GET_OP "Failed to get NdbOperation object"
#define FAILED_DEFINE_OP "Failed to define RonDB operation"

// Redis errors
#define REDIS_UNKNOWN_COMMAND "unknown command '%s'"
#define REDIS_WRONG_NUMBER_OF_ARGS "wrong number of arguments for '%s' command"
#define REDIS_NO_SUCH_KEY "$-1\r\n"
#define REDIS_KEY_TOO_LARGE "key is too large (3000 bytes max)"
#endif
