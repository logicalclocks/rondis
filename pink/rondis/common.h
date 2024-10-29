#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#define MAX_CONNECTIONS 2

#define REDIS_DB_NAME "redis"

#define FOREIGN_KEY_RESTRICT_ERROR 256

#define RONDB_INTERNAL_ERROR 2
#define READ_ERROR 626

int execute_no_commit(NdbTransaction *trans, int &ret_code, bool allow_fail);
int execute_commit(Ndb *ndb, NdbTransaction *trans, int &ret_code);
int write_formatted(char *buffer, int bufferSize, const char *format, ...);
void assign_ndb_err_to_response(std::string *response, const char *app_str, Uint32 error_code);
void assign_generic_err_to_response(std::string *response,const char *app_str);

// NDB API error messages
#define FAILED_GET_DICT "Failed to get NdbDict"
#define FAILED_CREATE_TABLE_OBJECT "Failed to create table object"
#define FAILED_CREATE_TXN_OBJECT "Failed to create transaction object"
#define FAILED_EXEC_TXN "Failed to execute transaction"
#define FAILED_READ_KEY "Failed to read key"
#define FAILED_GET_OP "Failed to get NdbOperation object"
#define FAILED_DEFINE_OP "Failed to define RonDB operation"

// Redis errors
#define UNKNOWN_COMMAND "unknown command '%s'"
#define WRONG_NUMBER_OF_ARGS "wrong number of arguments for '%s' command"

// Generic errors
void failed_no_such_row_error(std::string *response);
void failed_large_key(std::string *response);
