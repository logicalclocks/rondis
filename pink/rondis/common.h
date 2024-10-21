#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#define MAX_CONNECTIONS 1
#define MAX_NDB_PER_CONNECTION 1

#define FOREIGN_KEY_RESTRICT_ERROR 256

#define READ_VALUE_ROWS 1
#define RONDB_INTERNAL_ERROR 2
#define READ_ERROR 626

extern Ndb_cluster_connection *rondb_conn[MAX_CONNECTIONS];
extern Ndb *rondb_ndb[MAX_CONNECTIONS][MAX_NDB_PER_CONNECTION];

int execute_no_commit(NdbTransaction *trans, int &ret_code, bool allow_fail);
int execute_commit(Ndb *ndb, NdbTransaction *trans, int &ret_code);
int write_formatted(char *buffer, int bufferSize, const char *format, ...);
void append_response(std::string *response, const char *app_str, Uint32 error_code);
void failed_no_such_row_error(std::string *response);
void failed_read_error(std::string *response, Uint32 error_code);
void failed_create_table(std::string *response, Uint32 error_code);
void failed_create_transaction(std::string *response, Uint32 error_code);
void failed_execute(std::string *response, Uint32 error_code);
void failed_get_operation(std::string *response);
void failed_define(std::string *response, Uint32 error_code);
void failed_large_key(std::string *response);
