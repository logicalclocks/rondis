#include <stdarg.h>
#include "pink/include/redis_conn.h"
#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#include "common.h"

#define FOREIGN_KEY_RESTRICT_ERROR 256


int execute_no_commit(NdbTransaction *trans, int &ret_code, bool allow_fail)
{
    printf("Execute NoCommit\n");
    if (trans->execute(NdbTransaction::NoCommit) != 0)
    {
        ret_code = trans->getNdbError().code;
        return -1;
    }
    return 0;
}

int execute_commit(Ndb *ndb, NdbTransaction *trans, int &ret_code)
{
    // printf("Execute transaction\n");
    if (trans->execute(NdbTransaction::Commit) != 0)
    {
        ret_code = trans->getNdbError().code;
        return -1;
    }
    ndb->closeTransaction(trans);
    return 0;
}

int write_formatted(char *buffer, int bufferSize, const char *format, ...)
{
    int len = 0;
    va_list arguments;
    va_start(arguments, format);
    len = vsnprintf(buffer, bufferSize, format, arguments);
    va_end(arguments);
    return len;
}

void append_response(std::string *response, const char *app_str, Uint32 error_code)
{
    char buf[512];
    printf("Add %s to response, error: %u\n", app_str, error_code);
    if (error_code == 0)
    {
        write_formatted(buf, sizeof(buf), "-%s\r\n", app_str);
        response->append(app_str);
    }
    else
    {
        write_formatted(buf, sizeof(buf), "-%s: %u\r\n", app_str, error_code);
        response->append(app_str);
    }
}

void failed_no_such_row_error(std::string *response)
{
    // Redis stuff
    response->append("$-1\r\n");
}

void failed_read_error(std::string *response, Uint32 error_code)
{
    append_response(response,
                    "RonDB Error: Failed to read key, code:",
                    error_code);
}

void failed_create_table(std::string *response, Uint32 error_code)
{
    append_response(response,
                    "RonDB Error: Failed to create table object",
                    error_code);
}

void failed_create_transaction(std::string *response,
                               Uint32 error_code)
{
    append_response(response,
                    "RonDB Error: Failed to create transaction object",
                    error_code);
}

void failed_execute(std::string *response, Uint32 error_code)
{
    append_response(response,
                    "RonDB Error: Failed to execute transaction, code:",
                    error_code);
}

void failed_get_operation(std::string *response)
{
    response->append("-RonDB Error: Failed to get NdbOperation object\r\n");
}

void failed_define(std::string *response, Uint32 error_code)
{
    append_response(response,
                    "RonDB Error: Failed to define RonDB operation, code:",
                    error_code);
}

void failed_large_key(std::string *response)
{
    response->append("-RonDB Error: Support up to 3000 bytes long keys\r\n");
}