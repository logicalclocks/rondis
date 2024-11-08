#include <stdarg.h>
#include "pink/include/redis_conn.h"
#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#include "common.h"

void assign_ndb_err_to_response(
    std::string *response,
    const char *app_str,
    NdbError error)
{
    char buf[512];
    snprintf(buf, sizeof(buf), "-ERR %s; NDB(%u) %s\r\n", app_str, error.code, error.message);
    std::cout << buf;
    response->assign(buf);
}

void assign_generic_err_to_response(
    std::string *response,
    const char *app_str)
{
    char buf[512];
    snprintf(buf, sizeof(buf), "-ERR %s\r\n", app_str);
    std::cout << buf;
    response->assign(buf);
}
