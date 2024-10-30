#include <stdarg.h>
#include "pink/include/redis_conn.h"
#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#include "common.h"

/**
 * @brief Writes formatted data to a buffer.
 *
 * This function writes formatted data to the provided buffer using a format string
 * and a variable number of arguments, similar to printf. It ensures that the
 * formatted string does not exceed the specified buffer size.
 *
 * @param buffer A pointer to the buffer where the formatted string will be written.
 * @param bufferSize The size of the buffer.
 * @param format A format string that specifies how subsequent arguments are converted for output.
 * @param ... Additional arguments specifying the data to be formatted.
 * @return The number of characters written, excluding the null terminator. If the output
 *         is truncated due to the buffer size limit, the return value is the number of
 *         characters (excluding the null terminator) which would have been written if
 *         enough space had been available.
 */
int write_formatted(char *buffer, int bufferSize, const char *format, ...)
{
    int len = 0;
    va_list arguments;
    va_start(arguments, format);
    len = vsnprintf(buffer, bufferSize, format, arguments);
    va_end(arguments);
    return len;
}

void assign_ndb_err_to_response(
    std::string *response,
    const char *app_str,
    NdbError error)
{
    char buf[512];
    write_formatted(buf, sizeof(buf), "-ERR %s; NDB(%u) %s\r\n", app_str, error.code, error.message);
    std::cout << buf;
    response->assign(buf);
}

void assign_generic_err_to_response(
    std::string *response,
    const char *app_str)
{
    char buf[512];
    write_formatted(buf, sizeof(buf), "-ERR %s\r\n", app_str);
    std::cout << buf;
    response->assign(buf);
}
