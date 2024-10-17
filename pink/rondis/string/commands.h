#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include "pink/include/redis_conn.h"
#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#include "db_interactions.h"

void rondb_get_command(pink::RedisCmdArgsType &argv,
                       std::string *response,
                       int fd);

void rondb_set_command(pink::RedisCmdArgsType &argv,
                       std::string *response,
                       int fd);