#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include "pink/include/redis_conn.h"
#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#include "db_operations.h"

/*
    All STRING commands:
    https://redis.io/docs/latest/commands/?group=string

    STYLE GUIDE:
    From a RonDB perspective, this file is where transactions are created and
    removed again. The data operations inbetween are done in db_operations.
    This avoids redundant calls to `ndb->closeTransaction(trans);` and therefore
    also reduces the risk to forget calling this function (causing memory leaks).

    The db_operations level however handles most of the low-level NdbError handling.
    Most importantly, it writes Ndb error messages to the response string. This may
    however change in the future, since this causes redundancy.
*/

void rondb_get_command(Ndb *ndb,
                       const pink::RedisCmdArgsType &argv,
                       std::string *response);

void rondb_set_command(Ndb *ndb,
                       const pink::RedisCmdArgsType &argv,
                       std::string *response);
