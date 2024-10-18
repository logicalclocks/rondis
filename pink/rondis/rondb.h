#include <stdio.h>
#include "pink/include/redis_conn.h"

int initialize_connections(const char *connect_string);

int setup_rondb(const char *connect_string);

void rondb_end();

int rondb_redis_handler(pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int fd);
