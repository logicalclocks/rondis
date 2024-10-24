#include <stdio.h>
#include "pink/include/redis_conn.h"
#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

extern std::vector<Ndb *> ndb_objects;

int initialize_ndb_objects(const char *connect_string, int num_ndb_objects);

int setup_rondb(const char *connect_string, int num_ndb_objects);

void rondb_end();

int rondb_redis_handler(const pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int fd);
