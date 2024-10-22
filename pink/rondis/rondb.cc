#include "pink/include/server_thread.h"
#include "pink/include/pink_conn.h"
#include "pink/include/redis_conn.h"
#include "pink/include/pink_thread.h"
#include "rondb.h"
#include "common.h"
#include "string/table_definitions.h"
#include "string/commands.h"

Ndb_cluster_connection *rondb_conn[MAX_CONNECTIONS];
Ndb *rondb_ndb[MAX_CONNECTIONS][MAX_NDB_PER_CONNECTION];

int initialize_connections(const char *connect_string)
{
    for (unsigned int i = 0; i < MAX_CONNECTIONS; i++)
    {
        rondb_conn[i] = new Ndb_cluster_connection(connect_string);
        if (rondb_conn[i]->connect() != 0)
        {
            printf("Failed with RonDB MGMd connection nr. %d\n", i);
            return -1;
        }
        printf("RonDB MGMd connection nr. %d is ready\n", i);
        if (rondb_conn[i]->wait_until_ready(30, 0) != 0)
        {
            printf("Failed with RonDB data node connection nr. %d\n", i);
            return -1;
        }
        printf("RonDB data node connection nr. %d is ready\n", i);
        for (unsigned int j = 0; j < MAX_NDB_PER_CONNECTION; j++)
        {
            Ndb *ndb = new Ndb(rondb_conn[i], REDIS_DB_NAME);
            if (ndb == nullptr)
            {
                printf("Failed creating Ndb object nr. %d for cluster connection %d\n", j, i);
                return -1;
            }
            if (ndb->init() != 0)
            {
                printf("Failed initializing Ndb object nr. %d for cluster connection %d\n", j, i);
                return -1;
            }
            printf("Successfully initialized Ndb object nr. %d for cluster connection %d\n", j, i);
            rondb_ndb[i][j] = ndb;
        }
    }
    return 0;
}

int setup_rondb(const char *connect_string)
{
    // Creating static thread-safe Ndb objects for all connections
    ndb_init();

    int res = initialize_connections(connect_string);
    if (res != 0)
    {
        return res;
    }

    Ndb *ndb = rondb_ndb[0][0];
    NdbDictionary::Dictionary *dict = ndb->getDictionary();

    if (init_string_records(dict) != 0)
    {
        printf("Failed initializing records for Redis data type STRING; error: %s\n",
               ndb->getNdbError().message);
        return -1;
    }

    return 0;
}

void rondb_end()
{
    ndb_end(0);
}

int rondb_redis_handler(const pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int fd)
{
    if (argv[0] == "GET")
    {
        if (argv.size() != 2)
        {
            printf("Invalid number of arguments for GET command\n");
            return -1;
        }
        rondb_get_command(argv, response, fd);
    }
    else if (argv[0] == "SET")
    {
        if (argv.size() != 3)
        {
            printf("Invalid number of arguments for SET command\n");
            return -1;
        }
        rondb_set_command(argv, response, fd);
    }
    else
    {
        printf("Unsupported command\n");
        return -1;
    }
    return 0;
}
