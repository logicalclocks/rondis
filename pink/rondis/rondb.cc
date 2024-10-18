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
            Ndb *ndb = new Ndb(rondb_conn[i], "redis_0");
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

    return init_string_records(dict);
}

void rondb_end()
{
    ndb_end(0);
}

int rondb_redis_handler(pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int fd)
{
    if (argv.size() == 0)
    {
        return -1;
    }
    const char *cmd_str = argv[0].c_str();
    unsigned int cmd_len = strlen(cmd_str);
    if (cmd_len == 3)
    {
        const char *set_str = "set";
        const char *get_str = "get";
        if (memcmp(cmd_str, get_str, 3) == 0)
        {
            rondb_get_command(argv, response, fd);
        }
        else if (memcmp(cmd_str, set_str, 3) == 0)
        {
            rondb_set_command(argv, response, fd);
        }
        return 0;
    }
    else if (cmd_len == 1)
    {
        const char *shutdown_str = "shutdown";
        if (memcmp(cmd_str, shutdown_str, 8) == 0)
        {
            printf("Shutdown Rondis server\n");
            return -1;
        }
    }
    return -1;
}