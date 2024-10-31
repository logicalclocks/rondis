#include "pink/include/server_thread.h"
#include "pink/include/pink_conn.h"
#include "pink/include/redis_conn.h"
#include "pink/include/pink_thread.h"
#include "rondb.h"
#include "common.h"
#include "string/table_definitions.h"
#include "string/commands.h"

/*
    Ndb objects are not thread-safe. Hence, each worker thread / RonDB connection should
    have its own Ndb object. If we have more worker threads than cluster connections, we
    can create multiple Ndb objects from a single cluster connection.
    Essentially we want:
        num worker threads == number Ndbs objects
    whereby some cluster connections may have created more Ndb objects than others.
*/
int initialize_ndb_objects(const char *connect_string, int num_ndb_objects)
{
    Ndb_cluster_connection *rondb_conn[MAX_CONNECTIONS];

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
    }

    for (unsigned int j = 0; j < num_ndb_objects; j++)
    {
        int connection_num = j % MAX_CONNECTIONS;
        Ndb *ndb = new Ndb(rondb_conn[connection_num], REDIS_DB_NAME);
        if (ndb == nullptr)
        {
            printf("Failed creating Ndb object nr. %d for cluster connection %d\n", j, connection_num);
            return -1;
        }
        if (ndb->init() != 0)
        {
            printf("Failed initializing Ndb object nr. %d for cluster connection %d\n", j, connection_num);
            return -1;
        }
        printf("Successfully initialized Ndb object nr. %d for cluster connection %d\n", j, connection_num);
        ndb_objects[j] = ndb;
    }

    return 0;
}

int setup_rondb(const char *connect_string, int num_ndb_objects)
{
    // Creating static thread-safe Ndb objects for all connections
    ndb_init();

    int res = initialize_ndb_objects(connect_string, num_ndb_objects);
    if (res != 0)
    {
        return res;
    }

    Ndb *ndb = ndb_objects[0];
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

void print_args(const pink::RedisCmdArgsType &argv)
{
    for (const auto &arg : argv)
    {
        printf("%s ", arg.c_str());
    }
    printf("\n");
}

int rondb_redis_handler(const pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int worker_id)
{
    // First check non-ndb commands
    if (argv[0] == "ping")
    {
        if (argv.size() != 1)
        {
            char error_message[256];
            snprintf(error_message, sizeof(error_message), REDIS_WRONG_NUMBER_OF_ARGS, argv[0].c_str());
            assign_generic_err_to_response(response, error_message);
            return 0;
        }
        response->append("+PONG\r\n");
    }
    else
    {
        Ndb *ndb = ndb_objects[worker_id];
        if (argv[0] == "GET")
        {
            if (argv.size() == 2)
            {
                rondb_get_command(ndb, argv, response);
            }
            else
            {
                char error_message[256];
                snprintf(error_message, sizeof(error_message), REDIS_WRONG_NUMBER_OF_ARGS, argv[0].c_str());
                assign_generic_err_to_response(response, error_message);
            }
        }
        else if (argv[0] == "SET")
        {
            if (argv.size() == 3)
            {
                rondb_set_command(ndb, argv, response);
            }
            else
            {
                char error_message[256];
                snprintf(error_message, sizeof(error_message), REDIS_WRONG_NUMBER_OF_ARGS, argv[0].c_str());
                assign_generic_err_to_response(response, error_message);
            }
        }
        else
        {
            printf("Unsupported command: ");
            print_args(argv);
            char error_message[256];
            snprintf(error_message, sizeof(error_message), REDIS_UNKNOWN_COMMAND, argv[0].c_str());
            assign_generic_err_to_response(response, error_message);
        }
        if (ndb->getClientStat(ndb->TransStartCount) != ndb->getClientStat(ndb->TransCloseCount))
        {
            /*
                If we are here, we have a transaction that was not closed.
                Only a certain amount of transactions can be open at the same time.
                If this limit is reached, the Ndb object will not create any new ones.
                Hence, better to catch these cases early.
            */
            print_args(argv);
            printf("Number of transactions started: %lld\n", ndb->getClientStat(ndb->TransStartCount));
            printf("Number of transactions closed: %lld\n", ndb->getClientStat(ndb->TransCloseCount));
            exit(1);
        }
    }
    return 0;
}
