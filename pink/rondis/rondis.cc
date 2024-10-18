#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include <atomic>
#include <map>

#include "pink/include/server_thread.h"
#include "pink/include/pink_conn.h"
#include "pink/include/redis_conn.h"
#include "pink/include/pink_thread.h"
#include "common.h"
#include "string/init.h"
#include "string/commands.h"

using namespace pink;

std::map<std::string, std::string> db;

Ndb_cluster_connection *rondb_conn[MAX_CONNECTIONS];
Ndb *rondb_ndb[MAX_CONNECTIONS][MAX_NDB_PER_CONNECTION];

/*
    All STRING commands: https://redis.io/docs/latest/commands/?group=string
*/
class RondisConn : public RedisConn
{
public:
    RondisConn(int fd, const std::string &ip_port, ServerThread *thread,
               void *worker_specific_data);
    virtual ~RondisConn() = default;

protected:
    int DealMessage(RedisCmdArgsType &argv, std::string *response) override;

private:
};

RondisConn::RondisConn(int fd, const std::string &ip_port,
                       ServerThread *thread, void *worker_specific_data)
    : RedisConn(fd, ip_port, thread)
{
    // Handle worker_specific_data ...
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

int RondisConn::DealMessage(RedisCmdArgsType &argv, std::string *response)
{
    printf("Get redis message ");
    for (int i = 0; i < argv.size(); i++)
    {
        printf("%s ", argv[i].c_str());
    }
    printf("\n");
    return rondb_redis_handler(argv, response, 0);
}

class RondisConnFactory : public ConnFactory
{
public:
    virtual PinkConn *NewPinkConn(int connfd, const std::string &ip_port,
                                  ServerThread *thread,
                                  void *worker_specific_data) const
    {
        return new RondisConn(connfd, ip_port, thread, worker_specific_data);
    }
};

static std::atomic<bool> running(false);

static void IntSigHandle(const int sig)
{
    printf("Catch Signal %d, cleanup...\n", sig);
    running.store(false);
    printf("server Exit");
}

static void SignalSetup()
{
    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, &IntSigHandle);
    signal(SIGQUIT, &IntSigHandle);
    signal(SIGTERM, &IntSigHandle);
}

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

    res = init_key_record_specs(dict);
    if (res != 0)
    {
        return res;
    }

    return init_value_record_specs(dict);
}

void rondb_end()
{
    ndb_end(0);
}

int main(int argc, char *argv[])
{
    int port = 6379;
    char *connect_string = "localhost:13000";
    if (argc != 3)
    {
        printf("Not receiving 2 arguments, just using defaults\n");
    }
    else
    {
        port = atoi(argv[1]);
        connect_string = argv[2];
    }
    printf("Server will listen to %d and connect to MGMd at %s\n", port, connect_string);

    // TODO: Distribute resources across pink threads
    if (setup_rondb(connect_string) != 0)
    {
        printf("Failed to setup RonDB environment\n");
        return -1;
    }
    SignalSetup();

    ConnFactory *conn_factory = new RondisConnFactory();

    ServerThread *my_thread = NewHolyThread(port, conn_factory, 1000);
    if (my_thread->StartThread() != 0)
    {
        printf("StartThread error happened!\n");
        rondb_end();
        return -1;
    }

    running.store(true);
    while (running.load())
    {
        sleep(1);
    }
    my_thread->StopThread();

    delete my_thread;
    delete conn_factory;

    rondb_end();

    return 0;
}
