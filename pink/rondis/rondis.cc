#include <signal.h>

#include "pink/include/server_thread.h"
#include "pink/include/pink_conn.h"
#include "pink/include/redis_conn.h"
#include "pink/include/pink_thread.h"
#include "rondb.h"

using namespace pink;

std::map<std::string, std::string> db;

class RondisConn : public RedisConn
{
public:
    RondisConn(int fd, const std::string &ip_port, Thread *thread,
               void *worker_specific_data);
    virtual ~RondisConn() = default;

protected:
    int DealMessage(const RedisCmdArgsType &argv, std::string *response) override;

private:
};

RondisConn::RondisConn(int fd, const std::string &ip_port,
                       Thread *thread, void *worker_specific_data)
    : RedisConn(fd, ip_port, thread)
{
    // Handle worker_specific_data ...
}

int RondisConn::DealMessage(const RedisCmdArgsType &argv, std::string *response)
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
    virtual std::shared_ptr<PinkConn> NewPinkConn(int connfd, const std::string &ip_port,
                                                  Thread *thread,
                                                  void *worker_specific_data, pink::PinkEpoll *pink_epoll = nullptr) const
    {
        return std::make_shared<RondisConn>(connfd, ip_port, thread, worker_specific_data);
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
