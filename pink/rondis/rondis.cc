#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include <atomic>
#include <map>

#include "pink/include/server_thread.h"
#include "pink/include/pink_conn.h"
#include "pink/include/redis_conn.h"
#include "pink/include/pink_thread.h"
#include "rondis_handler.cc"

using namespace pink;

std::map<std::string, std::string> db;

/*
    All STRING commands: https://redis.io/docs/latest/commands/?group=string
*/
class RondisConn: public RedisConn {
 public:
  RondisConn(int fd, const std::string& ip_port, ServerThread *thread,
         void* worker_specific_data);
  virtual ~RondisConn() = default;

 protected:
  int DealMessage(RedisCmdArgsType& argv, std::string* response) override;

 private:
};

RondisConn::RondisConn(int fd, const std::string& ip_port,
               ServerThread *thread, void* worker_specific_data)
    : RedisConn(fd, ip_port, thread) {
  // Handle worker_specific_data ...
}

int RondisConn::DealMessage(RedisCmdArgsType& argv, std::string* response) {
  printf("Get redis message ");
  for (int i = 0; i < argv.size(); i++) {
    printf("%s ", argv[i].c_str());
  }
  printf("\n");
  return rondb_redis_handler(argv, response, 0);
}

class RondisConnFactory : public ConnFactory {
 public:
  virtual PinkConn *NewPinkConn(int connfd, const std::string &ip_port,
                                ServerThread *thread,
                                void* worker_specific_data) const {
    return new RondisConn(connfd, ip_port, thread, worker_specific_data);
  }
};

static std::atomic<bool> running(false);

static void IntSigHandle(const int sig) {
  printf("Catch Signal %d, cleanup...\n", sig);
  running.store(false);
  printf("server Exit");
}

static void SignalSetup() {
  signal(SIGHUP, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, &IntSigHandle);
  signal(SIGQUIT, &IntSigHandle);
  signal(SIGTERM, &IntSigHandle);
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    printf("server will listen to 6379\n");
  } else {
    printf("server will listen to %d\n", atoi(argv[1]));
  }
  int my_port = (argc > 1) ? atoi(argv[1]) : 6379;

  SignalSetup();

  ConnFactory *conn_factory = new RondisConnFactory();

  ServerThread* my_thread = NewHolyThread(my_port, conn_factory, 1000);
  if (my_thread->StartThread() != 0) {
    printf("StartThread error happened!\n");
    exit(-1);
  }
  running.store(true);
  while (running.load()) {
    sleep(1);
  }
  my_thread->StopThread();

  delete my_thread;
  delete conn_factory;

  return 0;
}
