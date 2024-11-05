// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CLIENT_CONN_H_
#define PIKA_CLIENT_CONN_H_

#include <bitset>
#include <utility>

#include "command.h"

class PikaClientConn : public pink::RedisConn {
 public:
  using WriteCompleteCallback = std::function<void()>;

  PikaClientConn(int fd, const std::string& ip_port, pink::Thread* server_thread, pink::NetMultiplexer* mpx,
                 const pink::HandleType& handle_type, int max_conn_rbuf_size);
  ~PikaClientConn() = default;

  int DealMessage(const pink::RedisCmdArgsType& argv, std::string* response) override { return 0; }

  void SetWriteCompleteCallback(WriteCompleteCallback cb) { write_completed_cb_ = std::move(cb); }

  // Txn
  std::queue<std::shared_ptr<Cmd>> GetTxnCmdQue();
  void PushCmdToQue(std::shared_ptr<Cmd> cmd);
  void ClearTxnCmdQue();
  void ExitTxn();

  pink::ServerThread* server_thread() { return server_thread_; }

  std::atomic<int> resp_num;
  std::vector<std::shared_ptr<std::string>> resp_array;

 private:
  pink::ServerThread* const server_thread_;
  WriteCompleteCallback write_completed_cb_;
  bool is_pubsub_ = false;
  std::queue<std::shared_ptr<Cmd>> txn_cmd_que_;
  std::bitset<16> txn_state_;
  std::unordered_set<std::string> watched_db_keys_;
  std::mutex txn_state_mu_;

  bool authenticated_ = false;

  std::shared_ptr<Cmd> DoCmd(const pink::RedisCmdArgsType& argv, const std::string& opt,
                             const std::shared_ptr<std::string>& resp_ptr, bool cache_miss_in_rtc);

  void ExecRedisCmd(const pink::RedisCmdArgsType& argv, std::shared_ptr<std::string>& resp_ptr, bool cache_miss_in_rtc);
  void TryWriteResp();
};

#endif
