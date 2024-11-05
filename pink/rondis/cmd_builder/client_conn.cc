// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <utility>
#include <vector>

#include "slash_string.h"

#include "command.h"
#include "cmd_table_manager.h"
#include "client_conn.h"

extern std::unique_ptr<PikaCmdTableManager> g_pika_cmd_table_manager;


std::shared_ptr<Cmd> PikaClientConn::DoCmd(const pink::RedisCmdArgsType& argv, const std::string& opt,
                                           const std::shared_ptr<std::string>& resp_ptr, bool cache_miss_in_rtc) {
  // Get command info
  std::shared_ptr<Cmd> c_ptr = g_pika_cmd_table_manager->GetCmd(opt);
  if (!c_ptr) {
    return c_ptr;
  }
  c_ptr->SetCacheMissedInRtc(cache_miss_in_rtc);
  c_ptr->SetResp(resp_ptr);

  // Initial
  c_ptr->Initial(argv);
  if (!c_ptr->res().ok()) {
    return c_ptr;
  }

  int8_t subCmdIndex = -1;
  std::string errKey;
  std::string cmdName = c_ptr->name();

  if (IsInTxn() && opt != kCmdNameExec && opt != kCmdNameWatch && opt != kCmdNameDiscard && opt != kCmdNameMulti) {
    PushCmdToQue(c_ptr);
    c_ptr->res().SetRes(CmdRes::kTxnQueued);
    return c_ptr;
  }

  // Process Command
  c_ptr->Execute();

  return c_ptr;
}

void PikaClientConn::TryWriteResp() {
  int expected = 0;
  if (resp_num.compare_exchange_strong(expected, -1)) {
    for (auto& resp : resp_array) {
      WriteResp(*resp);
    }
    if (write_completed_cb_) {
      write_completed_cb_();
      write_completed_cb_ = nullptr;
    }
    resp_array.clear();
    NotifyEpoll(true);
  }
}

void PikaClientConn::PushCmdToQue(std::shared_ptr<Cmd> cmd) { txn_cmd_que_.push(cmd); }

void PikaClientConn::ClearTxnCmdQue() { txn_cmd_que_ = std::queue<std::shared_ptr<Cmd>>{}; }

void PikaClientConn::ExitTxn() {
  if (IsInTxn()) {
    ClearTxnCmdQue();
    std::lock_guard<std::mutex> lg(txn_state_mu_);
    txn_state_.reset();
  }
}

void PikaClientConn::ExecRedisCmd(const pink::RedisCmdArgsType& argv, std::shared_ptr<std::string>& resp_ptr,
                                  bool cache_miss_in_rtc) {
  // get opt
  std::string opt = argv[0];
  slash::StringToLower(opt);
  if (opt == kClusterPrefix) {
    if (argv.size() >= 2) {
      opt += argv[1];
      slash::StringToLower(opt);
    }
  }

  std::shared_ptr<Cmd> cmd_ptr = DoCmd(argv, opt, resp_ptr, cache_miss_in_rtc);
  *resp_ptr = std::move(cmd_ptr->res().message());
  resp_num--;
}

std::queue<std::shared_ptr<Cmd>> PikaClientConn::GetTxnCmdQue() { return txn_cmd_que_; }
