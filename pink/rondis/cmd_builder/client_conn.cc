// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <utility>
#include <vector>

#include "command.h"
#include "cmd_table_manager.h"
#include "client_conn.h"

extern std::unique_ptr<PikaCmdTableManager> g_pika_cmd_table_manager;


std::shared_ptr<Cmd> PikaClientConn::DoCmd(const PikaCmdArgsType& argv, const std::string& opt,
                                           const std::shared_ptr<std::string>& resp_ptr, bool cache_miss_in_rtc) {
  // Get command info
  std::shared_ptr<Cmd> c_ptr = g_pika_cmd_table_manager->GetCmd(opt);
  if (!c_ptr) {
    std::shared_ptr<Cmd> tmp_ptr = std::make_shared<DummyCmd>(DummyCmd());
    tmp_ptr->res().SetRes(CmdRes::kErrOther, "unknown command \"" + opt + "\"");
    return tmp_ptr;
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

  if (c_ptr->is_write()) {
    std::vector<std::string> cur_key = c_ptr->current_key();
    if (cur_key.empty() && opt != kCmdNameExec) {
      c_ptr->res().SetRes(CmdRes::kErrOther, "Internal ERROR");
      return c_ptr;
    }
  } else if (c_ptr->is_read() && c_ptr->flag_ == 0) {
    const auto& server_guard = std::lock_guard(g_pika_server->GetDBLock());
    int role = 0;
    auto status = g_pika_rm->CheckDBRole(current_db_, &role);
    if (!status.ok()) {
      c_ptr->res().SetRes(CmdRes::kErrOther, "Internal ERROR");
      return c_ptr;
    } else if ((role & PIKA_ROLE_SLAVE) == PIKA_ROLE_SLAVE) {
      const auto& slave_db = g_pika_rm->GetSyncSlaveDBByName(DBInfo(current_db_));
      if (!slave_db) {
        c_ptr->res().SetRes(CmdRes::kErrOther, "Internal ERROR");
        return c_ptr;
      } else if (slave_db->State() != ReplState::kConnected) {
        c_ptr->res().SetRes(CmdRes::kErrOther, "Full sync not completed");
        return c_ptr;
      }
    }
  }

  if (c_ptr->res().ok() && c_ptr->is_write() && name() != kCmdNameExec) {
    if (c_ptr->name() == kCmdNameFlushdb) {
      auto flushdb = std::dynamic_pointer_cast<FlushdbCmd>(c_ptr);
      SetTxnFailedIfKeyExists(flushdb->GetFlushDBname());
    } else if (c_ptr->name() == kCmdNameFlushall) {
      SetTxnFailedIfKeyExists();
    } else {
      auto table_keys = c_ptr->current_key();
      for (auto& key : table_keys) {
        key = c_ptr->db_name().append("_").append(key);
      }
      SetTxnFailedFromKeys(table_keys);
    }
  }

  // Process Command
  c_ptr->Execute();
  time_stat_->process_done_ts_ = pstd::NowMicros();
  auto cmdstat_map = g_pika_cmd_table_manager->GetCommandStatMap();
  (*cmdstat_map)[opt].cmd_count.fetch_add(1);
  (*cmdstat_map)[opt].cmd_time_consuming.fetch_add(time_stat_->total_time());

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

bool PikaClientConn::IsInTxn() {
  std::lock_guard<std::mutex> lg(txn_state_mu_);
  return txn_state_[TxnStateBitMask::Start];
}

bool PikaClientConn::IsTxnInitFailed() {
  std::lock_guard<std::mutex> lg(txn_state_mu_);
  return txn_state_[TxnStateBitMask::InitCmdFailed];
}

bool PikaClientConn::IsTxnWatchFailed() {
  std::lock_guard<std::mutex> lg(txn_state_mu_);
  return txn_state_[TxnStateBitMask::WatchFailed];
}

bool PikaClientConn::IsTxnExecing() {
  std::lock_guard<std::mutex> lg(txn_state_mu_);
  return txn_state_[TxnStateBitMask::Execing] && txn_state_[TxnStateBitMask::Start];
}

void PikaClientConn::SetTxnWatchFailState(bool is_failed) {
  std::lock_guard<std::mutex> lg(txn_state_mu_);
  txn_state_[TxnStateBitMask::WatchFailed] = is_failed;
}

void PikaClientConn::SetTxnInitFailState(bool is_failed) {
  std::lock_guard<std::mutex> lg(txn_state_mu_);
  txn_state_[TxnStateBitMask::InitCmdFailed] = is_failed;
}

void PikaClientConn::SetTxnStartState(bool is_start) {
  std::lock_guard<std::mutex> lg(txn_state_mu_);
  txn_state_[TxnStateBitMask::Start] = is_start;
}

void PikaClientConn::ClearTxnCmdQue() { txn_cmd_que_ = std::queue<std::shared_ptr<Cmd>>{}; }

void PikaClientConn::AddKeysToWatch(const std::vector<std::string>& db_keys) {
  for (const auto& it : db_keys) {
    watched_db_keys_.emplace(it);
  }

  auto dispatcher = dynamic_cast<net::DispatchThread*>(server_thread());
  if (dispatcher != nullptr) {
    dispatcher->AddWatchKeys(watched_db_keys_, shared_from_this());
  }
}

void PikaClientConn::RemoveWatchedKeys() {
  auto dispatcher = dynamic_cast<net::DispatchThread*>(server_thread());
  if (dispatcher != nullptr) {
    watched_db_keys_.clear();
    dispatcher->RemoveWatchKeys(shared_from_this());
  }
}

void PikaClientConn::SetTxnFailedFromKeys(const std::vector<std::string>& db_keys) {
  auto dispatcher = dynamic_cast<net::DispatchThread*>(server_thread());
  if (dispatcher != nullptr) {
    auto involved_conns = std::vector<std::shared_ptr<NetConn>>{};
    involved_conns = dispatcher->GetInvolvedTxn(db_keys);
    for (auto& conn : involved_conns) {
      if (auto c = std::dynamic_pointer_cast<PikaClientConn>(conn); c != nullptr) {
        c->SetTxnWatchFailState(true);
      }
    }
  }
}

// if key in target_db exists, then the key been watched multi will be failed
void PikaClientConn::SetTxnFailedIfKeyExists(std::string target_db_name) {
  auto dispatcher = dynamic_cast<net::DispatchThread*>(server_thread());
  if (dispatcher == nullptr) {
    return;
  }
  auto involved_conns = dispatcher->GetAllTxns();
  for (auto& conn : involved_conns) {
    std::shared_ptr<PikaClientConn> c;
    if (c = std::dynamic_pointer_cast<PikaClientConn>(conn); c == nullptr) {
      continue;
    }

    for (const auto& db_key : c->watched_db_keys_) {
      size_t pos = db_key.find('_');
      if (pos == std::string::npos) {
        continue;
      }

      auto db_name = db_key.substr(0, pos);
      auto key = db_key.substr(pos + 1);

      if (target_db_name == "" || target_db_name == "all" || target_db_name == db_name) {
        auto db = g_pika_server->GetDB(db_name);
        // if watched key exists, set watch state to failed
        if (db->storage()->Exists({key}) > 0) {
          c->SetTxnWatchFailState(true);
          break;
        }
      }
    }
  }
}

void PikaClientConn::ExitTxn() {
  if (IsInTxn()) {
    RemoveWatchedKeys();
    ClearTxnCmdQue();
    std::lock_guard<std::mutex> lg(txn_state_mu_);
    txn_state_.reset();
  }
}

void PikaClientConn::ExecRedisCmd(const PikaCmdArgsType& argv, std::shared_ptr<std::string>& resp_ptr,
                                  bool cache_miss_in_rtc) {
  // get opt
  std::string opt = argv[0];
  pstd::StringToLower(opt);
  if (opt == kClusterPrefix) {
    if (argv.size() >= 2) {
      opt += argv[1];
      pstd::StringToLower(opt);
    }
  }

  std::shared_ptr<Cmd> cmd_ptr = DoCmd(argv, opt, resp_ptr, cache_miss_in_rtc);
  *resp_ptr = std::move(cmd_ptr->res().message());
  resp_num--;
}

std::queue<std::shared_ptr<Cmd>> PikaClientConn::GetTxnCmdQue() { return txn_cmd_que_; }

// compare addr in ClientInfo
bool AddrCompare(const ClientInfo& lhs, const ClientInfo& rhs) { return rhs.ip_port < lhs.ip_port; }

bool IdleCompare(const ClientInfo& lhs, const ClientInfo& rhs) { return lhs.last_interaction < rhs.last_interaction; }
