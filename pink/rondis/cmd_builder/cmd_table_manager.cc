// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "cmd_table_manager.h"

#include <sys/syscall.h>
#include <unistd.h>


PikaCmdTableManager::PikaCmdTableManager() {
  cmds_ = std::make_unique<CmdTable>();
  cmds_->reserve(300);
}

void PikaCmdTableManager::InitCmdTable(void) {
  ::InitCmdTable(cmds_.get());
}

std::shared_ptr<Cmd> PikaCmdTableManager::GetCmd(const std::string& opt) {
  const std::string& internal_opt = opt;
  return NewCommand(internal_opt);
}

std::shared_ptr<Cmd> PikaCmdTableManager::NewCommand(const std::string& opt) {
  Cmd* cmd = GetCmdFromDB(opt, *cmds_);
  if (cmd) {
    return std::shared_ptr<Cmd>(cmd->Clone());
  }
  return nullptr;
}

CmdTable* PikaCmdTableManager::GetCmdTable() { return cmds_.get(); }

uint32_t PikaCmdTableManager::GetMaxCmdId() { return cmdId_; }

bool PikaCmdTableManager::CmdExist(const std::string& cmd) const { return cmds_->find(cmd) != cmds_->end(); }
