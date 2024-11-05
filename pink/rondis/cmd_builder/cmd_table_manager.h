// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CMD_TABLE_MANAGER_H_
#define PIKA_CMD_TABLE_MANAGER_H_

#include <shared_mutex>
#include <thread>

#include "command.h"

class PikaCmdTableManager {

 public:
  PikaCmdTableManager();
  virtual ~PikaCmdTableManager() = default;
  void InitCmdTable(void);
  std::shared_ptr<Cmd> GetCmd(const std::string& opt);
  bool CmdExist(const std::string& cmd) const;
  CmdTable* GetCmdTable();
  uint32_t GetMaxCmdId();

 private:
  std::shared_ptr<Cmd> NewCommand(const std::string& opt);

  void InsertCurrentThreadDistributionMap();

  std::unique_ptr<CmdTable> cmds_;

  uint32_t cmdId_ = 0;

  std::shared_mutex map_protector_;

};
#endif
