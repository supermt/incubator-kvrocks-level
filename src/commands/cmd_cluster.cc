/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include "cluster/cluster_defs.h"
#include "cluster/slot_import.h"
#include "cluster/sync_migrate_context.h"
#include "commander.h"
#include "error_constants.h"
#include "time_util.h"

namespace redis {

class CommandCluster : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = util::ToLower(args[1]);

    if (args.size() == 2 && (subcommand_ == "nodes" || subcommand_ == "slots" || subcommand_ == "info"))
      return Status::OK();

    if (subcommand_ == "keyslot" && args_.size() == 3) return Status::OK();

    if (subcommand_ == "import") {
      if (args.size() != 4) return {Status::RedisParseErr, errWrongNumOfArguments};
      slot_ = GET_OR_RET(ParseInt<int64_t>(args[2], 10));

      auto state = ParseInt<unsigned>(args[3], {kImportStart, kImportNone}, 10);
      if (!state) return {Status::NotOK, "Invalid import state"};

      state_ = static_cast<ImportStatus>(*state);
      return Status::OK();
    }

    return {Status::RedisParseErr, "CLUSTER command, CLUSTER INFO|NODES|SLOTS|KEYSLOT"};
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!svr->GetConfig()->cluster_enabled) {
      *output = redis::Error("Cluster mode is not enabled");
      return Status::OK();
    }

    if (!conn->IsAdmin()) {
      *output = redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }

    if (subcommand_ == "keyslot") {
      auto slot_id = GetSlotIdFromKey(args_[2]);
      *output = redis::Integer(slot_id);
    } else if (subcommand_ == "slots") {
      std::vector<SlotInfo> infos;
      Status s = svr->cluster->GetSlotsInfo(&infos);
      if (s.IsOK()) {
        output->append(redis::MultiLen(infos.size()));
        for (const auto &info : infos) {
          output->append(redis::MultiLen(info.nodes.size() + 2));
          output->append(redis::Integer(info.start));
          output->append(redis::Integer(info.end));
          for (const auto &n : info.nodes) {
            output->append(redis::MultiLen(3));
            output->append(redis::BulkString(n.host));
            output->append(redis::Integer(n.port));
            output->append(redis::BulkString(n.id));
          }
        }
      } else {
        *output = redis::Error(s.Msg());
      }
    } else if (subcommand_ == "nodes") {
      std::string nodes_desc;
      Status s = svr->cluster->GetClusterNodes(&nodes_desc);
      if (s.IsOK()) {
        *output = redis::BulkString(nodes_desc);
      } else {
        *output = redis::Error(s.Msg());
      }
    } else if (subcommand_ == "info") {
      std::string cluster_info;
      Status s = svr->cluster->GetClusterInfo(&cluster_info);
      if (s.IsOK()) {
        *output = redis::BulkString(cluster_info);
      } else {
        *output = redis::Error(s.Msg());
      }
    } else if (subcommand_ == "import") {
      Status s = svr->cluster->ImportSlot(conn, static_cast<int>(slot_), state_);
      if (s.IsOK()) {
        *output = redis::SimpleString("OK");
      } else {
        *output = redis::Error(s.Msg());
      }
    } else {
      *output = redis::Error("Invalid cluster command options");
    }
    return Status::OK();
  }

 private:
  std::string subcommand_;
  int64_t slot_ = -1;
  ImportStatus state_ = kImportNone;
};

class CommandClusterX : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = util::ToLower(args[1]);

    if (args.size() == 2 && (subcommand_ == "version")) return Status::OK();

    if (subcommand_ == "setnodeid" && args_.size() == 3 && args_[2].size() == kClusterNodeIdLen) return Status::OK();

    //    if (subcommand_ == "migrate") {
    //      if (args.size() < 4 || args.size() > 6) return {Status::RedisParseErr, errWrongNumOfArguments};
    //
    //      slot_ = GET_OR_RET(ParseInt<int64_t>(args[2], 10));
    //
    //      dst_node_id_ = args[3];
    //

    //      return Status::OK();
    //    }

    if (subcommand_ == "migrate") {
      if (args.size() < 4 || args.size() > 6) return {Status::RedisParseErr, errWrongNumOfArguments};
      if (args.size() == 4) {
        std::string slot_str = args[2];
        if (slot_str.back() == ',') slot_str.pop_back();
        auto slot_list = util::Split(slot_str, ",");
        if (slot_list.size() > 1) {
          slot_ = -1;
          for (const auto &slot : slot_list) {
            int temp = GET_OR_RET(ParseInt<int64_t>(slot, 10));
            slots_.push_back(temp);
          }
        } else {
          slot_ = GET_OR_RET(ParseInt<int64_t>(slot_str, 10));
          slots_.push_back(slot_);
        }
        dst_node_id_ = args[3];
        return Status::OK();
      }
      if (args.size() >= 5) {
        auto sync_flag = util::ToLower(args[4]);
        if (sync_flag == "async") {
          sync_migrate_ = false;

          if (args.size() == 6) {
            return {Status::RedisParseErr, "Async migration does not support timeout"};
          }
        } else if (sync_flag == "sync") {
          sync_migrate_ = true;

          if (args.size() == 6) {
            auto parse_result = ParseInt<int>(args[5], 10);
            if (!parse_result) {
              return {Status::RedisParseErr, "timeout is not an integer or out of range"};
            }
            if (*parse_result < 0) {
              return {Status::RedisParseErr, errTimeoutIsNegative};
            }
            sync_migrate_timeout_ = *parse_result;
          }
        } else {
          return {Status::RedisParseErr, "Invalid sync flag"};
        }
      }
    }

    if (subcommand_ == "setnodes" && args_.size() >= 4) {
      nodes_str_ = args_[2];

      auto parse_result = ParseInt<int64_t>(args[3], 10);
      if (!parse_result) {
        return {Status::RedisParseErr, "Invalid version"};
      }

      set_version_ = *parse_result;

      if (args_.size() == 4) return Status::OK();

      if (args_.size() == 5 && strcasecmp(args_[4].c_str(), "force") == 0) {
        force_ = true;
        return Status::OK();
      }

      return {Status::RedisParseErr, "Invalid setnodes options"};
    }

    // CLUSTERX SETSLOT $SLOT_ID NODE $NODE_ID $VERSION
    if (subcommand_ == "setslot" && args_.size() == 6) {
      Status s = CommanderHelper::ParseSlotRanges(args_[2], slot_ranges_);
      if (!s.IsOK()) {
        return s;
      }

      if (strcasecmp(args_[3].c_str(), "node") != 0) {
        return {Status::RedisParseErr, "Invalid setslot options"};
      }

      if (args_[4].size() != kClusterNodeIdLen) {
        return {Status::RedisParseErr, "Invalid node id"};
      }

      auto parse_version = ParseInt<int64_t>(args[5], 10);
      if (!parse_version) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      if (*parse_version < 0) return {Status::RedisParseErr, "Invalid version"};

      set_version_ = *parse_version;

      return Status::OK();
    }

    return {Status::RedisParseErr, "CLUSTERX command, CLUSTERX VERSION|SETNODEID|SETNODES|SETSLOT|MIGRATE"};
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!svr->GetConfig()->cluster_enabled) {
      *output = redis::Error("Cluster mode is not enabled");
      return Status::OK();
    }

    if (!conn->IsAdmin()) {
      *output = redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }

    bool need_persist_nodes_info = false;
    if (subcommand_ == "setnodes") {
      Status s = svr->cluster->SetClusterNodes(nodes_str_, set_version_, force_);
      if (s.IsOK()) {
        need_persist_nodes_info = true;
        *output = redis::SimpleString("OK");
      } else {
        *output = redis::Error(s.Msg());
      }
    } else if (subcommand_ == "setnodeid") {
      Status s = svr->cluster->SetNodeId(args_[2]);
      if (s.IsOK()) {
        need_persist_nodes_info = true;
        *output = redis::SimpleString("OK");
      } else {
        *output = redis::Error(s.Msg());
      }
    } else if (subcommand_ == "setslot") {
      Status s = svr->cluster->SetSlotRanges(slot_ranges_, args_[4], set_version_);
      if (s.IsOK()) {
        need_persist_nodes_info = true;
        *output = redis::SimpleString("OK");
      } else {
        *output = redis::Error(s.Msg());
      }
    } else if (subcommand_ == "version") {
      int64_t v = svr->cluster->GetVersion();
      *output = redis::BulkString(std::to_string(v));
    } else if (subcommand_ == "migrate") {
      Status s;
      std::cout << "Is batched? " << svr->slot_migrator->IsBatched()
                << ", migration name: " << svr->slot_migrator->GetName() << std::endl;
      if (!svr->slot_migrator->IsBatched()) {
        s = svr->cluster->MigrateSlot(static_cast<int>(slot_), dst_node_id_, sync_migrate_ctx_.get());
      } else {
        s = svr->cluster->MigrateSlots(slots_, dst_node_id_);
      }

      if (s.IsOK()) {
        *output = redis::SimpleString("OK");
      } else {
        *output = redis::Error(s.Msg());
      }
    } else {
      *output = redis::Error("Invalid cluster command options");
    }
    if (need_persist_nodes_info && svr->GetConfig()->persist_cluster_nodes_enabled) {
      return svr->cluster->DumpClusterNodes(svr->GetConfig()->NodesFilePath());
    }
    return Status::OK();
  }

 private:
  std::string subcommand_;
  std::string nodes_str_;
  std::string dst_node_id_;
  int64_t set_version_ = 0;
  int64_t slot_ = -1;
  std::vector<int> slots_;
  std::vector<SlotRange> slot_ranges_;
  bool force_ = false;

  bool sync_migrate_ = true;
  int sync_migrate_timeout_ = 0;
  std::unique_ptr<SyncMigrateContext> sync_migrate_ctx_ = nullptr;
};

class CommandIngest : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    remote_or_local_ = args[1];
    column_family_name_ = args[2];
    files_str_ = args[3];
    server_id_ = args[4];
    ingestion_mode_ = args[5];
    if (args.size() > 6) {
      auto level_str = args[6];
      auto temp = GET_OR_RET(ParseInt<int64_t>(level_str, 10));
      target_level_ = temp;
    }
    if (remote_or_local_ != "local" && remote_or_local_ != "remote") {
      return {Status::NotOK, "Failed cmd format, it should be like: ingest remote|local file1,file2,file3"};
    }
    return Status::OK();
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    LOG(INFO) << "Receive Ingest command:" << files_str_;
    //    LOG(INFO) << "Start Ingesting files:" << files_str_;
    std::string target_dir = svr->GetConfig()->backup_sync_dir;
    std::vector<std::string> files = util::Split(files_str_, ",");
    LOG(INFO) << "Ingesting files from: " << remote_or_local_;

    if (remote_or_local_ == "local") {
      std::vector<std::string> ingest_files;
      std::string file_str;
      for (auto file : files) {
        auto abs_path = file;
        if (file[0] != '/') {
          abs_path = svr->GetConfig()->global_migration_sync_dir + "/" + svr->cluster->GetMyId() + "/" + file;
        }
        ingest_files.push_back(abs_path);
        file_str += ingest_files.back() + ",";
      }
      file_str.pop_back();
      LOG(INFO) << "Ingesting files: " << file_str;
      bool fast_ingest = ingestion_mode_ == "fast";
      auto s = svr->cluster->IngestFiles(column_family_name_, ingest_files, fast_ingest, target_level_);
      if (!s.IsOK()) {
        *output = redis::SimpleString("error: " + s.Msg());
        return s;
      }

      *output = redis::SimpleString("OK");
      return Status::OK();
    }
    return {Status::NotOK, "Execution failed"};
  }

 private:
  std::string remote_or_local_;
  std::string files_str_;
  std::string column_family_name_;
  std::string server_id_;
  std::string ingestion_mode_;
  int target_level_;
};

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandCluster>("cluster", -2, "cluster no-script", 0, 0, 0),
                        MakeCmdAttr<CommandClusterX>("clusterx", -2, "cluster no-script", 0, 0, 0),
                        MakeCmdAttr<CommandIngest>("sst_ingest", 7, "cluster no-script", 0, 0, 0), )

}  // namespace redis
