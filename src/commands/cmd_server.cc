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

#include "commander.h"
#include "commands/scan_base.h"
#include "common/io_util.h"
#include "config/config.h"
#include "error_constants.h"
#include "server/redis_connection.h"
#include "server/server.h"
#include "stats/disk_stats.h"
#include "time_util.h"

namespace redis {

enum class AuthResult {
  OK,
  INVALID_PASSWORD,
  NO_REQUIRE_PASS,
};

AuthResult AuthenticateUser(Connection *conn, Config *config, const std::string &user_password) {
  auto iter = config->tokens.find(user_password);
  if (iter != config->tokens.end()) {
    conn->SetNamespace(iter->second);
    conn->BecomeUser();
    return AuthResult::OK;
  }

  const auto &requirepass = config->requirepass;
  if (!requirepass.empty() && user_password != requirepass) {
    return AuthResult::INVALID_PASSWORD;
  }

  conn->SetNamespace(kDefaultNamespace);
  conn->BecomeAdmin();
  if (requirepass.empty()) {
    return AuthResult::NO_REQUIRE_PASS;
  }

  return AuthResult::OK;
}

class CommandAuth : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Config *config = svr->GetConfig();
    auto &user_password = args_[1];
    AuthResult result = AuthenticateUser(conn, config, user_password);
    switch (result) {
      case AuthResult::OK:
        *output = redis::SimpleString("OK");
        break;
      case AuthResult::INVALID_PASSWORD:
        *output = redis::Error("ERR invalid password");
        break;
      case AuthResult::NO_REQUIRE_PASS:
        *output = redis::Error("ERR Client sent AUTH, but no password is set");
        break;
    }
    return Status::OK();
  }
};

class CommandNamespace : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!conn->IsAdmin()) {
      *output = redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }

    Config *config = svr->GetConfig();
    std::string sub_command = util::ToLower(args_[1]);
    if (args_.size() == 3 && sub_command == "get") {
      if (args_[2] == "*") {
        std::vector<std::string> namespaces;
        auto tokens = config->tokens;
        for (auto &token : tokens) {
          namespaces.emplace_back(token.second);  // namespace
          namespaces.emplace_back(token.first);   // token
        }
        namespaces.emplace_back(kDefaultNamespace);
        namespaces.emplace_back(config->requirepass);
        *output = redis::MultiBulkString(namespaces, false);
      } else {
        std::string token;
        auto s = config->GetNamespace(args_[2], &token);
        if (s.Is<Status::NotFound>()) {
          *output = redis::NilString();
        } else {
          *output = redis::BulkString(token);
        }
      }
    } else if (args_.size() == 4 && sub_command == "set") {
      Status s = config->SetNamespace(args_[2], args_[3]);
      *output = s.IsOK() ? redis::SimpleString("OK") : redis::Error(s.Msg());
      LOG(WARNING) << "Updated namespace: " << args_[2] << " with token: " << args_[3] << ", addr: " << conn->GetAddr()
                   << ", result: " << s.Msg();
    } else if (args_.size() == 4 && sub_command == "add") {
      Status s = config->AddNamespace(args_[2], args_[3]);
      *output = s.IsOK() ? redis::SimpleString("OK") : redis::Error(s.Msg());
      LOG(WARNING) << "New namespace: " << args_[2] << " with token: " << args_[3] << ", addr: " << conn->GetAddr()
                   << ", result: " << s.Msg();
    } else if (args_.size() == 3 && sub_command == "del") {
      Status s = config->DelNamespace(args_[2]);
      *output = s.IsOK() ? redis::SimpleString("OK") : redis::Error(s.Msg());
      LOG(WARNING) << "Deleted namespace: " << args_[2] << ", addr: " << conn->GetAddr() << ", result: " << s.Msg();
    } else {
      *output = redis::Error("NAMESPACE subcommand must be one of GET, SET, DEL, ADD");
    }
    return Status::OK();
  }
};

class CommandKeys : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string prefix = args_[1];
    std::vector<std::string> keys;
    redis::Database redis(svr->storage, conn->GetNamespace());
    if (prefix == "*") {
      redis.Keys(std::string(), &keys);
    } else {
      if (prefix[prefix.size() - 1] != '*') {
        *output = redis::Error("ERR only keys prefix match was supported");
        return Status::OK();
      }

      redis.Keys(prefix.substr(0, prefix.size() - 1), &keys);
    }
    *output = redis::MultiBulkString(keys);
    return Status::OK();
  }
};

class CommandFlushDB : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (svr->GetConfig()->cluster_enabled) {
      if (svr->slot_migrator->IsMigrationInProgress()) {
        svr->slot_migrator->SetStopMigrationFlag(true);
        LOG(INFO) << "Stop migration task for flushdb";
      }
    }
    redis::Database redis(svr->storage, conn->GetNamespace());
    auto s = redis.FlushDB();
    LOG(WARNING) << "DB keys in namespace: " << conn->GetNamespace() << " was flushed, addr: " << conn->GetAddr();
    if (s.ok()) {
      *output = redis::SimpleString("OK");
      return Status::OK();
    }

    return {Status::RedisExecErr, s.ToString()};
  }
};

class CommandFlushAll : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!conn->IsAdmin()) {
      *output = redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }

    if (svr->GetConfig()->cluster_enabled) {
      if (svr->slot_migrator->IsMigrationInProgress()) {
        svr->slot_migrator->SetStopMigrationFlag(true);
        LOG(INFO) << "Stop migration task for flushall";
      }
    }

    redis::Database redis(svr->storage, conn->GetNamespace());
    auto s = redis.FlushAll();
    if (s.ok()) {
      LOG(WARNING) << "All DB keys was flushed, addr: " << conn->GetAddr();
      *output = redis::SimpleString("OK");
      return Status::OK();
    }

    return {Status::RedisExecErr, s.ToString()};
  }
};

class CommandPing : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (args_.size() == 1) {
      *output = redis::SimpleString("PONG");
    } else if (args_.size() == 2) {
      *output = redis::BulkString(args_[1]);
    } else {
      return {Status::NotOK, errWrongNumOfArguments};
    }
    return Status::OK();
  }
};

class CommandSelect : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    *output = redis::SimpleString("OK");
    return Status::OK();
  }
};

class CommandConfig : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!conn->IsAdmin()) {
      *output = redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }

    Config *config = svr->GetConfig();
    std::string sub_command = util::ToLower(args_[1]);
    if ((sub_command == "rewrite" && args_.size() != 2) || (sub_command == "get" && args_.size() != 3) ||
        (sub_command == "set" && args_.size() != 4)) {
      *output = redis::Error(errWrongNumOfArguments);
      return Status::OK();
    }

    if (args_.size() == 2 && sub_command == "rewrite") {
      Status s = config->Rewrite();
      if (!s.IsOK()) return {Status::RedisExecErr, s.Msg()};

      *output = redis::SimpleString("OK");
      LOG(INFO) << "# CONFIG REWRITE executed with success";
    } else if (args_.size() == 3 && sub_command == "get") {
      std::vector<std::string> values;
      config->Get(args_[2], &values);
      *output = redis::MultiBulkString(values);
    } else if (args_.size() == 4 && sub_command == "set") {
      Status s = config->Set(svr, args_[2], args_[3]);
      if (!s.IsOK()) {
        *output = redis::Error("CONFIG SET '" + args_[2] + "' error: " + s.Msg());
      } else {
        *output = redis::SimpleString("OK");
      }
    } else {
      *output = redis::Error("CONFIG subcommand must be one of GET, SET, REWRITE");
    }
    return Status::OK();
  }
};

class CommandInfo : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string section = "all";
    if (args_.size() == 2) {
      section = util::ToLower(args_[1]);
    }
    std::string info;
    svr->GetInfo(conn->GetNamespace(), section, &info);
    *output = redis::BulkString(info);
    return Status::OK();
  }
};

class CommandDisk : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    std::string opname = util::ToLower(args[1]);
    if (opname != "usage") return {Status::RedisInvalidCmd, "Unknown operation"};
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisType type = kRedisNone;
    redis::Disk disk_db(svr->storage, conn->GetNamespace());
    auto s = disk_db.Type(args_[2], &type);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    uint64_t result = 0;
    s = disk_db.GetKeySize(args_[2], type, &result);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(result);
    return Status::OK();
  }
};

class CommandMemory : public CommandDisk {};

class CommandRole : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    svr->GetRoleInfo(output);
    return Status::OK();
  }
};

class CommandDBSize : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string ns = conn->GetNamespace();
    if (args_.size() == 1) {
      KeyNumStats stats;
      svr->GetLatestKeyNumStats(ns, &stats);
      *output = redis::Integer(stats.n_key);
    } else if (args_.size() == 2 && args_[1] == "scan") {
      Status s = svr->AsyncScanDBSize(ns);
      if (s.IsOK()) {
        *output = redis::SimpleString("OK");
      } else {
        *output = redis::Error(s.Msg());
      }
    } else {
      *output = redis::Error("DBSIZE subcommand only supports scan");
    }
    return Status::OK();
  }
};

class CommandPerfLog : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = util::ToLower(args[1]);
    if (subcommand_ != "reset" && subcommand_ != "get" && subcommand_ != "len") {
      return {Status::NotOK, "PERFLOG subcommand must be one of RESET, LEN, GET"};
    }

    if (subcommand_ == "get" && args.size() >= 3) {
      if (args[2] == "*") {
        cnt_ = 0;
      } else {
        cnt_ = GET_OR_RET(ParseInt<int64_t>(args[2], 10));
      }
    }

    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    auto perf_log = srv->GetPerfLog();
    if (subcommand_ == "len") {
      *output = redis::Integer(static_cast<int64_t>(perf_log->Size()));
    } else if (subcommand_ == "reset") {
      perf_log->Reset();
      *output = redis::SimpleString("OK");
    } else if (subcommand_ == "get") {
      *output = perf_log->GetLatestEntries(cnt_);
    }
    return Status::OK();
  }

 private:
  std::string subcommand_;
  int64_t cnt_ = 10;
};

class CommandSlowlog : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = util::ToLower(args[1]);
    if (subcommand_ != "reset" && subcommand_ != "get" && subcommand_ != "len") {
      return {Status::NotOK, "SLOWLOG subcommand must be one of RESET, LEN, GET"};
    }

    if (subcommand_ == "get" && args.size() >= 3) {
      if (args[2] == "*") {
        cnt_ = 0;
      } else {
        cnt_ = GET_OR_RET(ParseInt<int64_t>(args[2], 10));
      }
    }

    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    auto slowlog = srv->GetSlowLog();
    if (subcommand_ == "reset") {
      slowlog->Reset();
      *output = redis::SimpleString("OK");
      return Status::OK();
    } else if (subcommand_ == "len") {
      *output = redis::Integer(static_cast<int64_t>(slowlog->Size()));
      return Status::OK();
    } else if (subcommand_ == "get") {
      *output = slowlog->GetLatestEntries(cnt_);
      return Status::OK();
    }
    return {Status::NotOK, "SLOWLOG subcommand must be one of RESET, LEN, GET"};
  }

 private:
  std::string subcommand_;
  int64_t cnt_ = 10;
};

class CommandClient : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = util::ToLower(args[1]);
    // subcommand: getname id kill list info setname
    if ((subcommand_ == "id" || subcommand_ == "getname" || subcommand_ == "list" || subcommand_ == "info") &&
        args.size() == 2) {
      return Status::OK();
    }

    if ((subcommand_ == "setname") && args.size() == 3) {
      // Check if the charset is ok. We need to do this otherwise
      // CLIENT LIST or CLIENT INFO format will break. You should always be able to
      // split by space to get the different fields.
      for (auto ch : args[2]) {
        if (ch < '!' || ch > '~') {
          return {Status::RedisInvalidCmd, "Client names cannot contain spaces, newlines or special characters"};
        }
      }

      conn_name_ = args[2];
      return Status::OK();
    }

    if ((subcommand_ == "kill")) {
      if (args.size() == 2) {
        return {Status::RedisParseErr, errInvalidSyntax};
      }

      if (args.size() == 3) {
        addr_ = args[2];
        new_format_ = false;
        return Status::OK();
      }

      size_t i = 2;
      new_format_ = true;

      while (i < args.size()) {
        bool more_args = i < args.size();
        if (!strcasecmp(args[i].c_str(), "addr") && more_args) {
          addr_ = args[i + 1];
        } else if (!strcasecmp(args[i].c_str(), "id") && more_args) {
          auto parse_result = ParseInt<uint64_t>(args[i + 1], 10);
          if (!parse_result) {
            return {Status::RedisParseErr, errValueNotInteger};
          }

          id_ = *parse_result;
        } else if (!strcasecmp(args[i].c_str(), "skipme") && more_args) {
          if (!strcasecmp(args[i + 1].c_str(), "yes")) {
            skipme_ = true;
          } else if (!strcasecmp(args[i + 1].c_str(), "no")) {
            skipme_ = false;
          } else {
            return {Status::RedisParseErr, errInvalidSyntax};
          }
        } else if (!strcasecmp(args[i].c_str(), "type") && more_args) {
          if (!strcasecmp(args[i + 1].c_str(), "normal")) {
            kill_type_ |= kTypeNormal;
          } else if (!strcasecmp(args[i + 1].c_str(), "pubsub")) {
            kill_type_ |= kTypePubsub;
          } else if (!strcasecmp(args[i + 1].c_str(), "master")) {
            kill_type_ |= kTypeMaster;
          } else if (!strcasecmp(args[i + 1].c_str(), "replica") || !strcasecmp(args[i + 1].c_str(), "slave")) {
            kill_type_ |= kTypeSlave;
          } else {
            return {Status::RedisParseErr, errInvalidSyntax};
          }
        } else {
          return {Status::RedisParseErr, errInvalidSyntax};
        }
        i += 2;
      }
      return Status::OK();
    }
    return {Status::RedisInvalidCmd, "Syntax error, try CLIENT LIST|INFO|KILL ip:port|GETNAME|SETNAME"};
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    if (subcommand_ == "list") {
      *output = redis::BulkString(srv->GetClientsStr());
      return Status::OK();
    } else if (subcommand_ == "info") {
      *output = redis::BulkString(conn->ToString());
      return Status::OK();
    } else if (subcommand_ == "setname") {
      conn->SetName(conn_name_);
      *output = redis::SimpleString("OK");
      return Status::OK();
    } else if (subcommand_ == "getname") {
      std::string name = conn->GetName();
      *output = name == "" ? redis::NilString() : redis::BulkString(name);
      return Status::OK();
    } else if (subcommand_ == "id") {
      *output = redis::Integer(conn->GetID());
      return Status::OK();
    } else if (subcommand_ == "kill") {
      int64_t killed = 0;
      srv->KillClient(&killed, addr_, id_, kill_type_, skipme_, conn);
      if (new_format_) {
        *output = redis::Integer(killed);
      } else {
        if (killed == 0)
          *output = redis::Error("No such client");
        else
          *output = redis::SimpleString("OK");
      }
      return Status::OK();
    }

    return {Status::RedisInvalidCmd, "Syntax error, try CLIENT LIST|INFO|KILL ip:port|GETNAME|SETNAME"};
  }

 private:
  std::string addr_;
  std::string conn_name_;
  std::string subcommand_;
  bool skipme_ = false;
  int64_t kill_type_ = 0;
  uint64_t id_ = 0;
  bool new_format_ = true;
};

class CommandMonitor : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    conn->Owner()->BecomeMonitorConn(conn);
    *output = redis::SimpleString("OK");
    return Status::OK();
  }
};

class CommandShutdown : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    if (!conn->IsAdmin()) {
      *output = redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }

    if (!srv->IsStopped()) {
      LOG(INFO) << "bye bye";
      srv->Stop();
    }
    return Status::OK();
  }
};

class CommandQuit : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    conn->EnableFlag(redis::Connection::kCloseAfterReply);
    *output = redis::SimpleString("OK");
    return Status::OK();
  }
};

class CommandDebug : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = util::ToLower(args[1]);
    if ((subcommand_ == "sleep") && args.size() == 3) {
      auto second = ParseFloat(args[2]);
      if (!second) {
        return {Status::RedisParseErr, "invalid debug sleep time"};
      }

      microsecond_ = static_cast<uint64_t>(*second * 1000 * 1000);
      return Status::OK();
    }
    return {Status::RedisInvalidCmd, "Syntax error, DEBUG SLEEP <seconds>"};
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    if (subcommand_ == "sleep") {
      usleep(microsecond_);
    }
    *output = redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  std::string subcommand_;
  uint64_t microsecond_ = 0;
};

class CommandCommand : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (args_.size() == 1) {
      GetAllCommandsInfo(output);
    } else {
      std::string sub_command = util::ToLower(args_[1]);
      if ((sub_command == "count" && args_.size() != 2) || (sub_command == "getkeys" && args_.size() < 3) ||
          (sub_command == "info" && args_.size() < 3)) {
        *output = redis::Error(errWrongNumOfArguments);
        return Status::OK();
      }

      if (sub_command == "count") {
        *output = redis::Integer(GetCommandNum());
      } else if (sub_command == "info") {
        GetCommandsInfo(output, std::vector<std::string>(args_.begin() + 2, args_.end()));
      } else if (sub_command == "getkeys") {
        std::vector<int> keys_indexes;
        auto s = GetKeysFromCommand(args_[2], static_cast<int>(args_.size()) - 2, &keys_indexes);
        if (!s.IsOK()) return s;

        if (keys_indexes.size() == 0) {
          *output = redis::Error("Invalid arguments specified for command");
          return Status::OK();
        }

        std::vector<std::string> keys;
        keys.reserve(keys_indexes.size());
        for (const auto &key_index : keys_indexes) {
          keys.emplace_back(args_[key_index + 2]);
        }
        *output = redis::MultiBulkString(keys);
      } else {
        *output = redis::Error("Command subcommand must be one of COUNT, GETKEYS, INFO");
      }
    }
    return Status::OK();
  }
};

class CommandEcho : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    *output = redis::BulkString(args_[1]);
    return Status::OK();
  }
};

class CommandTime : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    uint64_t now = util::GetTimeStampUS();
    uint64_t s = now / 1000 / 1000;         // unix time in seconds.
    uint64_t us = now - (s * 1000 * 1000);  // microseconds.

    *output = redis::MultiLen(2);
    *output += redis::BulkString(std::to_string(s));
    *output += redis::BulkString(std::to_string(us));

    return Status::OK();
  }
};

/*
 * HELLO [<protocol-version> [AUTH [<password>|<username> <password>]] [SETNAME <name>] ]
 *   Note that the <username> should always be `default` if provided otherwise AUTH fails.
 *   And it is only meant to be aligning syntax with Redis HELLO.
 */
class CommandHello final : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    size_t next_arg = 1;
    if (args_.size() >= 2) {
      auto parse_result = ParseInt<int64_t>(args_[next_arg], 10);
      ++next_arg;
      if (!parse_result) {
        return {Status::NotOK, "Protocol version is not an integer or out of range"};
      }

      int64_t protocol = *parse_result;

      // In redis, it will check protocol < 2 or protocol > 3,
      // kvrocks only supports REPL2 by now, but for supporting some
      // `hello 3`, it will not report error when using 3.
      if (protocol < 2 || protocol > 3) {
        return {Status::NotOK, "-NOPROTO unsupported protocol version"};
      }
    }

    // Handling AUTH and SETNAME
    for (; next_arg < args_.size(); ++next_arg) {
      size_t more_args = args_.size() - next_arg - 1;
      const std::string &opt = args_[next_arg];
      if (util::ToLower(opt) == "auth" && more_args != 0) {
        if (more_args == 2 || more_args == 4) {
          if (args_[next_arg + 1] != "default") {
            return {Status::NotOK, "invalid password"};
          }
          next_arg++;
        }
        const auto &user_password = args_[next_arg + 1];
        auto auth_result = AuthenticateUser(conn, svr->GetConfig(), user_password);
        switch (auth_result) {
          case AuthResult::INVALID_PASSWORD:
            return {Status::NotOK, "invalid password"};
          case AuthResult::NO_REQUIRE_PASS:
            return {Status::NotOK, "Client sent AUTH, but no password is set"};
          case AuthResult::OK:
            break;
        }
        next_arg += 1;
      } else if (util::ToLower(opt) == "setname" && more_args != 0) {
        const std::string &name = args_[next_arg + 1];
        conn->SetName(name);
        next_arg += 1;
      } else {
        *output = redis::Error("Syntax error in HELLO option " + opt);
        return Status::OK();
      }
    }

    std::vector<std::string> output_list;
    output_list.push_back(redis::BulkString("server"));
    output_list.push_back(redis::BulkString("redis"));
    output_list.push_back(redis::BulkString("proto"));
    output_list.push_back(redis::Integer(2));

    output_list.push_back(redis::BulkString("mode"));
    // Note: sentinel is not supported in kvrocks.
    if (svr->GetConfig()->cluster_enabled) {
      output_list.push_back(redis::BulkString("cluster"));
    } else {
      output_list.push_back(redis::BulkString("standalone"));
    }
    *output = redis::Array(output_list);
    return Status::OK();
  }
};

class CommandScan : public CommandScanBase {
 public:
  CommandScan() : CommandScanBase() {}

  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 != 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    ParseCursor(args[1]);
    if (args.size() >= 4) {
      Status s = ParseMatchAndCountParam(util::ToLower(args[2]), args_[3]);
      if (!s.IsOK()) {
        return s;
      }
    }

    if (args.size() >= 6) {
      Status s = ParseMatchAndCountParam(util::ToLower(args[4]), args_[5]);
      if (!s.IsOK()) {
        return s;
      }
    }
    return Commander::Parse(args);
  }

  static std::string GenerateOutput(const std::vector<std::string> &keys, std::string end_cursor) {
    std::vector<std::string> list;
    if (!end_cursor.empty()) {
      end_cursor = kCursorPrefix + end_cursor;
      list.emplace_back(redis::BulkString(end_cursor));
    } else {
      list.emplace_back(redis::BulkString("0"));
    }

    list.emplace_back(redis::MultiBulkString(keys, false));

    return redis::Array(list);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::Database redis_db(svr->storage, conn->GetNamespace());
    std::vector<std::string> keys;
    std::string end_cursor;
    auto s = redis_db.Scan(cursor_, limit_, prefix_, &keys, &end_cursor);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = GenerateOutput(keys, end_cursor);
    return Status::OK();
  }
};

class CommandRandomKey : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string key;
    auto cursor = svr->GetLastRandomKeyCursor();
    redis::Database redis(svr->storage, conn->GetNamespace());
    redis.RandomKey(cursor, &key);
    svr->SetLastRandomKeyCursor(key);
    *output = redis::BulkString(key);
    return Status::OK();
  }
};

class CommandCompact : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string begin_key, end_key;
    auto ns = conn->GetNamespace();

    if (ns != kDefaultNamespace) {
      std::string prefix;
      ComposeNamespaceKey(ns, "", &prefix, false);

      redis::Database redis_db(svr->storage, conn->GetNamespace());
      auto s = redis_db.FindKeyRangeWithPrefix(prefix, std::string(), &begin_key, &end_key);
      if (!s.ok()) {
        if (s.IsNotFound()) {
          *output = redis::SimpleString("OK");
          return Status::OK();
        }

        return {Status::RedisExecErr, s.ToString()};
      }
    }

    Status s = svr->AsyncCompactDB(begin_key, end_key);
    if (!s.IsOK()) return s;

    *output = redis::SimpleString("OK");
    LOG(INFO) << "Compact was triggered by manual with executed success";
    return Status::OK();
  }
};

class CommandBGSave : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!conn->IsAdmin()) {
      *output = redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }

    Status s = svr->AsyncBgSaveDB();
    if (!s.IsOK()) return s;

    *output = redis::SimpleString("OK");
    LOG(INFO) << "BGSave was triggered by manual with executed success";
    return Status::OK();
  }
};

class CommandFlushBackup : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!conn->IsAdmin()) {
      *output = redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }

    Status s = svr->AsyncPurgeOldBackups(0, 0);
    if (!s.IsOK()) return s;

    *output = redis::SimpleString("OK");
    LOG(INFO) << "flushbackup was triggered by manual with executed success";
    return Status::OK();
  }
};

class CommandSlaveOf : public Commander {
 public:
  static Status IsTryingToReplicateItself(Server *svr, const std::string &host, uint32_t port) {
    auto ip_addresses = util::LookupHostByName(host);
    if (!ip_addresses) {
      return {Status::NotOK, "Can not resolve hostname: " + host};
    }
    for (auto &ip : *ip_addresses) {
      if (util::MatchListeningIP(svr->GetConfig()->binds, ip) && port == svr->GetConfig()->port) {
        return {Status::NotOK, "can't replicate itself"};
      }
      for (std::pair<std::string, uint32_t> &host_port_pair : svr->GetSlaveHostAndPort()) {
        if (host_port_pair.first == ip && host_port_pair.second == port) {
          return {Status::NotOK, "can't replicate your own replicas"};
        }
      }
    }
    return Status::OK();
  }

  Status Parse(const std::vector<std::string> &args) override {
    host_ = args[1];
    const auto &port = args[2];
    if (util::ToLower(host_) == "no" && util::ToLower(port) == "one") {
      host_.clear();
      return Status::OK();
    }

    auto parse_result = ParseInt<uint32_t>(port, 10);
    if (!parse_result) {
      return {Status::RedisParseErr, "port should be number"};
    }

    port_ = *parse_result;
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (svr->GetConfig()->cluster_enabled) {
      return {Status::RedisExecErr, "can't change to slave in cluster mode"};
    }

    if (svr->GetConfig()->rocks_db.write_options.disable_wal) {
      return {Status::RedisExecErr, "slaveof doesn't work with disable_wal option"};
    }

    if (!conn->IsAdmin()) {
      *output = redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }

    if (host_.empty()) {
      auto s = svr->RemoveMaster();
      if (!s.IsOK()) {
        return s.Prefixed("failed to remove master");
      }

      *output = redis::SimpleString("OK");
      LOG(WARNING) << "MASTER MODE enabled (user request from '" << conn->GetAddr() << "')";
      if (svr->GetConfig()->cluster_enabled) {
        svr->slot_migrator->SetStopMigrationFlag(false);
        LOG(INFO) << "Change server role to master, restart migration task";
      }

      return Status::OK();
    }

    auto s = IsTryingToReplicateItself(svr, host_, port_);
    if (!s.IsOK()) {
      return {Status::RedisExecErr, s.Msg()};
    }
    s = svr->AddMaster(host_, port_, false);
    if (s.IsOK()) {
      *output = redis::SimpleString("OK");
      LOG(WARNING) << "SLAVE OF " << host_ << ":" << port_ << " enabled (user request from '" << conn->GetAddr()
                   << "')";
      if (svr->GetConfig()->cluster_enabled) {
        svr->slot_migrator->SetStopMigrationFlag(true);
        LOG(INFO) << "Change server role to slave, stop migration task";
      }
    } else {
      LOG(ERROR) << "SLAVE OF " << host_ << ":" << port_ << " (user request from '" << conn->GetAddr()
                 << "') encounter error: " << s.Msg();
    }

    return s;
  }

 private:
  std::string host_;
  uint32_t port_ = 0;
};

class CommandStats : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string stats_json = svr->GetRocksDBStatsJson();
    *output = redis::BulkString(stats_json);
    return Status::OK();
  }
};

class CommandHotness : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string hotness_json = svr->GetHotnessJson();
    *output = redis::BulkString(hotness_json);
    return Status::OK();
  }
};

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandAuth>("auth", 2, "read-only ok-loading", 0, 0, 0),
                        MakeCmdAttr<CommandPing>("ping", -1, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandSelect>("select", 2, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandInfo>("info", -1, "read-only ok-loading", 0, 0, 0),
                        MakeCmdAttr<CommandRole>("role", 1, "read-only ok-loading", 0, 0, 0),
                        MakeCmdAttr<CommandConfig>("config", -2, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandNamespace>("namespace", -3, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandKeys>("keys", 2, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandFlushDB>("flushdb", 1, "write", 0, 0, 0),
                        MakeCmdAttr<CommandFlushAll>("flushall", 1, "write", 0, 0, 0),
                        MakeCmdAttr<CommandDBSize>("dbsize", -1, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandSlowlog>("slowlog", -2, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandPerfLog>("perflog", -2, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandClient>("client", -2, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandMonitor>("monitor", 1, "read-only no-multi", 0, 0, 0),
                        MakeCmdAttr<CommandShutdown>("shutdown", 1, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandQuit>("quit", 1, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandScan>("scan", -2, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandRandomKey>("randomkey", 1, "read-only no-script", 0, 0, 0),
                        MakeCmdAttr<CommandDebug>("debug", -2, "read-only exclusive", 0, 0, 0),
                        MakeCmdAttr<CommandCommand>("command", -1, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandEcho>("echo", 2, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandTime>("time", 1, "read-only ok-loading", 0, 0, 0),
                        MakeCmdAttr<CommandDisk>("disk", 3, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandMemory>("memory", 3, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandHello>("hello", -1, "read-only ok-loading", 0, 0, 0),

                        MakeCmdAttr<CommandCompact>("compact", 1, "read-only no-script", 0, 0, 0),
                        MakeCmdAttr<CommandBGSave>("bgsave", 1, "read-only no-script", 0, 0, 0),
                        MakeCmdAttr<CommandFlushBackup>("flushbackup", 1, "read-only no-script", 0, 0, 0),
                        MakeCmdAttr<CommandSlaveOf>("slaveof", 3, "read-only exclusive no-script", 0, 0, 0),
                        MakeCmdAttr<CommandStats>("stats", 1, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandHotness>("hotness", 1, "read-only", 0, 0, 0), )

}  // namespace redis
