#include <memory>
#include <utility>

#include "db_util.h"
#include "event_util.h"
#include "fmt/format.h"
#include "io_util.h"
#include "rocksdb/convenience.h"
#include "slot_migrate.h"
#include "storage/batch_extractor.h"
#include "storage/compact_filter.h"
#include "storage/table_properties_collector.h"
#include "thread_util.h"
#include "time_util.h"
#include "types/redis_stream_base.h"
#include "types/redis_string.h"

LevelMigrator::LevelMigrator(Server *svr, int migration_speed, int pipeline_size_limit, int seq_gap)
    : CompactAndMergeMigrator(svr, migration_speed, pipeline_size_limit, seq_gap) {
  meta_cf_handle_ = storage_->GetCFHandle(engine::kMetadataColumnFamilyName);
  subkey_cf_handle_ = storage_->GetCFHandle(engine::kSubkeyColumnFamilyName);
}

Status LevelMigrator::sendSnapshot() {
  auto start = util::GetTimeStampUS();
  auto end = util::GetTimeStampUS();
  storage_->GetDB()->PauseBackgroundWork();
  auto src_config = svr_->GetConfig();
  std::string src_info = "127.0.0.1:" + std::to_string(src_config->port) + "@" + src_config->db_dir;
  std::string dst_info =
      dst_ip_ + ":" + std::to_string(dst_port_) + "@" + src_config->global_migration_sync_dir + "/" + dst_node_;
  // we can directly send data to target server
  std::string db_path_abs;
  auto db_ptr = storage_->GetDB();
  db_ptr->GetEnv()->GetAbsolutePath(src_config->db_dir, &db_path_abs);
  std::vector<std::string> slot_prefix_list_;

  for (int slot : migration_job_->slots) {
    std::string prefix;
    ComposeSlotKeyPrefix(namespace_, slot, &prefix);
    slot_prefix_list_.push_back(prefix);
  }

  rocksdb::ColumnFamilyMetaData metacf_ssts;
  rocksdb::ColumnFamilyMetaData subkeycf_ssts;
  meta_cf_handle_ = storage_->GetCFHandle(engine::kMetadataColumnFamilyName);
  subkey_cf_handle_ = storage_->GetCFHandle(engine::kSubkeyColumnFamilyName);
  storage_->GetDB()->GetColumnFamilyMetaData(meta_cf_handle_, &metacf_ssts);
  storage_->GetDB()->GetColumnFamilyMetaData(subkey_cf_handle_, &subkeycf_ssts);

  std::vector<std::string> meta_compact_sst(0);
  std::vector<std::string> subkey_compact_sst(0);
  std::map<int, std::vector<std::string>> meta_level_files;
  std::map<int, std::vector<std::string>> subkey_level_files;

  start = util::GetTimeStampUS();
  for (const auto &level_stat : metacf_ssts.levels) {
    meta_level_files[level_stat.level] = {};
    for (const auto &sst_info : level_stat.files) {
      for (auto prefix : slot_prefix_list_) {
        if (compare_with_prefix(sst_info.smallestkey, prefix) <= 0 &&
            compare_with_prefix(sst_info.largestkey, prefix) >= 0) {
          meta_level_files[level_stat.level].push_back(util::Split(sst_info.name, "/").back());
          meta_compact_sst.push_back(sst_info.name);
          break;  // no need for redundant inserting
        }
      }
    }
  }

  for (const auto &level_stat : subkeycf_ssts.levels) {
    subkey_level_files[level_stat.level] = {};
    for (const auto &sst_info : level_stat.files) {
      for (auto prefix : slot_prefix_list_) {
        if (compare_with_prefix(sst_info.smallestkey, prefix) <= 0 &&
            compare_with_prefix(sst_info.largestkey, prefix) >= 0) {
          subkey_level_files[level_stat.level].push_back(util::Split(sst_info.name, "/").back());
          subkey_compact_sst.push_back(sst_info.name);
          break;
        }
      }
    }
  }

  if (meta_compact_sst.empty() || subkey_compact_sst.empty()) {
    LOG(ERROR) << "No SSTs are found";
    storage_->GetDB()->ContinueBackgroundWork();
    return {Status::NotOK, "No SSTs can be found."};
  }

  std::string meta_sst_str;
  std::vector<std::string> result_ssts;
  for (const auto &s : meta_compact_sst) {
    auto fn = util::Split(s, "/").back();
    meta_sst_str += (fn + ",");
    result_ssts.push_back(s);
  }
  meta_sst_str.pop_back();

  std::string sub_sst_str;
  sub_sst_str.clear();
  for (const auto &s : subkey_compact_sst) {
    auto fn = util::Split(s, "/").back();
    sub_sst_str += (fn + ",");
    result_ssts.push_back(s);
  }
  sub_sst_str.pop_back();
  end = util::GetTimeStampUS();

  LOG(INFO) << "Meta SSTs:[" << meta_sst_str << "]";
  LOG(INFO) << "Subkey SSTs:[" << sub_sst_str << "]" << std::endl;
  LOG(INFO) << "SST collected, Time taken(us): " << end - start << std::endl;

  // copy files to remote server
  auto remote_username = svr_->GetConfig()->migration_user;

  std::string source_ssts = "";

  for (const auto &fn : result_ssts) {
    std::string abs_name;
    if (src_config->db_dir[0] == '/') {
      abs_name = src_config->db_dir + fn + " ";
    } else {
      abs_name = db_path_abs + "/" + src_config->db_dir + fn + " ";
    }
    source_ssts += abs_name;
  }
  source_ssts.pop_back();
  LOG(INFO) << "SST waiting for ingestion: " << source_ssts;

  std::string source_space = svr_->GetConfig()->db_dir;
  std::string target_space = svr_->GetConfig()->global_migration_sync_dir + "/" + dst_node_;

  std::string worthy_result;
  Status s;
  bool is_local = (dst_ip_ == "0.0.0.0" || dst_ip_ == "127.0.0.1");
  if (!is_local) {
    start = util::GetTimeStampUS();
    std::string mkdir_remote_cmd =
        "ssh " + svr_->GetConfig()->migration_user + "@" + dst_ip_ + " mkdir -p -m 777 " + target_space;

    s = util::CheckCmdOutput(mkdir_remote_cmd, &worthy_result);
    LOG(INFO) << worthy_result;
    if (!s.IsOK()) {
      LOG(ERROR) << "Failed on copying";
      storage_->GetDB()->ContinueBackgroundWork();
      return {Status::NotOK, "Failed on create directory: " + worthy_result};
    }
    LOG(INFO) << "command: " << mkdir_remote_cmd;
    LOG(INFO) << worthy_result;
    std::string migration_cmds = "ls " + source_ssts + " |xargs -n 1 basename| parallel -v -j8 rsync -raz --progress " +
                                 source_space + "/{} " + remote_username + "@" + dst_ip_ + ":" + target_space + "/{}";
    LOG(INFO) << migration_cmds;

    s = util::CheckCmdOutput(migration_cmds, &worthy_result);
    LOG(INFO) << worthy_result;
    if (!s.IsOK()) {
      LOG(ERROR) << "Failed on copying";
      storage_->GetDB()->ContinueBackgroundWork();
      return {Status::NotOK, "Failed on copy file: " + worthy_result};
    }

    end = util::GetTimeStampUS();
    LOG(INFO) << "File copied, time taken(us): " << end - start;
  }

  s = startIngestion(meta_level_files, true, engine::kMetadataColumnFamilyName);
  if (!s.IsOK()) {
    storage_->GetDB()->ContinueBackgroundWork();
    return s;
  }
  s = startIngestion(subkey_level_files, true, engine::kSubkeyColumnFamilyName);
  if (!s.IsOK()) {
    storage_->GetDB()->ContinueBackgroundWork();
    return s;
  }

  storage_->GetDB()->ContinueBackgroundWork();
  return Status::OK();
}
Status LevelMigrator::syncWal() { return Status::OK(); }
Status LevelMigrator::startIngestion(const std::map<int, std::vector<std::string>> &file_list, bool remote_or_local,
                                     const std::string &cf_name) {
  // Start ingestion
  std::string ingest_output;
  std::string target_server_pre = "redis-cli";
  target_server_pre += (" -h " + dst_ip_);
  target_server_pre += (" -p " + std::to_string(dst_port_));
  Status s;
  auto start = util::GetTimeStampUS();
  for (const auto &meta_level : file_list) {
    std::string meta_file_str;
    if (meta_level.second.empty()) {
      continue;
    }
    for (const auto &file : meta_level.second) {
      if (remote_or_local) {
        meta_file_str += (svr_->GetConfig()->db_dir + "/" + file + ",");
      } else {
        meta_file_str += (file + ",");
      }
    }
    meta_file_str.pop_back();

    std::string ingestion_command = " sst_ingest local";
    ingestion_command += (" " + cf_name);
    ingestion_command += (" " + meta_file_str);
    ingestion_command += (" " + dst_node_);
    auto level_ingest_cmd = target_server_pre + ingestion_command + " fast " + std::to_string(meta_level.first);
    LOG(INFO) << level_ingest_cmd;
    s = util::CheckCmdOutput(level_ingest_cmd, &ingest_output);
    if (!s.IsOK()) {
      LOG(ERROR) << "META Ingestion failed";
      storage_->GetDB()->ContinueBackgroundWork();
      return s;
    }
  }

  auto end = util::GetTimeStampUS();

  LOG(INFO) << "Level ingestion on column family " << cf_name << "finished, Time taken(us)" << end - start;
  return Status::OK();
}
