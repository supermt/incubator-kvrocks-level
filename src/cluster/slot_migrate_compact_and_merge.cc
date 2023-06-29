#include <memory>
#include <utility>

#include "db_util.h"
#include "event_util.h"
#include "fmt/format.h"
#include "io_util.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/sst_file_reader.h"
#include "slot_migrate.h"
#include "storage/batch_extractor.h"
#include "storage/compact_filter.h"
#include "storage/table_properties_collector.h"
#include "thread_util.h"
#include "time_util.h"
#include "types/redis_stream_base.h"
#include "types/redis_string.h"

CompactAndMergeMigrator::CompactAndMergeMigrator(Server *svr, int migration_speed, int pipeline_size_limit, int seq_gap,
                                                 int pull_method)
    : BatchedSlotMigrator(svr, migration_speed, pipeline_size_limit, seq_gap) {}
Status CompactAndMergeMigrator::sendSnapshot() {
  // Step 1. extract keys prefix
  auto db_ptr = storage_->GetDB();
  db_ptr->PauseBackgroundWork();

  auto start = util::GetTimeStampUS();
  auto s = extractPrefix();
  if (!s.IsOK()) {
    storage_->GetDB()->ContinueBackgroundWork();
    return s;
  }
  auto end = util::GetTimeStampUS();
  LOG(INFO) << "Prefix extracted, time cost (us)" << end - start;

  rocksdb::ColumnFamilyMetaData metacf_ssts;
  rocksdb::ColumnFamilyMetaData subkeycf_ssts;

  rocksdb::ColumnFamilyHandle *meta_cf_handle = storage_->GetCFHandle(engine::kMetadataColumnFamilyName);
  rocksdb::ColumnFamilyHandle *subkey_cf_handle = storage_->GetCFHandle(engine::kSubkeyColumnFamilyName);

  storage_->GetDB()->GetColumnFamilyMetaData(meta_cf_handle, &metacf_ssts);
  storage_->GetDB()->GetColumnFamilyMetaData(subkey_cf_handle, &subkeycf_ssts);

  // step 2. locate ssts with prefix
  start = util::GetTimeStampUS();
  s = extractSlotSSTs(metacf_ssts, &compaction_input_meta);
  if (!s.IsOK()) {
    storage_->GetDB()->ContinueBackgroundWork();
    return s;
  }
  end = util::GetTimeStampUS();
  LOG(INFO) << "Meta file located, time taken(us)" << end - start;

  start = util::GetTimeStampUS();
  s = extractSlotSSTs(subkeycf_ssts, &compaction_input_subkey);
  if (!s.IsOK()) {
    storage_->GetDB()->ContinueBackgroundWork();
    return s;
  }
  end = util::GetTimeStampUS();
  LOG(INFO) << "Subkey file located, time taken(us)" << end - start;

  // step 3. doing compaction
  start = util::GetTimeStampUS();
  std::vector<std::string> compaction_output_meta;
  s = performCompactionOnCF(meta_cf_handle, compaction_input_meta, &compaction_output_meta);
  if (!s.IsOK()) {
    storage_->GetDB()->ContinueBackgroundWork();
    return s;
  }
  end = util::GetTimeStampUS();
  LOG(INFO) << "Meta file compacted, time taken(us)" << end - start;

  start = util::GetTimeStampUS();

  std::vector<std::string> compaction_output_subkey;
  s = performCompactionOnCF(subkey_cf_handle, compaction_input_subkey, &compaction_output_subkey);
  if (!s.IsOK()) {
    storage_->GetDB()->ContinueBackgroundWork();
    return s;
  }
  end = util::GetTimeStampUS();
  LOG(INFO) << "Subkey file compacted, time taken(us)" << end - start;

  // step 4. file formatting and ingest to destination server

  start = util::GetTimeStampUS();
  std::vector<std::string> filter_ext_files_meta;
  s = generateExternalFile(compaction_output_meta, &filter_ext_files_meta, meta_cf_handle);
  if (!s.IsOK()) {
    storage_->GetDB()->ContinueBackgroundWork();
    return s;
  }
  end = util::GetTimeStampUS();
  LOG(INFO) << "Meta file filtered, time taken(us)" << end - start;

  start = util::GetTimeStampUS();
  std::vector<std::string> filter_ext_files_subkey;
  s = generateExternalFile(compaction_output_meta, &filter_ext_files_subkey, meta_cf_handle);
  if (!s.IsOK()) {
    storage_->GetDB()->ContinueBackgroundWork();
    return s;
  }
  end = util::GetTimeStampUS();
  LOG(INFO) << "Subkey file filtered, time taken(us)" << end - start;

  // Step 5. File ingest to remote server
  s = copyAndIngest(filter_ext_files_meta, engine::kMetadataColumnFamilyName);
  if (!s.IsOK()) {
    storage_->GetDB()->ContinueBackgroundWork();
    return s;
  }
  LOG(INFO) << "Meta file copy and ingested";

  s = copyAndIngest(filter_ext_files_subkey, engine::kSubkeyColumnFamilyName);
  if (!s.IsOK()) {
    storage_->GetDB()->ContinueBackgroundWork();
    return s;
  }
  LOG(INFO) << "Subkey file copy and ingested";

  storage_->GetDB()->ContinueBackgroundWork();

  return {Status::NotOK, "not-finished"};
}

Status CompactAndMergeMigrator::extractSlotSSTs(rocksdb::ColumnFamilyMetaData &cf_ssts,
                                                std::vector<std::string> *target) {
  for (const auto &level_stat : cf_ssts.levels) {
    for (const auto &sst_info : level_stat.files) {
      for (const auto &prefix : slot_prefix_list_) {
        if (compare_with_prefix(sst_info.smallestkey, prefix) <= 0 &&
            compare_with_prefix(sst_info.largestkey, prefix) >= 0) {
          compaction_input_meta.push_back(sst_info.name);
          break;  // no need for redundant inserting
        }
      }
    }
  }

  return Status::OK();
}
Status CompactAndMergeMigrator::extractPrefix() {
  slot_prefix_list_.clear();
  // Step 0. Fill namespace if empty

  rocksdb::ReadOptions read_options;
  read_options.snapshot = slot_snapshot_;
  storage_->SetReadOptions(read_options);

  // Step 1. Compose prefix key
  for (int slot : migration_job_->slots) {
    std::string prefix;
    ComposeSlotKeyPrefix(namespace_, slot, &prefix);
    //    std::cout << prefix << "," << slot << "," << Slice(prefix).ToString(true);
    // After ComposeSlotKeyPrefix
    //  +-------------|---------|-------|--------|---|-------|-------+
    // |namespace_size|namespace|slot_id|, therefore we compare only the prefix key

    // This is prefix key: and the subkey is empty
    // +-------------|---------|-------|--------|---|-------|-------+
    // |namespace_size|namespace|slot_id|key_size|key|version|subkey|
    // +-------------|---------|-------|--------|---|-------|-------+
    slot_prefix_list_.push_back(prefix);
  }
  std::sort(slot_prefix_list_.begin(), slot_prefix_list_.end(), [&](const Slice &a, const Slice &b) {
    return storage_->GetDB()->GetOptions().comparator->Compare(a, b) < 0;
  });
  return Status::OK();
}
Status CompactAndMergeMigrator::performCompactionOnCF(rocksdb::ColumnFamilyHandle *cfh,
                                                      std::vector<std::string> &input_file,
                                                      std::vector<std::string> *compaction_output) {
  auto options = storage_->GetDB()->GetOptions();
  rocksdb::CompactionOptions co;
  co.compression = options.compression;
  co.max_subcompactions = options.max_background_compactions;
  auto compact_s = storage_->GetDB()->CompactFiles(co, cfh, input_file, options.num_levels - 1, -1, compaction_output);
  if (!compact_s.ok()) {
    return {Status::NotOK, "Compaction error:" + compact_s.ToString()};
  }
  return Status::OK();
}

Status CompactAndMergeMigrator::generateExternalFile(std::vector<std::string> &compaction_output,
                                                     std::vector<std::string> *external_file_list,
                                                     rocksdb::ColumnFamilyHandle *cfh) {
  auto options = storage_->GetDB()->GetOptions();
  rocksdb::ReadOptions read_options;
  read_options.snapshot = slot_snapshot_;
  storage_->SetReadOptions(read_options);

  for (const auto &fn : compaction_output) {
    rocksdb::SstFileReader reader(options);
    rocksdb::SstFileWriter writer(rocksdb::EnvOptions(), options, cfh);
    auto rocks_s = reader.Open(fn);
    if (!rocks_s.ok()) {
      return {Status::NotOK, "Open file reader failed: " + rocks_s.ToString()};
    }
    auto read_it = reader.NewIterator(read_options);
    auto out_put_name = fn + ".out";
    rocks_s = writer.Open(out_put_name);

    if (!rocks_s.ok()) {
      return {Status::NotOK, "Open file write failed: " + rocks_s.ToString()};
    }

    read_it->SeekToFirst();
    std::string smallest_prefix = slot_prefix_list_.front();
    std::string largest_prefix = slot_prefix_list_.back();
    for (; read_it->Valid(); read_it->Next()) {
      auto current_key = read_it->key();
      if (compare_with_prefix(smallest_prefix, current_key.ToString()) <= 0 &&
          compare_with_prefix(largest_prefix, current_key.ToString()) >= 0) {
        writer.Put(read_it->key(), read_it->value());
      }
    }
    rocks_s = writer.Finish();
    if (!rocks_s.ok()) {
      return {Status::NotOK, "File reader failed: " + rocks_s.ToString()};
    }
    external_file_list->push_back(out_put_name);
  }

  return Status::OK();
}
Status CompactAndMergeMigrator::copyAndIngest(std::vector<std::string> &external_file_list,
                                              const std::string &cf_name) {
  std::string db_path_abs;
  auto db_ptr = storage_->GetDB();
  std::string source_ssts;
  db_ptr->GetEnv()->GetAbsolutePath(storage_->GetConfig()->db_dir, &db_path_abs);
  for (const auto &fn : external_file_list) {
    std::string abs_name;
    if (storage_->GetConfig()->db_dir[0] == '/') {
      abs_name = storage_->GetConfig()->db_dir + fn + " ";
    } else {
      abs_name = db_path_abs + "/" + storage_->GetConfig()->db_dir + fn + " ";
    }
    source_ssts += abs_name;
  }
  source_ssts.pop_back();
  LOG(INFO) << "CF " << cf_name << " SST(s) waiting for ingestion: " << source_ssts;

  bool is_local = (dst_ip_ == "0.0.0.0" || dst_ip_ == "127.0.0.1");

  std::string worthy_result;
  if (!is_local) {
    auto start = util::GetTimeStampUS();

    std::string source_space = svr_->GetConfig()->db_dir;
    std::string target_space = svr_->GetConfig()->global_migration_sync_dir + "/" + dst_node_;
    std::string mkdir_remote_cmd =
        "ssh " + svr_->GetConfig()->migration_user + "@" + dst_ip_ + " mkdir -p -m 777 " + target_space;

    auto s = util::CheckCmdOutput(mkdir_remote_cmd, &worthy_result);
    LOG(INFO) << worthy_result;
    if (!s.IsOK()) {
      LOG(ERROR) << "Failed on copying";
      return {Status::NotOK, "Failed on create directory: " + worthy_result};
    }
    LOG(INFO) << "command: " << mkdir_remote_cmd;
    LOG(INFO) << worthy_result;
    std::string migration_cmds = "ls " + source_ssts + " |xargs -n 1 basename| parallel -v -j8 rsync -raz --progress " +
                                 source_space + "/{} " + storage_->GetConfig()->migration_user + "@" + dst_ip_ + ":" +
                                 target_space + "/{}";
    LOG(INFO) << migration_cmds;

    s = util::CheckCmdOutput(migration_cmds, &worthy_result);
    LOG(INFO) << worthy_result;
    if (!s.IsOK()) {
      LOG(ERROR) << "Failed on copying";
      return {Status::NotOK, "Failed on copy file: " + worthy_result};
    }

    auto end = util::GetTimeStampUS();
    LOG(INFO) << "File copied, time taken(us): " << end - start;
  }

  auto start = util::GetTimeStampUS();
  auto s = startIngestion(external_file_list, is_local, cf_name);
  auto end = util::GetTimeStampUS();
  if (!s.IsOK()) {
    return s;
  }
  LOG(INFO) << "File ingested into target server, time taken(us): " << end - start;

  return Status::OK();
}

Status CompactAndMergeMigrator::startIngestion(const std::vector<std::string> &file_list, bool is_local,
                                               const std::string &cf_name) {
  // Start ingestion
  std::string ingest_output;
  std::string target_server_pre = "redis-cli";
  target_server_pre += (" -h " + dst_ip_);
  target_server_pre += (" -p " + std::to_string(dst_port_));
  Status s;
  auto start = util::GetTimeStampUS();
  std::string file_str;
  for (const auto &file_name : file_list) {
    file_str += (svr_->GetConfig()->db_dir + "/" + file_name + ",");
    file_str.pop_back();

    std::string ingestion_command = " sst_ingest local";
    ingestion_command += (" " + cf_name);
    ingestion_command += (" " + file_str);
    ingestion_command += (" " + dst_node_);
    auto file_ingestion_cmd = target_server_pre + ingestion_command + " fast " + file_name;
    LOG(INFO) << file_ingestion_cmd;
    s = util::CheckCmdOutput(file_ingestion_cmd, &ingest_output);
    if (!s.IsOK()) {
      return s;
    }
  }

  auto end = util::GetTimeStampUS();

  LOG(INFO) << "File ingestion on column family [" << cf_name << "] finished, Time taken(us)" << end - start;
  return Status::OK();
}