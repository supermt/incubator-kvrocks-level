
#include <fcntl.h>

#include <memory>
#include <utility>

#include "db_util.h"
#include "event_util.h"
#include "fmt/format.h"
#include "io_util.h"
#include "slot_migrate.h"
#include "storage/batch_extractor.h"
#include "storage/compact_filter.h"
#include "storage/table_properties_collector.h"
#include "thread_util.h"
#include "time_util.h"
#include "types/redis_stream_base.h"
#include "types/redis_string.h"

BatchedSlotMigrator::BatchedSlotMigrator(Server *svr, int max_migration_speed, int max_pipeline_size, int seq_gap_limit)
    : SlotMigrator(svr, max_migration_speed, max_pipeline_size, seq_gap_limit, true) {}
ParallelSlotMigrator::ParallelSlotMigrator(Server *svr, int max_migration_speed, int max_pipeline_size,
                                           int seq_gap_limit)
    : BatchedSlotMigrator(svr, max_migration_speed, max_pipeline_size, seq_gap_limit) {}

Status BatchedSlotMigrator::MigrateStart(Server *svr, const std::string &node_id, const std::string &dst_ip,
                                         int dst_port, int seq_gap, bool join) {
  current_stage_ = SlotMigrationStage::kStart;
  dst_node_ = node_id;

  auto job = std::make_unique<SlotMigrationJob>(migrating_slots_, dst_ip, dst_port, 0, 16, seq_gap);
  LOG(INFO) << "[migrate] Start migrating slots, from slot: " << migrating_slots_.front()
            << " to slot: " << migrating_slots_.back() << ". Slots are moving to " << dst_ip << ":" << dst_port;
  {
    std::lock_guard<std::mutex> guard(job_mutex_);
    migration_job_ = std::move(job);
    job_cv_.notify_one();
  }

  return Status::OK();
}

Status BatchedSlotMigrator::SetMigrationSlots(std::vector<int> &target_slots) {
  if (!migrating_slots_.empty() || this->IsMigrationInProgress()) {
    return {Status::NotOK, "Last Migrate Batch is not finished"};
  }
  migrating_slots_.clear();
  migrating_slots_.insert(migrating_slots_.end(), target_slots.begin(), target_slots.end());
  return Status::OK();
}

Status BatchedSlotMigrator::syncWal() {
  Status s;
  for (auto slot : migrating_slots_) {
    migration_job_->slot_id = slot;
    migrating_slot_ = slot;
    s = SlotMigrator::syncWal();
    if (!s.IsOK()) {
      return s;
    }
  }
  return Status::OK();
}

Status BatchedSlotMigrator::setImportStatusOnDstNode(int sock_fd, int status) {
  Status s;
  for (auto slot : migrating_slots_) {
    migration_job_->slot_id = slot;
    migrating_slot_ = slot;
    s = SlotMigrator::setImportStatusOnDstNode(sock_fd, status);
    if (!s.IsOK()) {
      return s;
    }
  }
  return Status::OK();
}

Status ParallelSlotMigrator::sendSnapshot() {
  Status s;
  for (auto slot : migrating_slots_) {
    migration_job_->slot_id = slot;
    migrating_slot_ = slot;
    s = SlotMigrator::sendSnapshot();
    if (!s.IsOK()) {
      return s;
    }
  }
  return Status::OK();
}