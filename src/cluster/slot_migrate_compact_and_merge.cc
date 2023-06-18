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

CompactAndMergeMigrator::CompactAndMergeMigrator(Server *svr, int migration_speed, int pipeline_size_limit, int seq_gap, int pull_method)
    : BatchedSlotMigrator(svr, migration_speed, pipeline_size_limit, seq_gap){
}
Status CompactAndMergeMigrator::sendSnapshot() { return {Status::NotOK, "not-finished"}; }