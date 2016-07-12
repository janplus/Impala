// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/hdfs-parquet-scanner.h"

#include <limits> // for std::numeric_limits
#include <queue>

#include <boost/algorithm/string.hpp>
#include <gflags/gflags.h>
#include <gutil/strings/substitute.h>

#include "common/object-pool.h"
#include "common/logging.h"
#include "exec/hdfs-scan-node.h"
#include "exec/scanner-context.inline.h"
#include "exec/read-write-util.h"
#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "gutil/bits.h"
#include "runtime/collection-value-builder.h"
#include "runtime/descriptors.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-pool.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-filter.inline.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "runtime/string-value.h"
#include "util/bitmap.h"
#include "util/bit-util.h"
#include "util/decompress.h"
#include "util/debug-util.h"
#include "util/error-util.h"
#include "util/dict-encoding.h"
#include "util/rle-encoding.h"
#include "util/runtime-profile-counters.h"
#include "rpc/thrift-util.h"

#include "common/names.h"

using boost::algorithm::is_any_of;
using boost::algorithm::split;
using boost::algorithm::token_compress_on;
using namespace impala;
using namespace strings;

// Provide a workaround for IMPALA-1658.
DEFINE_bool(convert_legacy_hive_parquet_utc_timestamps, false,
    "When true, TIMESTAMPs read from files written by Parquet-MR (used by Hive) will "
    "be converted from UTC to local time. Writes are unaffected.");

DEFINE_double(parquet_min_filter_reject_ratio, 0.1, "(Advanced) If the percentage of "
    "rows rejected by a runtime filter drops below this value, the filter is disabled.");

const int64_t HdfsParquetScanner::FOOTER_SIZE = 100 * 1024;
const int16_t HdfsParquetScanner::ROW_GROUP_END = numeric_limits<int16_t>::min();
const int16_t HdfsParquetScanner::INVALID_LEVEL = -1;
const int16_t HdfsParquetScanner::INVALID_POS = -1;

// Max data page header size in bytes. This is an estimate and only needs to be an upper
// bound. It is theoretically possible to have a page header of any size due to string
// value statistics, but in practice we'll have trouble reading string values this large.
// Also, this limit is in place to prevent impala from reading corrupt parquet files.
DEFINE_int32(max_page_header_size, 8*1024*1024, "max parquet page header size in bytes");

// Max dictionary page header size in bytes. This is an estimate and only needs to be an
// upper bound.
const int MAX_DICT_HEADER_SIZE = 100;

// The number of rows between checks to see if a filter is not effective, and should be
// disabled. Must be a power of two.
const int ROWS_PER_FILTER_SELECTIVITY_CHECK = 16 * 1024;
static_assert(
    !(ROWS_PER_FILTER_SELECTIVITY_CHECK & (ROWS_PER_FILTER_SELECTIVITY_CHECK - 1)),
    "ROWS_PER_FILTER_SELECTIVITY_CHECK must be a power of two");

// FILE_CHECKs are conditions that we expect to be true but could fail due to a malformed
// input file. They differentiate these cases from DCHECKs, which indicate conditions that
// are true unless there's a bug in Impala. We would ideally always return a bad Status
// instead of failing a FILE_CHECK, but in many cases we use FILE_CHECK instead because
// there's a performance cost to doing the check in a release build, or just due to legacy
// code.
#define FILE_CHECK(a) DCHECK(a)
#define FILE_CHECK_EQ(a, b) DCHECK_EQ(a, b)
#define FILE_CHECK_NE(a, b) DCHECK_NE(a, b)
#define FILE_CHECK_GT(a, b) DCHECK_GT(a, b)
#define FILE_CHECK_LT(a, b) DCHECK_LT(a, b)
#define FILE_CHECK_GE(a, b) DCHECK_GE(a, b)
#define FILE_CHECK_LE(a, b) DCHECK_LE(a, b)

Status HdfsParquetScanner::IssueInitialRanges(HdfsScanNode* scan_node,
    const std::vector<HdfsFileDesc*>& files) {
  vector<DiskIoMgr::ScanRange*> footer_ranges;
  for (int i = 0; i < files.size(); ++i) {
    // If the file size is less than 12 bytes, it is an invalid Parquet file.
    if (files[i]->file_length < 12) {
      return Status(Substitute("Parquet file $0 has an invalid file length: $1",
          files[i]->filename, files[i]->file_length));
    }
    // Compute the offset of the file footer.
    int64_t footer_size = min(FOOTER_SIZE, files[i]->file_length);
    int64_t footer_start = files[i]->file_length - footer_size;

    // Try to find the split with the footer.
    DiskIoMgr::ScanRange* footer_split = FindFooterSplit(files[i]);

    for (int j = 0; j < files[i]->splits.size(); ++j) {
      DiskIoMgr::ScanRange* split = files[i]->splits[j];

      DCHECK_LE(split->offset() + split->len(), files[i]->file_length);
      // If there are no materialized slots (such as count(*) over the table), we can
      // get the result with the file metadata alone and don't need to read any row
      // groups. We only want a single node to process the file footer in this case,
      // which is the node with the footer split.  If it's not a count(*), we create a
      // footer range for the split always.
      if (!scan_node->IsZeroSlotTableScan() || footer_split == split) {
        ScanRangeMetadata* split_metadata =
            reinterpret_cast<ScanRangeMetadata*>(split->meta_data());
        // Each split is processed by first issuing a scan range for the file footer, which
        // is done here, followed by scan ranges for the columns of each row group within
        // the actual split (in InitColumns()). The original split is stored in the
        // metadata associated with the footer range.
        DiskIoMgr::ScanRange* footer_range;
        if (footer_split != NULL) {
          footer_range = scan_node->AllocateScanRange(files[i]->fs,
              files[i]->filename.c_str(), footer_size, footer_start,
              split_metadata->partition_id, footer_split->disk_id(),
              footer_split->try_cache(), footer_split->expected_local(), files[i]->mtime,
              split);
        } else {
          // If we did not find the last split, we know it is going to be a remote read.
          footer_range = scan_node->AllocateScanRange(files[i]->fs,
              files[i]->filename.c_str(), footer_size, footer_start,
              split_metadata->partition_id, -1, false, false, files[i]->mtime, split);
        }

        footer_ranges.push_back(footer_range);
      } else {
        scan_node->RangeComplete(THdfsFileFormat::PARQUET, THdfsCompression::NONE);
      }
    }
  }
  // The threads that process the footer will also do the scan, so we mark all the files
  // as complete here.
  RETURN_IF_ERROR(scan_node->AddDiskIoRanges(footer_ranges, files.size()));
  return Status::OK();
}

DiskIoMgr::ScanRange* HdfsParquetScanner::FindFooterSplit(HdfsFileDesc* file) {
  DCHECK(file != NULL);
  for (int i = 0; i < file->splits.size(); ++i) {
    DiskIoMgr::ScanRange* split = file->splits[i];
    if (split->offset() + split->len() == file->file_length) return split;
  }
  return NULL;
}

namespace impala {

/// Helper struct that holds a batch of tuples allocated from a mem pool, as well
/// as state associated with iterating over its tuples and transferring
/// them to an output batch in TransferScratchTuples().
struct ScratchTupleBatch {
  // Memory backing the batch of tuples. Allocated from batch's tuple data pool.
  uint8_t* tuple_mem;
  // Keeps track of the current tuple index.
  int tuple_idx;
  // Number of valid tuples in tuple_mem.
  int num_tuples;
  // Cached for convenient access.
  const int tuple_byte_size;

  // Helper batch for safely allocating tuple_mem from its tuple data pool using
  // ResizeAndAllocateTupleBuffer().
  RowBatch batch;

  ScratchTupleBatch(
      const RowDescriptor& row_desc, int batch_size, MemTracker* mem_tracker)
    : tuple_mem(NULL),
      tuple_idx(0),
      num_tuples(0),
      tuple_byte_size(row_desc.GetRowSize()),
      batch(row_desc, batch_size, mem_tracker) {
    DCHECK_EQ(row_desc.tuple_descriptors().size(), 1);
  }

  Status Reset(RuntimeState* state) {
    tuple_idx = 0;
    num_tuples = 0;
    // Buffer size is not needed.
    int64_t buffer_size;
    RETURN_IF_ERROR(batch.ResizeAndAllocateTupleBuffer(state, &buffer_size, &tuple_mem));
    return Status::OK();
  }

  inline Tuple* GetTuple(int tuple_idx) const {
    return reinterpret_cast<Tuple*>(tuple_mem + tuple_idx * tuple_byte_size);
  }

  inline MemPool* mem_pool() { return batch.tuple_data_pool(); }
  inline int capacity() const { return batch.capacity(); }
  inline uint8_t* CurrTuple() const { return tuple_mem + tuple_idx * tuple_byte_size; }
  inline uint8_t* TupleEnd() const { return tuple_mem + num_tuples * tuple_byte_size; }
  inline bool AtEnd() const { return tuple_idx == num_tuples; }
};

const string PARQUET_MEM_LIMIT_EXCEEDED = "HdfsParquetScanner::$0() failed to allocate "
    "$1 bytes for $2.";

HdfsParquetScanner::HdfsParquetScanner(HdfsScanNode* scan_node, RuntimeState* state)
    : HdfsScanner(scan_node, state),
      scratch_batch_(new ScratchTupleBatch(
          scan_node->row_desc(), state_->batch_size(), scan_node->mem_tracker())),
      metadata_range_(NULL),
      dictionary_pool_(new MemPool(scan_node->mem_tracker())),
      assemble_rows_timer_(scan_node_->materialize_tuple_timer()) {
  assemble_rows_timer_.Stop();
}

HdfsParquetScanner::~HdfsParquetScanner() {
}

// TODO for 2.3: move column readers to separate file

/// Decoder for all supported Parquet level encodings. Optionally reads, decodes, and
/// caches level values in batches.
/// Level values are unsigned 8-bit integers because we support a maximum nesting
/// depth of 100, as enforced by the FE. Using a small type saves memory and speeds up
/// populating the level cache (e.g., with RLE we can memset() repeated values).
///
/// Inherits from RleDecoder instead of containing one for performance reasons.
/// The containment design would require two BitReaders per column reader. The extra
/// BitReader causes enough bloat for a column reader to require another cache line.
/// TODO: It is not clear whether the inheritance vs. containment choice still makes
/// sense with column-wise materialization. The containment design seems cleaner and
/// we should revisit.
class HdfsParquetScanner::LevelDecoder : public RleDecoder {
 public:
  LevelDecoder(bool is_def_level_decoder)
    : cached_levels_(NULL),
      num_cached_levels_(0),
      cached_level_idx_(0),
      encoding_(parquet::Encoding::PLAIN),
      max_level_(0),
      cache_size_(0),
      num_buffered_values_(0),
      decoding_error_code_(is_def_level_decoder ?
          TErrorCode::PARQUET_DEF_LEVEL_ERROR : TErrorCode::PARQUET_REP_LEVEL_ERROR) {
  }

  /// Initialize the LevelDecoder. Reads and advances the provided data buffer if the
  /// encoding requires reading metadata from the page header.
  Status Init(const string& filename, parquet::Encoding::type encoding,
      MemPool* cache_pool, int cache_size, int max_level, int num_buffered_values,
      uint8_t** data, int* data_size);

  /// Returns the next level or INVALID_LEVEL if there was an error.
  inline int16_t ReadLevel();

  /// Decodes and caches the next batch of levels. Resets members associated with the
  /// cache. Returns a non-ok status if there was a problem decoding a level, or if a
  /// level was encountered with a value greater than max_level_.
  Status CacheNextBatch(int batch_size);

  /// Functions for working with the level cache.
  inline bool CacheHasNext() const { return cached_level_idx_ < num_cached_levels_; }
  inline uint8_t CacheGetNext() {
    DCHECK_LT(cached_level_idx_, num_cached_levels_);
    return cached_levels_[cached_level_idx_++];
  }
  inline void CacheSkipLevels(int num_levels) {
    DCHECK_LE(cached_level_idx_ + num_levels, num_cached_levels_);
    cached_level_idx_ += num_levels;
  }
  inline int CacheSize() const { return num_cached_levels_; }
  inline int CacheRemaining() const { return num_cached_levels_ - cached_level_idx_; }
  inline int CacheCurrIdx() const { return cached_level_idx_; }

 private:
  /// Initializes members associated with the level cache. Allocates memory for
  /// the cache from pool, if necessary.
  Status InitCache(MemPool* pool, int cache_size);

  /// Decodes and writes a batch of levels into the cache. Sets the number of
  /// values written to the cache in *num_cached_levels. Returns false if there was
  /// an error decoding a level or if there was a level value greater than max_level_.
  bool FillCache(int batch_size, int* num_cached_levels);

  /// Buffer for a batch of levels. The memory is allocated and owned by a pool in
  /// passed in Init().
  uint8_t* cached_levels_;
  /// Number of valid level values in the cache.
  int num_cached_levels_;
  /// Current index into cached_levels_.
  int cached_level_idx_;
  parquet::Encoding::type encoding_;

  /// For error checking and reporting.
  int max_level_;
  /// Number of level values cached_levels_ has memory allocated for.
  int cache_size_;
  /// Number of remaining data values in the current data page.
  int num_buffered_values_;
  string filename_;
  TErrorCode::type decoding_error_code_;
};

/// Base class for reading a column. Reads a logical column, not necessarily a column
/// materialized in the file (e.g. collections). The two subclasses are
/// BaseScalarColumnReader and CollectionColumnReader. Column readers read one def and rep
/// level pair at a time. The current def and rep level are exposed to the user, and the
/// corresponding value (if defined) can optionally be copied into a slot via
/// ReadValue(). Can also write position slots.
class HdfsParquetScanner::ColumnReader {
 public:
  virtual ~ColumnReader() { }

  int def_level() const { return def_level_; }
  int rep_level() const { return rep_level_; }

  const SlotDescriptor* slot_desc() const { return slot_desc_; }
  const parquet::SchemaElement& schema_element() const { return *node_.element; }
  int16_t max_def_level() const { return max_def_level_; }
  int16_t max_rep_level() const { return max_rep_level_; }
  int def_level_of_immediate_repeated_ancestor() const {
    return node_.def_level_of_immediate_repeated_ancestor;
  }
  const SlotDescriptor* pos_slot_desc() const { return pos_slot_desc_; }
  void set_pos_slot_desc(const SlotDescriptor* pos_slot_desc) {
    DCHECK(pos_slot_desc_ == NULL);
    pos_slot_desc_ = pos_slot_desc;
  }

  /// Returns true if this reader materializes collections (i.e. CollectionValues).
  virtual bool IsCollectionReader() const { return false; }

  const char* filename() const { return parent_->filename(); };

  /// Read the current value (or null) into 'tuple' for this column. This should only be
  /// called when a value is defined, i.e., def_level() >=
  /// def_level_of_immediate_repeated_ancestor() (since empty or NULL collections produce
  /// no output values), otherwise NextLevels() should be called instead.
  ///
  /// Advances this column reader to the next value (i.e. NextLevels() doesn't need to be
  /// called after calling ReadValue()).
  ///
  /// Returns false if execution should be aborted for some reason, e.g. parse_error_ is
  /// set, the query is cancelled, or the scan node limit was reached. Otherwise returns
  /// true.
  ///
  /// NextLevels() must be called on this reader before calling ReadValue() for the first
  /// time. This is to initialize the current value that ReadValue() will read.
  ///
  /// TODO: this is the function that needs to be codegen'd (e.g. CodegenReadValue())
  /// The codegened functions from all the materialized cols will then be combined
  /// into one function.
  /// TODO: another option is to materialize col by col for the entire row batch in
  /// one call.  e.g. MaterializeCol would write out 1024 values.  Our row batches
  /// are currently dense so we'll need to figure out something there.
  virtual bool ReadValue(MemPool* pool, Tuple* tuple) = 0;

  /// Same as ReadValue() but does not advance repetition level. Only valid for columns
  /// not in collections.
  virtual bool ReadNonRepeatedValue(MemPool* pool, Tuple* tuple) = 0;

  /// Returns true if this reader needs to be seeded with NextLevels() before
  /// calling ReadValueBatch() or ReadNonRepeatedValueBatch().
  /// Note that all readers need to be seeded before calling the non-batched ReadValue().
  virtual bool NeedsSeedingForBatchedReading() const { return true; }

  /// Batched version of ReadValue() that reads up to max_values at once and materializes
  /// them into tuples in tuple_mem. Returns the number of values actually materialized
  /// in *num_values. The return value, error behavior and state changes are generally
  /// the same as in ReadValue(). For example, if an error occurs in the middle of
  /// materializing a batch then false is returned, and num_values, tuple_mem, as well as
  /// this column reader are left in an undefined state, assuming that the caller will
  /// immediately abort execution.
  virtual bool ReadValueBatch(MemPool* pool, int max_values, int tuple_size,
      uint8_t* tuple_mem, int* num_values);

  /// Batched version of ReadNonRepeatedValue() that reads up to max_values at once and
  /// materializes them into tuples in tuple_mem.
  /// The return value and error behavior are the same as in ReadValueBatch().
  virtual bool ReadNonRepeatedValueBatch(MemPool* pool, int max_values, int tuple_size,
      uint8_t* tuple_mem, int* num_values);

  /// Advances this column reader's def and rep levels to the next logical value, i.e. to
  /// the next scalar value or the beginning of the next collection, without attempting to
  /// read the value. This is used to skip past def/rep levels that don't materialize a
  /// value, such as the def/rep levels corresponding to an empty containing collection.
  ///
  /// NextLevels() must be called on this reader before calling ReadValue() for the first
  /// time. This is to initialize the current value that ReadValue() will read.
  ///
  /// Returns false if execution should be aborted for some reason, e.g. parse_error_ is
  /// set, the query is cancelled, or the scan node limit was reached. Otherwise returns
  /// true.
  virtual bool NextLevels() = 0;

  /// Should only be called if pos_slot_desc_ is non-NULL. Writes pos_current_value_ to
  /// 'tuple' (i.e. "reads" the synthetic position field of the parent collection into
  /// 'tuple') and increments pos_current_value_.
  void ReadPosition(Tuple* tuple);

  /// Returns true if this column reader has reached the end of the row group.
  inline bool RowGroupAtEnd() { return rep_level_ == ROW_GROUP_END; }

 protected:
  HdfsParquetScanner* parent_;
  const SchemaNode& node_;
  const SlotDescriptor* slot_desc_;

  /// The slot descriptor for the position field of the tuple, if there is one. NULL if
  /// there's not. Only one column reader for a given tuple desc will have this set.
  const SlotDescriptor* pos_slot_desc_;

  /// The next value to write into the position slot, if there is one. 64-bit int because
  /// the pos slot is always a BIGINT Set to -1 when this column reader does not have a
  /// current rep and def level (i.e. before the first NextLevels() call or after the last
  /// value in the column has been read).
  int64_t pos_current_value_;

  /// The current repetition and definition levels of this reader. Advanced via
  /// ReadValue() and NextLevels(). Set to -1 when this column reader does not have a
  /// current rep and def level (i.e. before the first NextLevels() call or after the last
  /// value in the column has been read). If this is not inside a collection, rep_level_ is
  /// always 0.
  /// int16_t is large enough to hold the valid levels 0-255 and sentinel value -1.
  /// The maximum values are cached here because they are accessed in inner loops.
  int16_t rep_level_;
  int16_t max_rep_level_;
  int16_t def_level_;
  int16_t max_def_level_;

  // Cache frequently accessed members of slot_desc_ for perf.

  /// slot_desc_->tuple_offset(). -1 if slot_desc_ is NULL.
  int tuple_offset_;

  /// slot_desc_->null_indicator_offset(). Invalid if slot_desc_ is NULL.
  NullIndicatorOffset null_indicator_offset_;

  ColumnReader(HdfsParquetScanner* parent, const SchemaNode& node,
      const SlotDescriptor* slot_desc)
    : parent_(parent),
      node_(node),
      slot_desc_(slot_desc),
      pos_slot_desc_(NULL),
      pos_current_value_(INVALID_POS),
      rep_level_(INVALID_LEVEL),
      max_rep_level_(node_.max_rep_level),
      def_level_(INVALID_LEVEL),
      max_def_level_(node_.max_def_level),
      tuple_offset_(slot_desc == NULL ? -1 : slot_desc->tuple_offset()),
      null_indicator_offset_(slot_desc == NULL ? NullIndicatorOffset(-1, -1) :
          slot_desc->null_indicator_offset()) {
    DCHECK_GE(node_.max_rep_level, 0);
    DCHECK_LE(node_.max_rep_level, std::numeric_limits<int16_t>::max());
    DCHECK_GE(node_.max_def_level, 0);
    DCHECK_LE(node_.max_def_level, std::numeric_limits<int16_t>::max());
    // rep_level_ is always valid and equal to 0 if col not in collection.
    if (max_rep_level() == 0) rep_level_ = 0;
  }
};

/// Collections are not materialized directly in parquet files; only scalar values appear
/// in the file. CollectionColumnReader uses the definition and repetition levels of child
/// column readers to figure out the boundaries of each collection in this column.
class HdfsParquetScanner::CollectionColumnReader :
      public HdfsParquetScanner::ColumnReader {
 public:
  CollectionColumnReader(HdfsParquetScanner* parent, const SchemaNode& node,
      const SlotDescriptor* slot_desc)
    : ColumnReader(parent, node, slot_desc) {
    DCHECK(node_.is_repeated());
    if (slot_desc != NULL) DCHECK(slot_desc->type().IsCollectionType());
  }

  virtual ~CollectionColumnReader() { }

  vector<ColumnReader*>* children() { return &children_; }

  virtual bool IsCollectionReader() const { return true; }

  /// The repetition level indicating that the current value is the first in a new
  /// collection (meaning the last value read was the final item in the previous
  /// collection).
  int new_collection_rep_level() const { return max_rep_level() - 1; }

  /// Materializes CollectionValue into tuple slot (if materializing) and advances to next
  /// value.
  virtual bool ReadValue(MemPool* pool, Tuple* tuple);

  /// Same as ReadValue but does not advance repetition level. Only valid for columns not
  /// in collections.
  virtual bool ReadNonRepeatedValue(MemPool* pool, Tuple* tuple);

  /// Advances all child readers to the beginning of the next collection and updates this
  /// reader's state.
  virtual bool NextLevels();

  /// This is called once for each row group in the file.
  void Reset() {
    def_level_ = -1;
    rep_level_ = -1;
    pos_current_value_ = -1;
  }

 private:
  /// Column readers of fields contained within this collection. There is at least one
  /// child reader per collection reader. Child readers either materialize slots in the
  /// collection item tuples, or there is a single child reader that does not materialize
  /// any slot and is only used by this reader to read def and rep levels.
  vector<ColumnReader*> children_;

  /// Updates this reader's def_level_, rep_level_, and pos_current_value_ based on child
  /// reader's state.
  void UpdateDerivedState();

  /// Recursively reads from children_ to assemble a single CollectionValue into
  /// *slot. Also advances rep_level_ and def_level_ via NextLevels().
  ///
  /// Returns false if execution should be aborted for some reason, e.g. parse_error_ is
  /// set, the query is cancelled, or the scan node limit was reached. Otherwise returns
  /// true.
  inline bool ReadSlot(void* slot, MemPool* pool);
};

/// Reader for a single column from the parquet file.  It's associated with a
/// ScannerContext::Stream and is responsible for decoding the data.  Super class for
/// per-type column readers. This contains most of the logic, the type specific functions
/// must be implemented in the subclass.
class HdfsParquetScanner::BaseScalarColumnReader :
      public HdfsParquetScanner::ColumnReader {
 public:
  BaseScalarColumnReader(HdfsParquetScanner* parent, const SchemaNode& node,
      const SlotDescriptor* slot_desc)
    : ColumnReader(parent, node, slot_desc),
      def_levels_(true),
      rep_levels_(false),
      num_buffered_values_(0),
      num_values_read_(0),
      metadata_(NULL),
      stream_(NULL),
      decompressed_data_pool_(new MemPool(parent->scan_node_->mem_tracker())) {
    DCHECK_GE(node_.col_idx, 0) << node_.DebugString();

  }

  virtual ~BaseScalarColumnReader() { }

  /// This is called once for each row group in the file.
  Status Reset(const parquet::ColumnMetaData* metadata, ScannerContext::Stream* stream) {
    DCHECK(stream != NULL);
    DCHECK(metadata != NULL);

    num_buffered_values_ = 0;
    data_ = NULL;
    data_end_ = NULL;
    stream_ = stream;
    metadata_ = metadata;
    num_values_read_ = 0;
    def_level_ = -1;
    // See ColumnReader constructor.
    rep_level_ = max_rep_level() == 0 ? 0 : -1;
    pos_current_value_ = -1;

    if (metadata_->codec != parquet::CompressionCodec::UNCOMPRESSED) {
      RETURN_IF_ERROR(Codec::CreateDecompressor(
          NULL, false, PARQUET_TO_IMPALA_CODEC[metadata_->codec], &decompressor_));
    }
    ClearDictionaryDecoder();
    return Status::OK();
  }

  /// Called once when the scanner is complete for final cleanup.
  void Close() {
    if (decompressor_.get() != NULL) decompressor_->Close();
  }

  int64_t total_len() const { return metadata_->total_compressed_size; }
  int col_idx() const { return node_.col_idx; }
  THdfsCompression::type codec() const {
    if (metadata_ == NULL) return THdfsCompression::NONE;
    return PARQUET_TO_IMPALA_CODEC[metadata_->codec];
  }
  MemPool* decompressed_data_pool() const { return decompressed_data_pool_.get(); }

  /// Reads the next definition and repetition levels for this column. Initializes the
  /// next data page if necessary.
  virtual bool NextLevels() { return NextLevels<true>(); }

  // TODO: Some encodings might benefit a lot from a SkipValues(int num_rows) if
  // we know this row can be skipped. This could be very useful with stats and big
  // sections can be skipped. Implement that when we can benefit from it.

 protected:
  // Friend parent scanner so it can perform validation (e.g. ValidateEndOfRowGroup())
  friend class HdfsParquetScanner;

  // Class members that are accessed for every column should be included up here so they
  // fit in as few cache lines as possible.

  /// Pointer to start of next value in data page
  uint8_t* data_;

  /// End of the data page.
  const uint8_t* data_end_;

  /// Decoder for definition levels.
  LevelDecoder def_levels_;

  /// Decoder for repetition levels.
  LevelDecoder rep_levels_;

  /// Page encoding for values. Cached here for perf.
  parquet::Encoding::type page_encoding_;

  /// Num values remaining in the current data page
  int num_buffered_values_;

  // Less frequently used members that are not accessed in inner loop should go below
  // here so they do not occupy precious cache line space.

  /// The number of values seen so far. Updated per data page.
  int64_t num_values_read_;

  const parquet::ColumnMetaData* metadata_;
  scoped_ptr<Codec> decompressor_;
  ScannerContext::Stream* stream_;

  /// Pool to allocate decompression buffers from.
  boost::scoped_ptr<MemPool> decompressed_data_pool_;

  /// Header for current data page.
  parquet::PageHeader current_page_header_;

  /// Read the next data page. If a dictionary page is encountered, that will be read and
  /// this function will continue reading the next data page.
  Status ReadDataPage();

  /// Try to move the the next page and buffer more values. Return false and sets rep_level_,
  /// def_level_ and pos_current_value_ to -1 if no more pages or an error encountered.
  bool NextPage();

  /// Implementation for NextLevels().
  template <bool ADVANCE_REP_LEVEL>
  bool NextLevels();

  /// Creates a dictionary decoder from values/size. 'decoder' is set to point to a
  /// dictionary decoder stored in this object. Subclass must implement this. Returns
  /// an error status if the dictionary values could not be decoded successfully.
  virtual Status CreateDictionaryDecoder(uint8_t* values, int size,
      DictDecoderBase** decoder) = 0;

  /// Return true if the column has an initialized dictionary decoder. Subclass must
  /// implement this.
  virtual bool HasDictionaryDecoder() = 0;

  /// Clear the dictionary decoder so HasDictionaryDecoder() will return false. Subclass
  /// must implement this.
  virtual void ClearDictionaryDecoder() = 0;

  /// Initializes the reader with the data contents. This is the content for the entire
  /// decompressed data page. Decoders can initialize state from here.
  virtual Status InitDataPage(uint8_t* data, int size) = 0;

 private:
  /// Writes the next value into *slot using pool if necessary. Also advances rep_level_
  /// and def_level_ via NextLevels().
  ///
  /// Returns false if execution should be aborted for some reason, e.g. parse_error_ is
  /// set, the query is cancelled, or the scan node limit was reached. Otherwise returns
  /// true.
  template <bool IN_COLLECTION>
  inline bool ReadSlot(void* slot, MemPool* pool);
};

/// Per column type reader. If MATERIALIZED is true, the column values are materialized
/// into the slot described by slot_desc. If MATERIALIZED is false, the column values
/// are not materialized, but the position can be accessed.
template<typename T, bool MATERIALIZED>
class HdfsParquetScanner::ScalarColumnReader :
      public HdfsParquetScanner::BaseScalarColumnReader {
 public:
  ScalarColumnReader(HdfsParquetScanner* parent, const SchemaNode& node,
      const SlotDescriptor* slot_desc)
    : BaseScalarColumnReader(parent, node, slot_desc),
      dict_decoder_init_(false) {
    if (!MATERIALIZED) {
      // We're not materializing any values, just counting them. No need (or ability) to
      // initialize state used to materialize values.
      DCHECK(slot_desc_ == NULL);
      return;
    }

    DCHECK(slot_desc_ != NULL);
    DCHECK_NE(slot_desc_->type().type, TYPE_BOOLEAN);
    if (slot_desc_->type().type == TYPE_DECIMAL) {
      fixed_len_size_ = ParquetPlainEncoder::DecimalSize(slot_desc_->type());
    } else if (slot_desc_->type().type == TYPE_VARCHAR) {
      fixed_len_size_ = slot_desc_->type().len;
    } else {
      fixed_len_size_ = -1;
    }
    needs_conversion_ = slot_desc_->type().type == TYPE_CHAR ||
        // TODO: Add logic to detect file versions that have unconverted TIMESTAMP
        // values. Currently all versions have converted values.
        (FLAGS_convert_legacy_hive_parquet_utc_timestamps &&
        slot_desc_->type().type == TYPE_TIMESTAMP &&
        parent->file_version_.application == "parquet-mr");
  }

  virtual ~ScalarColumnReader() { }

  virtual bool ReadValue(MemPool* pool, Tuple* tuple) {
    return ReadValue<true>(pool, tuple);
  }

  virtual bool ReadNonRepeatedValue(MemPool* pool, Tuple* tuple) {
    return ReadValue<false>(pool, tuple);
  }

  virtual bool NeedsSeedingForBatchedReading() const { return false; }

  virtual bool ReadValueBatch(MemPool* pool, int max_values, int tuple_size,
      uint8_t* tuple_mem, int* num_values) {
    return ReadValueBatch<true>(pool, max_values, tuple_size, tuple_mem, num_values);
  }

  virtual bool ReadNonRepeatedValueBatch(MemPool* pool, int max_values, int tuple_size,
      uint8_t* tuple_mem, int* num_values) {
    return ReadValueBatch<false>(pool, max_values, tuple_size, tuple_mem, num_values);
  }

 protected:
  template <bool IN_COLLECTION>
  inline bool ReadValue(MemPool* pool, Tuple* tuple) {
    // NextLevels() should have already been called and def and rep levels should be in
    // valid range.
    DCHECK_GE(rep_level_, 0);
    DCHECK_LE(rep_level_, max_rep_level());
    DCHECK_GE(def_level_, 0);
    DCHECK_LE(def_level_, max_def_level());
    DCHECK_GE(def_level_, def_level_of_immediate_repeated_ancestor()) <<
        "Caller should have called NextLevels() until we are ready to read a value";

    if (MATERIALIZED) {
      if (def_level_ >= max_def_level()) {
        if (page_encoding_ == parquet::Encoding::PLAIN_DICTIONARY) {
          if (!ReadSlot<true>(tuple->GetSlot(tuple_offset_), pool)) return false;
        } else {
          if (!ReadSlot<false>(tuple->GetSlot(tuple_offset_), pool)) return false;
        }
      } else {
        tuple->SetNull(null_indicator_offset_);
      }
    }
    return NextLevels<IN_COLLECTION>();
  }

  /// Implementation of the ReadValueBatch() functions specialized for this
  /// column reader type. This function drives the reading of data pages and
  /// caching of rep/def levels. Once a data page and cached levels are available,
  /// it calls into a more specialized MaterializeValueBatch() for doing the actual
  /// value materialization using the level caches.
  template<bool IN_COLLECTION>
  bool ReadValueBatch(MemPool* pool, int max_values, int tuple_size,
      uint8_t* tuple_mem, int* num_values) {
    // Repetition level is only present if this column is nested in a collection type.
    if (!IN_COLLECTION) DCHECK_EQ(max_rep_level(), 0) << slot_desc()->DebugString();
    if (IN_COLLECTION) DCHECK_GT(max_rep_level(), 0) << slot_desc()->DebugString();

    int val_count = 0;
    bool continue_execution = true;
    while (val_count < max_values && !RowGroupAtEnd() && continue_execution) {
      // Read next page if necessary.
      if (num_buffered_values_ == 0) {
        if (!NextPage()) {
          continue_execution = parent_->parse_status_.ok();
          continue;
        }
      }

      // Fill def/rep level caches if they are empty.
      int level_batch_size = min(parent_->state_->batch_size(), num_buffered_values_);
      if (!def_levels_.CacheHasNext()) {
        parent_->parse_status_.MergeStatus(def_levels_.CacheNextBatch(level_batch_size));
      }
      // We only need the repetition levels for populating the position slot since we
      // are only populating top-level tuples.
      if (IN_COLLECTION && pos_slot_desc_ != NULL && !rep_levels_.CacheHasNext()) {
        parent_->parse_status_.MergeStatus(rep_levels_.CacheNextBatch(level_batch_size));
      }
      if (UNLIKELY(!parent_->parse_status_.ok())) return false;

      // This special case is most efficiently handled here directly.
      if (!MATERIALIZED && !IN_COLLECTION) {
        int vals_to_add = min(def_levels_.CacheRemaining(), max_values - val_count);
        val_count += vals_to_add;
        def_levels_.CacheSkipLevels(vals_to_add);
        num_buffered_values_ -= vals_to_add;
        continue;
      }

      // Read data page and cached levels to materialize values.
      int cache_start_idx = def_levels_.CacheCurrIdx();
      uint8_t* next_tuple = tuple_mem + val_count * tuple_size;
      int remaining_val_capacity = max_values - val_count;
      int ret_val_count = 0;
      if (page_encoding_ == parquet::Encoding::PLAIN_DICTIONARY) {
        continue_execution = MaterializeValueBatch<IN_COLLECTION, true>(
            pool, remaining_val_capacity, tuple_size, next_tuple, &ret_val_count);
      } else {
        continue_execution = MaterializeValueBatch<IN_COLLECTION, false>(
            pool, remaining_val_capacity, tuple_size, next_tuple, &ret_val_count);
      }
      val_count += ret_val_count;
      num_buffered_values_ -= (def_levels_.CacheCurrIdx() - cache_start_idx);
    }
    *num_values = val_count;
    return continue_execution;
  }

  /// Helper function for ReadValueBatch() above that performs value materialization.
  /// It assumes a data page with remaining values is available, and that the def/rep
  /// level caches have been populated.
  /// For efficiency, the simple special case of !MATERIALIZED && !IN_COLLECTION is not
  /// handled in this function.
  template<bool IN_COLLECTION, bool IS_DICT_ENCODED>
  bool MaterializeValueBatch(MemPool* pool, int max_values, int tuple_size,
      uint8_t* tuple_mem, int* num_values) {
    DCHECK(MATERIALIZED || IN_COLLECTION);
    DCHECK_GT(num_buffered_values_, 0);
    DCHECK(def_levels_.CacheHasNext());
    if (IN_COLLECTION && pos_slot_desc_ != NULL) DCHECK(rep_levels_.CacheHasNext());

    uint8_t* curr_tuple = tuple_mem;
    int val_count = 0;
    while (def_levels_.CacheHasNext()) {
      Tuple* tuple = reinterpret_cast<Tuple*>(curr_tuple);
      int def_level = def_levels_.CacheGetNext();

      if (IN_COLLECTION) {
        if (def_level < def_level_of_immediate_repeated_ancestor()) {
          // A containing repeated field is empty or NULL. Skip the value but
          // move to the next repetition level if necessary.
          if (pos_slot_desc_ != NULL) rep_levels_.CacheGetNext();
          continue;
        }
        if (pos_slot_desc_ != NULL) {
          int rep_level = rep_levels_.CacheGetNext();
          // Reset position counter if we are at the start of a new parent collection.
          if (rep_level <= max_rep_level() - 1) pos_current_value_ = 0;
          void* pos_slot = tuple->GetSlot(pos_slot_desc()->tuple_offset());
          *reinterpret_cast<int64_t*>(pos_slot) = pos_current_value_++;
        }
      }

      if (MATERIALIZED) {
        if (def_level >= max_def_level()) {
          bool continue_execution =
              ReadSlot<IS_DICT_ENCODED>(tuple->GetSlot(tuple_offset_), pool);
          if (UNLIKELY(!continue_execution)) return false;
        } else {
          tuple->SetNull(null_indicator_offset_);
        }
      }

      curr_tuple += tuple_size;
      ++val_count;
      if (UNLIKELY(val_count == max_values)) break;
    }
    *num_values = val_count;
    return true;
  }

  virtual Status CreateDictionaryDecoder(uint8_t* values, int size,
      DictDecoderBase** decoder) {
    if (!dict_decoder_.Reset(values, size, fixed_len_size_)) {
        return Status(TErrorCode::PARQUET_CORRUPT_DICTIONARY, filename(),
            slot_desc_->type().DebugString(), "could not decode dictionary");
    }
    dict_decoder_init_ = true;
    *decoder = &dict_decoder_;
    return Status::OK();
  }

  virtual bool HasDictionaryDecoder() {
    return dict_decoder_init_;
  }

  virtual void ClearDictionaryDecoder() {
    dict_decoder_init_ = false;
  }

  virtual Status InitDataPage(uint8_t* data, int size) {
    page_encoding_ = current_page_header_.data_page_header.encoding;
    if (page_encoding_ != parquet::Encoding::PLAIN_DICTIONARY &&
        page_encoding_ != parquet::Encoding::PLAIN) {
      stringstream ss;
      ss << "File '" << filename() << "' is corrupt: unexpected encoding: "
         << PrintEncoding(page_encoding_) << " for data page of column '"
         << schema_element().name << "'.";
      return Status(ss.str());
    }

    // If slot_desc_ is NULL, dict_decoder_ is uninitialized
    if (page_encoding_ == parquet::Encoding::PLAIN_DICTIONARY && slot_desc_ != NULL) {
      if (!dict_decoder_init_) {
        return Status("File corrupt. Missing dictionary page.");
      }
      dict_decoder_.SetData(data, size);
    }

    // TODO: Perform filter selectivity checks here.
    return Status::OK();
  }

 private:
  /// Writes the next value into *slot using pool if necessary.
  ///
  /// Returns false if execution should be aborted for some reason, e.g. parse_error_ is
  /// set, the query is cancelled, or the scan node limit was reached. Otherwise returns
  /// true.
  template<bool IS_DICT_ENCODED>
  inline bool ReadSlot(void* slot, MemPool* pool) {
    T val;
    T* val_ptr = NeedsConversion() ? &val : reinterpret_cast<T*>(slot);
    if (IS_DICT_ENCODED) {
      DCHECK_EQ(page_encoding_, parquet::Encoding::PLAIN_DICTIONARY);
      if (UNLIKELY(!dict_decoder_.GetValue(val_ptr))) {
        SetDictDecodeError();
        return false;
      }
    } else {
      DCHECK_EQ(page_encoding_, parquet::Encoding::PLAIN);
      int encoded_len =
          ParquetPlainEncoder::Decode<T>(data_, data_end_, fixed_len_size_, val_ptr);
      if (UNLIKELY(encoded_len < 0)) {
        SetPlainDecodeError();
        return false;
      }
      data_ += encoded_len;
    }
    if (UNLIKELY(NeedsConversion() &&
            !ConvertSlot(&val, reinterpret_cast<T*>(slot), pool))) {
      return false;
    }
    return true;
  }

  /// Most column readers never require conversion, so we can avoid branches by
  /// returning constant false. Column readers for types that require conversion
  /// must specialize this function.
  inline bool NeedsConversion() const {
    DCHECK(!needs_conversion_);
    return false;
  }

  /// Converts and writes src into dst based on desc_->type()
  bool ConvertSlot(const T* src, T* dst, MemPool* pool) {
    DCHECK(false);
    return false;
  }

  /// Pull out slow-path Status construction code from ReadRepetitionLevel()/
  /// ReadDefinitionLevel() for performance.
  void __attribute__((noinline)) SetDictDecodeError() {
    parent_->parse_status_ = Status(TErrorCode::PARQUET_DICT_DECODE_FAILURE, filename(),
        slot_desc_->type().DebugString(), stream_->file_offset());
  }
  void __attribute__((noinline)) SetPlainDecodeError() {
    parent_->parse_status_ = Status(TErrorCode::PARQUET_CORRUPT_PLAIN_VALUE, filename(),
        slot_desc_->type().DebugString(), stream_->file_offset());
  }

  /// Dictionary decoder for decoding column values.
  DictDecoder<T> dict_decoder_;

  /// True if dict_decoder_ has been initialized with a dictionary page.
  bool dict_decoder_init_;

  /// true if decoded values must be converted before being written to an output tuple.
  bool needs_conversion_;

  /// The size of this column with plain encoding for FIXED_LEN_BYTE_ARRAY, or
  /// the max length for VARCHAR columns. Unused otherwise.
  int fixed_len_size_;
};

template<>
inline bool HdfsParquetScanner::ScalarColumnReader<StringValue, true>::NeedsConversion() const {
  return needs_conversion_;
}

template<>
bool HdfsParquetScanner::ScalarColumnReader<StringValue, true>::ConvertSlot(
    const StringValue* src, StringValue* dst, MemPool* pool) {
  DCHECK(slot_desc() != NULL);
  DCHECK(slot_desc()->type().type == TYPE_CHAR);
  int len = slot_desc()->type().len;
  StringValue sv;
  sv.len = len;
  if (slot_desc()->type().IsVarLenStringType()) {
    sv.ptr = reinterpret_cast<char*>(pool->TryAllocate(len));
    if (UNLIKELY(sv.ptr == NULL)) {
      string details = Substitute(PARQUET_MEM_LIMIT_EXCEEDED, "ConvertSlot",
          len, "StringValue");
      parent_->parse_status_ =
          pool->mem_tracker()->MemLimitExceeded(parent_->state_, details, len);
      return false;
    }
  } else {
    sv.ptr = reinterpret_cast<char*>(dst);
  }
  int unpadded_len = min(len, src->len);
  memcpy(sv.ptr, src->ptr, unpadded_len);
  StringValue::PadWithSpaces(sv.ptr, len, unpadded_len);

  if (slot_desc()->type().IsVarLenStringType()) *dst = sv;
  return true;
}

template<>
inline bool HdfsParquetScanner::ScalarColumnReader<TimestampValue, true>::NeedsConversion() const {
  return needs_conversion_;
}

template<>
bool HdfsParquetScanner::ScalarColumnReader<TimestampValue, true>::ConvertSlot(
    const TimestampValue* src, TimestampValue* dst, MemPool* pool) {
  // Conversion should only happen when this flag is enabled.
  DCHECK(FLAGS_convert_legacy_hive_parquet_utc_timestamps);
  *dst = *src;
  if (dst->HasDateAndTime()) dst->UtcToLocal();
  return true;
}

class HdfsParquetScanner::BoolColumnReader :
      public HdfsParquetScanner::BaseScalarColumnReader {
 public:
  BoolColumnReader(HdfsParquetScanner* parent, const SchemaNode& node,
      const SlotDescriptor* slot_desc)
    : BaseScalarColumnReader(parent, node, slot_desc) {
    if (slot_desc_ != NULL) DCHECK_EQ(slot_desc_->type().type, TYPE_BOOLEAN);
  }

  virtual ~BoolColumnReader() { }

  virtual bool ReadValue(MemPool* pool, Tuple* tuple) {
    return ReadValue<true>(pool, tuple);
  }

  virtual bool ReadNonRepeatedValue(MemPool* pool, Tuple* tuple) {
    return ReadValue<false>(pool, tuple);
  }

 protected:
  virtual Status CreateDictionaryDecoder(uint8_t* values, int size,
      DictDecoderBase** decoder) {
    DCHECK(false) << "Dictionary encoding is not supported for bools. Should never "
                  << "have gotten this far.";
    return Status::OK();
  }

  virtual bool HasDictionaryDecoder() {
    // Decoder should never be created for bools.
    return false;
  }

  virtual void ClearDictionaryDecoder() { }

  virtual Status InitDataPage(uint8_t* data, int size) {
    // Initialize bool decoder
    bool_values_ = BitReader(data, size);
    return Status::OK();
  }

 private:
  template<bool IN_COLLECTION>
  inline bool ReadValue(MemPool* pool, Tuple* tuple) {
    DCHECK(slot_desc_ != NULL);
    // Def and rep levels should be in valid range.
    DCHECK_GE(rep_level_, 0);
    DCHECK_LE(rep_level_, max_rep_level());
    DCHECK_GE(def_level_, 0);
    DCHECK_LE(def_level_, max_def_level());
    DCHECK_GE(def_level_, def_level_of_immediate_repeated_ancestor()) <<
        "Caller should have called NextLevels() until we are ready to read a value";

    if (def_level_ >= max_def_level()) {
      return ReadSlot<IN_COLLECTION>(tuple->GetSlot(tuple_offset_), pool);
    } else {
      // Null value
      tuple->SetNull(null_indicator_offset_);
      return NextLevels<IN_COLLECTION>();
    }
  }

  /// Writes the next value into *slot using pool if necessary. Also advances def_level_
  /// and rep_level_ via NextLevels().
  ///
  /// Returns false if execution should be aborted for some reason, e.g. parse_error_ is
  /// set, the query is cancelled, or the scan node limit was reached. Otherwise returns
  /// true.
  template <bool IN_COLLECTION>
  inline bool ReadSlot(void* slot, MemPool* pool)  {
    if (!bool_values_.GetValue(1, reinterpret_cast<bool*>(slot))) {
      parent_->parse_status_ = Status("Invalid bool column.");
      return false;
    }
    return NextLevels<IN_COLLECTION>();
  }

  BitReader bool_values_;
};

}

Status HdfsParquetScanner::Prepare(ScannerContext* context) {
  RETURN_IF_ERROR(HdfsScanner::Prepare(context));
  metadata_range_ = stream_->scan_range();
  num_cols_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumColumns", TUnit::UNIT);
  num_row_groups_counter_ =
      ADD_COUNTER(scan_node_->runtime_profile(), "NumRowGroups", TUnit::UNIT);

  scan_node_->IncNumScannersCodegenDisabled();

  level_cache_pool_.reset(new MemPool(scan_node_->mem_tracker()));

  for (int i = 0; i < context->filter_ctxs().size(); ++i) {
    const FilterContext* ctx = &context->filter_ctxs()[i];
    DCHECK(ctx->filter != NULL);
    if (!ctx->filter->AlwaysTrue()) filter_ctxs_.push_back(ctx);
  }
  filter_stats_.resize(filter_ctxs_.size());
  return Status::OK();
}

void HdfsParquetScanner::Close() {
  vector<THdfsCompression::type> compression_types;

  // Visit each column reader, including collection reader children.
  stack<ColumnReader*> readers;
  for (ColumnReader* r: column_readers_) readers.push(r);
  while (!readers.empty()) {
    ColumnReader* col_reader = readers.top();
    readers.pop();

    if (col_reader->IsCollectionReader()) {
      CollectionColumnReader* collection_reader =
          static_cast<CollectionColumnReader*>(col_reader);
      for (ColumnReader* r: *collection_reader->children()) readers.push(r);
      continue;
    }

    BaseScalarColumnReader* scalar_reader =
        static_cast<BaseScalarColumnReader*>(col_reader);
    if (scalar_reader->decompressed_data_pool() != NULL) {
      // No need to commit the row batches with the AttachPool() calls
      // since AddFinalRowBatch() already does below.
      AttachPool(scalar_reader->decompressed_data_pool(), false);
    }
    scalar_reader->Close();
    compression_types.push_back(scalar_reader->codec());
  }
  if (batch_ != NULL) {
    AttachPool(dictionary_pool_.get(), false);
    AttachPool(scratch_batch_->mem_pool(), false);
    AddFinalRowBatch();
  }
  // Verify all resources (if any) have been transferred.
  DCHECK_EQ(dictionary_pool_.get()->total_allocated_bytes(), 0);
  DCHECK_EQ(scratch_batch_->mem_pool()->total_allocated_bytes(), 0);
  DCHECK_EQ(context_->num_completed_io_buffers(), 0);
  // If this was a metadata only read (i.e. count(*)), there are no columns.
  if (compression_types.empty()) compression_types.push_back(THdfsCompression::NONE);
  scan_node_->RangeComplete(THdfsFileFormat::PARQUET, compression_types);
  assemble_rows_timer_.Stop();
  assemble_rows_timer_.ReleaseCounter();

  if (level_cache_pool_.get() != NULL) {
    level_cache_pool_->FreeAll();
    level_cache_pool_.reset(NULL);
  }

  for (int i = 0; i < filter_ctxs_.size(); ++i) {
    const FilterStats* stats = filter_ctxs_[i]->stats;
    const LocalFilterStats& local = filter_stats_[i];
    stats->IncrCounters(FilterStats::ROWS_KEY, local.total_possible,
        local.considered, local.rejected);
  }

  HdfsScanner::Close();
}

HdfsParquetScanner::ColumnReader* HdfsParquetScanner::CreateReader(
    const SchemaNode& node, bool is_collection_field, const SlotDescriptor* slot_desc) {
  ColumnReader* reader = NULL;
  if (is_collection_field) {
    // Create collection reader (note this handles both NULL and non-NULL 'slot_desc')
    reader = new CollectionColumnReader(this, node, slot_desc);
  } else if (slot_desc != NULL) {
    // Create the appropriate ScalarColumnReader type to read values into 'slot_desc'
    switch (slot_desc->type().type) {
      case TYPE_BOOLEAN:
        reader = new BoolColumnReader(this, node, slot_desc);
        break;
      case TYPE_TINYINT:
        reader = new ScalarColumnReader<int8_t, true>(this, node, slot_desc);
        break;
      case TYPE_SMALLINT:
        reader = new ScalarColumnReader<int16_t, true>(this, node, slot_desc);
        break;
      case TYPE_INT:
        reader = new ScalarColumnReader<int32_t, true>(this, node, slot_desc);
        break;
      case TYPE_BIGINT:
        reader = new ScalarColumnReader<int64_t, true>(this, node, slot_desc);
        break;
      case TYPE_FLOAT:
        reader = new ScalarColumnReader<float, true>(this, node, slot_desc);
        break;
      case TYPE_DOUBLE:
        reader = new ScalarColumnReader<double, true>(this, node, slot_desc);
        break;
      case TYPE_TIMESTAMP:
        reader = new ScalarColumnReader<TimestampValue, true>(this, node, slot_desc);
        break;
      case TYPE_STRING:
      case TYPE_VARCHAR:
      case TYPE_CHAR:
        reader = new ScalarColumnReader<StringValue, true>(this, node, slot_desc);
        break;
      case TYPE_DECIMAL:
        switch (slot_desc->type().GetByteSize()) {
          case 4:
            reader = new ScalarColumnReader<Decimal4Value, true>(this, node, slot_desc);
            break;
          case 8:
            reader = new ScalarColumnReader<Decimal8Value, true>(this, node, slot_desc);
            break;
          case 16:
            reader = new ScalarColumnReader<Decimal16Value, true>(this, node, slot_desc);
            break;
        }
        break;
      default:
        DCHECK(false) << slot_desc->type().DebugString();
    }
  } else {
    // Special case for counting scalar values (e.g. count(*), no materialized columns in
    // the file, only materializing a position slot). We won't actually read any values,
    // only the rep and def levels, so it doesn't matter what kind of reader we make.
    reader = new ScalarColumnReader<int8_t, false>(this, node, slot_desc);
  }
  return obj_pool_.Add(reader);
}

bool HdfsParquetScanner::ColumnReader::ReadValueBatch(MemPool* pool, int max_values,
    int tuple_size, uint8_t* tuple_mem, int* num_values) {
  int val_count = 0;
  bool continue_execution = true;
  while (val_count < max_values && !RowGroupAtEnd() && continue_execution) {
    Tuple* tuple = reinterpret_cast<Tuple*>(tuple_mem + val_count * tuple_size);
    if (def_level_ < def_level_of_immediate_repeated_ancestor()) {
      // A containing repeated field is empty or NULL
      continue_execution = NextLevels();
      continue;
    }
    // Fill in position slot if applicable
    if (pos_slot_desc_ != NULL) ReadPosition(tuple);
    continue_execution = ReadValue(pool, tuple);
    ++val_count;
  }
  *num_values = val_count;
  return continue_execution;
}

bool HdfsParquetScanner::ColumnReader::ReadNonRepeatedValueBatch(MemPool* pool,
    int max_values, int tuple_size, uint8_t* tuple_mem, int* num_values) {
  int val_count = 0;
  bool continue_execution = true;
  while (val_count < max_values && !RowGroupAtEnd() && continue_execution) {
    Tuple* tuple = reinterpret_cast<Tuple*>(tuple_mem + val_count * tuple_size);
    continue_execution = ReadNonRepeatedValue(pool, tuple);
    ++val_count;
  }
  *num_values = val_count;
  return continue_execution;
}

void HdfsParquetScanner::ColumnReader::ReadPosition(Tuple* tuple) {
  DCHECK(pos_slot_desc() != NULL);
  // NextLevels() should have already been called
  DCHECK_GE(rep_level_, 0);
  DCHECK_GE(def_level_, 0);
  DCHECK_GE(pos_current_value_, 0);
  DCHECK_GE(def_level_, def_level_of_immediate_repeated_ancestor()) <<
      "Caller should have called NextLevels() until we are ready to read a value";

  void* slot = tuple->GetSlot(pos_slot_desc()->tuple_offset());
  *reinterpret_cast<int64_t*>(slot) = pos_current_value_++;
}

// In 1.1, we had a bug where the dictionary page metadata was not set. Returns true
// if this matches those versions and compatibility workarounds need to be used.
static bool RequiresSkippedDictionaryHeaderCheck(
    const HdfsParquetScanner::FileVersion& v) {
  if (v.application != "impala") return false;
  return v.VersionEq(1,1,0) || (v.VersionEq(1,2,0) && v.is_impala_internal);
}

Status HdfsParquetScanner::BaseScalarColumnReader::ReadDataPage() {
  Status status;
  uint8_t* buffer;

  // We're about to move to the next data page.  The previous data page is
  // now complete, pass along the memory allocated for it.
  parent_->scratch_batch_->mem_pool()->AcquireData(decompressed_data_pool_.get(), false);

  // Read the next data page, skipping page types we don't care about.
  // We break out of this loop on the non-error case (a data page was found or we read all
  // the pages).
  while (true) {
    DCHECK_EQ(num_buffered_values_, 0);
    if (num_values_read_ == metadata_->num_values) {
      // No more pages to read
      // TODO: should we check for stream_->eosr()?
      break;
    } else if (num_values_read_ > metadata_->num_values) {
      ErrorMsg msg(TErrorCode::PARQUET_COLUMN_METADATA_INVALID,
          metadata_->num_values, num_values_read_, node_.element->name, filename());
      RETURN_IF_ERROR(parent_->LogOrReturnError(msg));
      return Status::OK();
    }

    int64_t buffer_size;
    RETURN_IF_ERROR(stream_->GetBuffer(true, &buffer, &buffer_size));
    if (buffer_size == 0) {
      // The data pages contain fewer values than stated in the column metadata.
      DCHECK(stream_->eosr());
      DCHECK_LT(num_values_read_, metadata_->num_values);
      // TODO for 2.3: node_.element->name isn't necessarily useful
      ErrorMsg msg(TErrorCode::PARQUET_COLUMN_METADATA_INVALID,
          metadata_->num_values, num_values_read_, node_.element->name, filename());
      RETURN_IF_ERROR(parent_->LogOrReturnError(msg));
      return Status::OK();
    }

    // We don't know the actual header size until the thrift object is deserialized.  Loop
    // until we successfully deserialize the header or exceed the maximum header size.
    uint32_t header_size;
    while (true) {
      header_size = buffer_size;
      status = DeserializeThriftMsg(
          buffer, &header_size, true, &current_page_header_);
      if (status.ok()) break;

      if (buffer_size >= FLAGS_max_page_header_size) {
        stringstream ss;
        ss << "ParquetScanner: could not read data page because page header exceeded "
           << "maximum size of "
           << PrettyPrinter::Print(FLAGS_max_page_header_size, TUnit::BYTES);
        status.AddDetail(ss.str());
        return status;
      }

      // Didn't read entire header, increase buffer size and try again
      Status status;
      int64_t new_buffer_size = max<int64_t>(buffer_size * 2, 1024);
      bool success = stream_->GetBytes(
          new_buffer_size, &buffer, &new_buffer_size, &status, /* peek */ true);
      if (!success) {
        DCHECK(!status.ok());
        return status;
      }
      DCHECK(status.ok());

      if (buffer_size == new_buffer_size) {
        DCHECK_NE(new_buffer_size, 0);
        return Status(TErrorCode::PARQUET_HEADER_EOF, filename());
      }
      DCHECK_GT(new_buffer_size, buffer_size);
      buffer_size = new_buffer_size;
    }

    // Successfully deserialized current_page_header_
    if (!stream_->SkipBytes(header_size, &status)) return status;

    int data_size = current_page_header_.compressed_page_size;
    int uncompressed_size = current_page_header_.uncompressed_page_size;

    if (current_page_header_.type == parquet::PageType::DICTIONARY_PAGE) {
      if (slot_desc_ == NULL) {
        // Skip processing the dictionary page if we don't need to decode any values. In
        // addition to being unnecessary, we are likely unable to successfully decode the
        // dictionary values because we don't necessarily create the right type of scalar
        // reader if there's no slot to read into (see CreateReader()).
        if (!stream_->ReadBytes(data_size, &data_, &status)) return status;
        continue;
      }

      if (HasDictionaryDecoder()) {
        return Status("Column chunk should not contain two dictionary pages.");
      }
      if (node_.element->type == parquet::Type::BOOLEAN) {
        return Status("Unexpected dictionary page. Dictionary page is not"
            " supported for booleans.");
      }
      const parquet::DictionaryPageHeader* dict_header = NULL;
      if (current_page_header_.__isset.dictionary_page_header) {
        dict_header = &current_page_header_.dictionary_page_header;
      } else {
        if (!RequiresSkippedDictionaryHeaderCheck(parent_->file_version_)) {
          return Status("Dictionary page does not have dictionary header set.");
        }
      }
      if (dict_header != NULL &&
          dict_header->encoding != parquet::Encoding::PLAIN &&
          dict_header->encoding != parquet::Encoding::PLAIN_DICTIONARY) {
        return Status("Only PLAIN and PLAIN_DICTIONARY encodings are supported "
            "for dictionary pages.");
      }

      if (!stream_->ReadBytes(data_size, &data_, &status)) return status;
      data_end_ = data_ + data_size;

      uint8_t* dict_values = NULL;
      if (decompressor_.get() != NULL) {
        dict_values = parent_->dictionary_pool_->TryAllocate(uncompressed_size);
        if (UNLIKELY(dict_values == NULL)) {
          string details = Substitute(PARQUET_MEM_LIMIT_EXCEEDED, "ReadDataPage",
              uncompressed_size, "dictionary");
          return parent_->dictionary_pool_->mem_tracker()->MemLimitExceeded(
              parent_->state_, details, uncompressed_size);
        }
        RETURN_IF_ERROR(decompressor_->ProcessBlock32(true, data_size, data_,
            &uncompressed_size, &dict_values));
        VLOG_FILE << "Decompressed " << data_size << " to " << uncompressed_size;
        if (current_page_header_.uncompressed_page_size != uncompressed_size) {
          return Status(Substitute("Error decompressing dictionary page in file '$0'. "
              "Expected $1 uncompressed bytes but got $2", filename(),
              current_page_header_.uncompressed_page_size, uncompressed_size));
        }
        data_size = uncompressed_size;
      } else {
        if (current_page_header_.uncompressed_page_size != data_size) {
          return Status(Substitute("Error reading dictionary page in file '$0'. "
              "Expected $1 bytes but got $2", filename(),
              current_page_header_.uncompressed_page_size, data_size));
        }
        // Copy dictionary from io buffer (which will be recycled as we read
        // more data) to a new buffer
        dict_values = parent_->dictionary_pool_->TryAllocate(data_size);
        if (UNLIKELY(dict_values == NULL)) {
          string details = Substitute(PARQUET_MEM_LIMIT_EXCEEDED, "ReadDataPage",
              data_size, "dictionary");
          return parent_->dictionary_pool_->mem_tracker()->MemLimitExceeded(
              parent_->state_, details, data_size);
        }
        memcpy(dict_values, data_, data_size);
      }

      DictDecoderBase* dict_decoder;
      RETURN_IF_ERROR(CreateDictionaryDecoder(dict_values, data_size, &dict_decoder));
      if (dict_header != NULL &&
          dict_header->num_values != dict_decoder->num_entries()) {
        return Status(TErrorCode::PARQUET_CORRUPT_DICTIONARY, filename(),
            slot_desc_->type().DebugString(),
            Substitute("Expected $0 entries but data contained $1 entries",
            dict_header->num_values, dict_decoder->num_entries()));
      }
      // Done with dictionary page, read next page
      continue;
    }

    if (current_page_header_.type != parquet::PageType::DATA_PAGE) {
      // We can safely skip non-data pages
      if (!stream_->SkipBytes(data_size, &status)) return status;
      continue;
    }

    // Read Data Page
    // TODO: when we start using page statistics, we will need to ignore certain corrupt
    // statistics. See IMPALA-2208 and PARQUET-251.
    if (!stream_->ReadBytes(data_size, &data_, &status)) return status;
    data_end_ = data_ + data_size;
    num_buffered_values_ = current_page_header_.data_page_header.num_values;
    num_values_read_ += num_buffered_values_;

    if (decompressor_.get() != NULL) {
      SCOPED_TIMER(parent_->decompress_timer_);
      uint8_t* decompressed_buffer =
          decompressed_data_pool_->TryAllocate(uncompressed_size);
      if (UNLIKELY(decompressed_buffer == NULL)) {
        string details = Substitute(PARQUET_MEM_LIMIT_EXCEEDED, "ReadDataPage",
            uncompressed_size, "decompressed data");
        return decompressed_data_pool_->mem_tracker()->MemLimitExceeded(
            parent_->state_, details, uncompressed_size);
      }
      RETURN_IF_ERROR(decompressor_->ProcessBlock32(true,
          current_page_header_.compressed_page_size, data_, &uncompressed_size,
          &decompressed_buffer));
      VLOG_FILE << "Decompressed " << current_page_header_.compressed_page_size
                << " to " << uncompressed_size;
      if (current_page_header_.uncompressed_page_size != uncompressed_size) {
        return Status(Substitute("Error decompressing data page in file '$0'. "
            "Expected $1 uncompressed bytes but got $2", filename(),
            current_page_header_.uncompressed_page_size, uncompressed_size));
      }
      data_ = decompressed_buffer;
      data_size = current_page_header_.uncompressed_page_size;
      data_end_ = data_ + data_size;
    } else {
      DCHECK_EQ(metadata_->codec, parquet::CompressionCodec::UNCOMPRESSED);
      if (current_page_header_.compressed_page_size != uncompressed_size) {
        return Status(Substitute("Error reading data page in file '$0'. "
            "Expected $1 bytes but got $2", filename(),
            current_page_header_.compressed_page_size, uncompressed_size));
      }
    }

    // Initialize the repetition level data
    RETURN_IF_ERROR(rep_levels_.Init(filename(),
        current_page_header_.data_page_header.repetition_level_encoding,
        parent_->level_cache_pool_.get(), parent_->state_->batch_size(),
        max_rep_level(), num_buffered_values_,
        &data_, &data_size));

    // Initialize the definition level data
    RETURN_IF_ERROR(def_levels_.Init(filename(),
        current_page_header_.data_page_header.definition_level_encoding,
        parent_->level_cache_pool_.get(), parent_->state_->batch_size(),
        max_def_level(), num_buffered_values_, &data_, &data_size));

    // Data can be empty if the column contains all NULLs
    if (data_size != 0) RETURN_IF_ERROR(InitDataPage(data_, data_size));
    break;
  }

  return Status::OK();
}

Status HdfsParquetScanner::LevelDecoder::Init(const string& filename,
    parquet::Encoding::type encoding, MemPool* cache_pool, int cache_size,
    int max_level, int num_buffered_values, uint8_t** data, int* data_size) {
  encoding_ = encoding;
  max_level_ = max_level;
  num_buffered_values_ = num_buffered_values;
  filename_ = filename;
  RETURN_IF_ERROR(InitCache(cache_pool, cache_size));

  // Return because there is no level data to read, e.g., required field.
  if (max_level == 0) return Status::OK();

  int32_t num_bytes = 0;
  switch (encoding) {
    case parquet::Encoding::RLE: {
      Status status;
      if (!ReadWriteUtil::Read(data, data_size, &num_bytes, &status)) {
        return status;
      }
      if (num_bytes < 0) {
        return Status(TErrorCode::PARQUET_CORRUPT_RLE_BYTES, filename, num_bytes);
      }
      int bit_width = Bits::Log2Ceiling64(max_level + 1);
      Reset(*data, num_bytes, bit_width);
      break;
    }
    case parquet::Encoding::BIT_PACKED:
      num_bytes = BitUtil::Ceil(num_buffered_values, 8);
      bit_reader_.Reset(*data, num_bytes);
      break;
    default: {
      stringstream ss;
      ss << "Unsupported encoding: " << encoding;
      return Status(ss.str());
    }
  }
  DCHECK_GT(num_bytes, 0);
  *data += num_bytes;
  *data_size -= num_bytes;
  return Status::OK();
}

Status HdfsParquetScanner::LevelDecoder::InitCache(MemPool* pool, int cache_size) {
  num_cached_levels_ = 0;
  cached_level_idx_ = 0;
  // Memory has already been allocated.
  if (cached_levels_ != NULL) {
    DCHECK_EQ(cache_size_, cache_size);
    return Status::OK();
  }

  cached_levels_ = reinterpret_cast<uint8_t*>(pool->TryAllocate(cache_size));
  if (cached_levels_ == NULL) {
    return pool->mem_tracker()->MemLimitExceeded(
        NULL, "Definition level cache", cache_size);
  }
  memset(cached_levels_, 0, cache_size);
  cache_size_ = cache_size;
  return Status::OK();
}

inline int16_t HdfsParquetScanner::LevelDecoder::ReadLevel() {
  bool valid;
  uint8_t level;
  if (encoding_ == parquet::Encoding::RLE) {
    valid = Get(&level);
  } else {
    DCHECK_EQ(encoding_, parquet::Encoding::BIT_PACKED);
    valid = bit_reader_.GetValue(1, &level);
  }
  return LIKELY(valid) ? level : INVALID_LEVEL;
}

Status HdfsParquetScanner::LevelDecoder::CacheNextBatch(int batch_size) {
  DCHECK_LE(batch_size, cache_size_);
  cached_level_idx_ = 0;
  if (max_level_ > 0) {
    if (UNLIKELY(!FillCache(batch_size, &num_cached_levels_))) {
      return Status(decoding_error_code_, num_buffered_values_, filename_);
    }
  } else {
    // No levels to read, e.g., because the field is required. The cache was
    // already initialized with all zeros, so we can hand out those values.
    DCHECK_EQ(max_level_, 0);
    num_cached_levels_ = batch_size;
  }
  return Status::OK();
}

bool HdfsParquetScanner::LevelDecoder::FillCache(int batch_size,
    int* num_cached_levels) {
  DCHECK(num_cached_levels != NULL);
  int num_values = 0;
  if (encoding_ == parquet::Encoding::RLE) {
    while (true) {
      // Add RLE encoded values by repeating the current value this number of times.
      uint32_t num_repeats_to_set =
          min<uint32_t>(repeat_count_, batch_size - num_values);
      memset(cached_levels_ + num_values, current_value_, num_repeats_to_set);
      num_values += num_repeats_to_set;
      repeat_count_ -= num_repeats_to_set;

      // Add remaining literal values, if any.
      uint32_t num_literals_to_set =
          min<uint32_t>(literal_count_, batch_size - num_values);
      int num_values_end = min<uint32_t>(num_values + literal_count_, batch_size);
      for (; num_values < num_values_end; ++num_values) {
        bool valid = bit_reader_.GetValue(bit_width_, &cached_levels_[num_values]);
        if (UNLIKELY(!valid || cached_levels_[num_values] > max_level_)) return false;
      }
      literal_count_ -= num_literals_to_set;

      if (num_values == batch_size) break;
      if (UNLIKELY(!NextCounts<int16_t>())) return false;
      if (repeat_count_ > 0 && current_value_ > max_level_) return false;
    }
  } else {
    DCHECK_EQ(encoding_, parquet::Encoding::BIT_PACKED);
    for (; num_values < batch_size; ++num_values) {
      bool valid = bit_reader_.GetValue(1, &cached_levels_[num_values]);
      if (UNLIKELY(!valid || cached_levels_[num_values] > max_level_)) return false;
    }
  }
  *num_cached_levels = num_values;
  return true;
}

template <bool ADVANCE_REP_LEVEL>
bool HdfsParquetScanner::BaseScalarColumnReader::NextLevels() {
  if (!ADVANCE_REP_LEVEL) DCHECK_EQ(max_rep_level(), 0) << slot_desc()->DebugString();

  if (UNLIKELY(num_buffered_values_ == 0)) {
    if (!NextPage()) return parent_->parse_status_.ok();
  }
  --num_buffered_values_;

  // Definition level is not present if column and any containing structs are required.
  def_level_ = max_def_level() == 0 ? 0 : def_levels_.ReadLevel();

  if (ADVANCE_REP_LEVEL && max_rep_level() > 0) {
    // Repetition level is only present if this column is nested in any collection type.
    rep_level_ = rep_levels_.ReadLevel();
    // Reset position counter if we are at the start of a new parent collection.
    if (rep_level_ <= max_rep_level() - 1) pos_current_value_ = 0;
  }

  return parent_->parse_status_.ok();
}

bool HdfsParquetScanner::BaseScalarColumnReader::NextPage() {
  parent_->assemble_rows_timer_.Stop();
  parent_->parse_status_ = ReadDataPage();
  if (UNLIKELY(!parent_->parse_status_.ok())) return false;
  if (num_buffered_values_ == 0) {
    rep_level_ = ROW_GROUP_END;
    def_level_ = INVALID_LEVEL;
    pos_current_value_ = INVALID_POS;
    return false;
  }
  parent_->assemble_rows_timer_.Start();
  return true;
}

bool HdfsParquetScanner::CollectionColumnReader::NextLevels() {
  DCHECK(!children_.empty());
  DCHECK_LE(rep_level_, new_collection_rep_level());
  for (int c = 0; c < children_.size(); ++c) {
    do {
      // TODO(skye): verify somewhere that all column readers are at end
      if (!children_[c]->NextLevels()) return false;
    } while (children_[c]->rep_level() > new_collection_rep_level());
  }
  UpdateDerivedState();
  return true;
}

bool HdfsParquetScanner::CollectionColumnReader::ReadValue(MemPool* pool, Tuple* tuple) {
  DCHECK_GE(rep_level_, 0);
  DCHECK_GE(def_level_, 0);
  DCHECK_GE(def_level_, def_level_of_immediate_repeated_ancestor()) <<
      "Caller should have called NextLevels() until we are ready to read a value";

  if (tuple_offset_ == -1) {
    return CollectionColumnReader::NextLevels();
  } else if (def_level_ >= max_def_level()) {
    return ReadSlot(tuple->GetSlot(tuple_offset_), pool);
  } else {
    // Null value
    tuple->SetNull(null_indicator_offset_);
    return CollectionColumnReader::NextLevels();
  }
}

bool HdfsParquetScanner::CollectionColumnReader::ReadNonRepeatedValue(
    MemPool* pool, Tuple* tuple) {
  return CollectionColumnReader::ReadValue(pool, tuple);
}

bool HdfsParquetScanner::CollectionColumnReader::ReadSlot(void* slot, MemPool* pool) {
  DCHECK(!children_.empty());
  DCHECK_LE(rep_level_, new_collection_rep_level());

  // Recursively read the collection into a new CollectionValue.
  CollectionValue* coll_slot = reinterpret_cast<CollectionValue*>(slot);
  *coll_slot = CollectionValue();
  CollectionValueBuilder builder(
      coll_slot, *slot_desc_->collection_item_descriptor(), pool, parent_->state_);
  bool continue_execution = parent_->AssembleCollection(
      children_, new_collection_rep_level(), &builder);
  if (!continue_execution) return false;

  // AssembleCollection() advances child readers, so we don't need to call NextLevels()
  UpdateDerivedState();
  return true;
}

void HdfsParquetScanner::CollectionColumnReader::UpdateDerivedState() {
  // We don't need to cap our def_level_ at max_def_level(). We always check def_level_
  // >= max_def_level() to check if the collection is defined.
  // TODO(skye): consider capping def_level_ at max_def_level()
  def_level_ = children_[0]->def_level();
  rep_level_ = children_[0]->rep_level();

  // All children should have been advanced to the beginning of the next collection
  for (int i = 0; i < children_.size(); ++i) {
    DCHECK_EQ(children_[i]->rep_level(), rep_level_);
    if (def_level_ < max_def_level()) {
      // Collection not defined
      FILE_CHECK_EQ(children_[i]->def_level(), def_level_);
    } else {
      // Collection is defined
      FILE_CHECK_GE(children_[i]->def_level(), max_def_level());
    }
  }

  if (RowGroupAtEnd()) {
    // No more values
    pos_current_value_ = INVALID_POS;
  } else if (rep_level_ <= max_rep_level() - 2) {
    // Reset position counter if we are at the start of a new parent collection (i.e.,
    // the current collection is the first item in a new parent collection).
    pos_current_value_ = 0;
  }
}

Status HdfsParquetScanner::ValidateColumnOffsets(const parquet::RowGroup& row_group) {
  const HdfsFileDesc* file_desc = scan_node_->GetFileDesc(filename());
  for (int i = 0; i < row_group.columns.size(); ++i) {
    const parquet::ColumnChunk& col_chunk = row_group.columns[i];
    int64_t col_start = col_chunk.meta_data.data_page_offset;
    // The file format requires that if a dictionary page exists, it be before data pages.
    if (col_chunk.meta_data.__isset.dictionary_page_offset) {
      if (col_chunk.meta_data.dictionary_page_offset >= col_start) {
        stringstream ss;
        ss << "File " << file_desc->filename << ": metadata is corrupt. "
            << "Dictionary page (offset=" << col_chunk.meta_data.dictionary_page_offset
            << ") must come before any data pages (offset=" << col_start << ").";
        return Status(ss.str());
      }
      col_start = col_chunk.meta_data.dictionary_page_offset;
    }
    int64_t col_len = col_chunk.meta_data.total_compressed_size;
    int64_t col_end = col_start + col_len;
    if (col_end <= 0 || col_end > file_desc->file_length) {
      stringstream ss;
      ss << "File " << file_desc->filename << ": metadata is corrupt. "
          << "Column " << i << " has invalid column offsets "
          << "(offset=" << col_start << ", size=" << col_len << ", "
          << "file_size=" << file_desc->file_length << ").";
      return Status(ss.str());
    }
  }
  return Status::OK();
}

// Get the start of the column.
static int64_t GetColumnStartOffset(const parquet::ColumnMetaData& column) {
  if (column.__isset.dictionary_page_offset) {
    DCHECK_LT(column.dictionary_page_offset, column.data_page_offset);
    return column.dictionary_page_offset;
  }
  return column.data_page_offset;
}

// Get the file offset of the middle of the row group.
static int64_t GetRowGroupMidOffset(const parquet::RowGroup& row_group) {
  int64_t start_offset = GetColumnStartOffset(row_group.columns[0].meta_data);

  const parquet::ColumnMetaData& last_column =
      row_group.columns[row_group.columns.size() - 1].meta_data;
  int64_t end_offset =
      GetColumnStartOffset(last_column) + last_column.total_compressed_size;

  return start_offset + (end_offset - start_offset) / 2;
}

int HdfsParquetScanner::CountScalarColumns(const vector<ColumnReader*>& column_readers) {
  DCHECK(!column_readers.empty());
  int num_columns = 0;
  stack<ColumnReader*> readers;
  for (ColumnReader* r: column_readers_) readers.push(r);
  while (!readers.empty()) {
    ColumnReader* col_reader = readers.top();
    readers.pop();
    if (col_reader->IsCollectionReader()) {
      CollectionColumnReader* collection_reader =
          static_cast<CollectionColumnReader*>(col_reader);
      for (ColumnReader* r: *collection_reader->children()) readers.push(r);
      continue;
    }
    ++num_columns;
  }
  return num_columns;
}

void HdfsParquetScanner::CreateTupleRow(int row_group_idx, TupleRow* row) {
  Tuple* min_tuple = scan_node_->InitEmptyTemplateTuple(*scan_node_->tuple_desc());
  Tuple* max_tuple = scan_node_->InitEmptyTemplateTuple(*scan_node_->tuple_desc());
  row->SetTuple(0, min_tuple);
  row->SetTuple(1, max_tuple);
  for (int i = 0; i < column_readers_.size(); ++i) {
    BaseScalarColumnReader* scalar_reader =
        static_cast<BaseScalarColumnReader*>(column_readers_[i]);
    const parquet::Statistics& statistics =
        file_metadata_.row_groups[row_group_idx].columns[scalar_reader->col_idx()].meta_data.statistics;
    SlotDescriptor* slot_desc = scan_node_->materialized_slots()[i];
    if (!statistics.__isset.min || !statistics.__isset.max) {
      min_tuple->SetNull(slot_desc->null_indicator_offset());
      max_tuple->SetNull(slot_desc->null_indicator_offset());
    }
    void* min_dst = min_tuple->GetSlot(slot_desc->tuple_offset());
    void* max_dst = max_tuple->GetSlot(slot_desc->tuple_offset());
    switch(slot_desc->type().type) {
      case TYPE_TINYINT:
      case TYPE_SMALLINT:
      case TYPE_INT:
      case TYPE_BIGINT:
      case TYPE_FLOAT:
      case TYPE_DOUBLE: {
        RawValue::Write(statistics.min.c_str(), min_dst, slot_desc->type(), NULL);
        RawValue::Write(statistics.max.c_str(), max_dst, slot_desc->type(), NULL);
        break;
      }
      default:
        min_tuple->SetNull(slot_desc->null_indicator_offset());
        max_tuple->SetNull(slot_desc->null_indicator_offset());
        break;
    }
  }
}

Status HdfsParquetScanner::ProcessSplit() {
  DCHECK(parse_status_.ok()) << "Invalid parse_status_" << parse_status_.GetDetail();
  // First process the file metadata in the footer
  bool eosr;
  RETURN_IF_ERROR(ProcessFooter(&eosr));

  if (eosr) return Status::OK();

  // We've processed the metadata and there are columns that need to be materialized.
  RETURN_IF_ERROR(CreateColumnReaders(*scan_node_->tuple_desc(), &column_readers_));
  COUNTER_SET(num_cols_counter_,
      static_cast<int64_t>(CountScalarColumns(column_readers_)));
  // Set top-level template tuple.
  template_tuple_ = template_tuple_map_[scan_node_->tuple_desc()];

  // The scanner-wide stream was used only to read the file footer.  Each column has added
  // its own stream.
  stream_ = NULL;

  // Iterate through each row group in the file and process any row groups that fall
  // within this split.
  for (int i = 0; i < file_metadata_.row_groups.size(); ++i) {
    const parquet::RowGroup& row_group = file_metadata_.row_groups[i];
    if (row_group.num_rows == 0) continue;

    const DiskIoMgr::ScanRange* split_range =
        reinterpret_cast<ScanRangeMetadata*>(metadata_range_->meta_data())->original_split;
    RETURN_IF_ERROR(ValidateColumnOffsets(row_group));

    int64_t row_group_mid_pos = GetRowGroupMidOffset(row_group);
    int64_t split_offset = split_range->offset();
    int64_t split_length = split_range->len();
    if (!(row_group_mid_pos >= split_offset &&
        row_group_mid_pos < split_offset + split_length)) continue;
    COUNTER_ADD(num_row_groups_counter_, 1);

    // Attach any resources and clear the streams before starting a new row group. These
    // streams could either be just the footer stream or streams for the previous row
    // group.
    context_->ReleaseCompletedResources(batch_, /* done */ true);
    // Commit the rows to flush the row batch from the previous row group
    CommitRows(0);

    RETURN_IF_ERROR(InitColumns(i, column_readers_));

    // IMPALA-2328 Parquet scan should use min/max statistics to skip blocks based on predicate
    TupleRow* row = new TupleRow();
    CreateTupleRow(i, row);
    if (!StatisticsEvalConjuncts(row)) {
        continue;
    }

    assemble_rows_timer_.Start();

    // Prepare column readers for first read
    bool continue_execution = true;
    for (ColumnReader* col_reader: column_readers_) {
      // Seed collection and boolean column readers with NextLevel().
      // The ScalarColumnReaders use an optimized ReadValueBatch() that
      // should not be seeded.
      // TODO: Refactor the column readers to look more like the optimized
      // ScalarColumnReader::ReadValueBatch() which does not need seeding. This
      // will allow better sharing of code between the row-wise and column-wise
      // materialization strategies.
      if (col_reader->NeedsSeedingForBatchedReading()) {
        continue_execution = col_reader->NextLevels();
      }
      if (!continue_execution) break;
      DCHECK(parse_status_.ok()) << "Invalid parse_status_" << parse_status_.GetDetail();
    }

    bool filters_pass = true;
    if (continue_execution) {
      continue_execution = AssembleRows(column_readers_, i, &filters_pass);
      assemble_rows_timer_.Stop();
    }

    // Check the query_status_ before logging the parse_status_. query_status_ is merged
    // with parse_status_ in AssembleRows(). It's misleading to log query_status_ as parse
    // error because it is shared by all threads in the same fragment instance and it's
    // unclear which threads caused the error.
    //
    // TODO: It's a really bad idea to propagate UDF error via the global RuntimeState.
    // Store UDF error in thread local storage or make UDF return status so it can merge
    // with parse_status_.
    RETURN_IF_ERROR(state_->GetQueryStatus());
    if (UNLIKELY(!parse_status_.ok())) {
      RETURN_IF_ERROR(LogOrReturnError(parse_status_.msg()));
    }
    if (scan_node_->ReachedLimit()) return Status::OK();
    if (context_->cancelled()) return Status::OK();
    if (!filters_pass) return Status::OK();

    DCHECK(continue_execution || !state_->abort_on_error());
    // We should be at the end of the row group if we get this far with no parse error
    if (parse_status_.ok()) DCHECK(column_readers_[0]->RowGroupAtEnd());
    // Reset parse_status_ for the next row group.
    parse_status_ = Status::OK();
  }
  return Status::OK();
}

int HdfsParquetScanner::TransferScratchTuples() {
  // This function must not be called when the output batch is already full. As long as
  // we always call CommitRows() after TransferScratchTuples(), the output batch can
  // never be empty.
  DCHECK_LT(batch_->num_rows(), batch_->capacity());

  const bool has_filters = !filter_ctxs_.empty();
  const bool has_conjuncts = !scanner_conjunct_ctxs_->empty();
  ExprContext* const* conjunct_ctxs = &(*scanner_conjunct_ctxs_)[0];
  const int num_conjuncts = scanner_conjunct_ctxs_->size();

  // Start/end/current iterators over the output rows.
  DCHECK_EQ(scan_node_->tuple_idx(), 0);
  DCHECK_EQ(batch_->row_desc().tuple_descriptors().size(), 1);
  Tuple** output_row_start =
      reinterpret_cast<Tuple**>(batch_->GetRow(batch_->num_rows()));
  Tuple** output_row_end = output_row_start + (batch_->capacity() - batch_->num_rows());
  Tuple** output_row = output_row_start;

  // Start/end/current iterators over the scratch tuples.
  uint8_t* scratch_tuple_start = scratch_batch_->CurrTuple();
  uint8_t* scratch_tuple_end = scratch_batch_->TupleEnd();
  uint8_t* scratch_tuple = scratch_tuple_start;
  const int tuple_size = scratch_batch_->tuple_byte_size;

  if (tuple_size == 0) {
    // We are materializing a collection with empty tuples. Add a NULL tuple to the
    // output batch per remaining scratch tuple and return. No need to evaluate
    // filters/conjuncts or transfer memory ownership.
    DCHECK(!has_filters);
    DCHECK(!has_conjuncts);
    DCHECK_EQ(scratch_batch_->mem_pool()->total_allocated_bytes(), 0);
    int num_tuples = min(batch_->capacity() - batch_->num_rows(),
        scratch_batch_->num_tuples - scratch_batch_->tuple_idx);
    memset(output_row, 0, num_tuples * sizeof(Tuple*));
    scratch_batch_->tuple_idx += num_tuples;
    return num_tuples;
  }

  // Loop until the scratch batch is exhausted or the output batch is full.
  // Do not use batch_->AtCapacity() in this loop because it is not necessary
  // to perform the memory capacity check.
  while (scratch_tuple != scratch_tuple_end) {
    *output_row = reinterpret_cast<Tuple*>(scratch_tuple);
    scratch_tuple += tuple_size;
    // Evaluate runtime filters and conjuncts. Short-circuit the evaluation if
    // the filters/conjuncts are empty to avoid function calls.
    if (has_filters && !EvalRuntimeFilters(reinterpret_cast<TupleRow*>(output_row))) {
      continue;
    }
    if (has_conjuncts && !ExecNode::EvalConjuncts(
        conjunct_ctxs, num_conjuncts, reinterpret_cast<TupleRow*>(output_row))) {
      continue;
    }
    // Row survived runtime filters and conjuncts.
    ++output_row;
    if (output_row == output_row_end) break;
  }
  scratch_batch_->tuple_idx += (scratch_tuple - scratch_tuple_start) / tuple_size;

  // TODO: Consider compacting the output row batch to better handle cases where
  // there are few surviving tuples per scratch batch. In such cases, we could
  // quickly accumulate memory in the output batch, hit the memory capacity limit,
  // and return an output batch with relatively few rows.
  if (scratch_batch_->AtEnd()) {
    batch_->tuple_data_pool()->AcquireData(scratch_batch_->mem_pool(), false);
  }
  return output_row - output_row_start;
}

bool HdfsParquetScanner::EvalRuntimeFilters(TupleRow* row) {
  int num_filters = filter_ctxs_.size();
  for (int i = 0; i < num_filters; ++i) {
    LocalFilterStats* stats = &filter_stats_[i];
    if (!stats->enabled) continue;
    const RuntimeFilter* filter = filter_ctxs_[i]->filter;
    // Check filter effectiveness every ROWS_PER_FILTER_SELECTIVITY_CHECK rows.
    // TODO: The stats updates and the filter effectiveness check are executed very
    // frequently. Consider hoisting it out of of this loop, and doing an equivalent
    // check less frequently, e.g., after producing an output batch.
    ++stats->total_possible;
    if (UNLIKELY(
        !(stats->total_possible & (ROWS_PER_FILTER_SELECTIVITY_CHECK - 1)))) {
      double reject_ratio = stats->rejected / static_cast<double>(stats->considered);
      if (filter->AlwaysTrue() ||
          reject_ratio < FLAGS_parquet_min_filter_reject_ratio) {
        stats->enabled = 0;
        continue;
      }
    }
    ++stats->considered;
    void* e = filter_ctxs_[i]->expr->GetValue(row);
    if (!filter->Eval<void>(e, filter_ctxs_[i]->expr->root()->type())) {
      ++stats->rejected;
      return false;
    }
  }
  return true;
}

/// High-level steps of this function:
/// 1. Allocate 'scratch' memory for tuples able to hold a full batch
/// 2. Populate the slots of all scratch tuples one column reader at a time,
///    using the ColumnReader::Read*ValueBatch() functions.
/// 3. Evaluate runtime filters and conjuncts against the scratch tuples and
///    set the surviving tuples in the output batch. Transfer the ownership of
///    scratch memory to the output batch once the scratch memory is exhausted.
/// 4. Repeat steps above until we are done with the row group or an error
///    occurred.
/// TODO: Since the scratch batch is populated in a column-wise fashion, it is
/// difficult to maintain a maximum memory footprint without throwing away at least
/// some work. This point needs further experimentation and thought.
bool HdfsParquetScanner::AssembleRows(
    const vector<ColumnReader*>& column_readers, int row_group_idx, bool* filters_pass) {
  DCHECK(!column_readers.empty());
  DCHECK(scratch_batch_ != NULL);

  int64_t rows_read = 0;
  bool continue_execution = !scan_node_->ReachedLimit() && !context_->cancelled();
  while (!column_readers[0]->RowGroupAtEnd()) {
    if (UNLIKELY(!continue_execution)) break;

    // Apply any runtime filters to static tuples containing the partition keys for this
    // partition. If any filter fails, we return immediately and stop processing this
    // row group.
    if (!scan_node_->PartitionPassesFilterPredicates(
        context_->partition_descriptor()->id(),
        FilterStats::ROW_GROUPS_KEY, context_->filter_ctxs())) {
      *filters_pass = false;
      return false;
    }

    // Start a new scratch batch.
    parse_status_.MergeStatus(scratch_batch_->Reset(state_));
    if (UNLIKELY(!parse_status_.ok())) return false;
    int scratch_capacity = scratch_batch_->capacity();

    // Initialize tuple memory.
    for (int i = 0; i < scratch_capacity; ++i) {
      InitTuple(template_tuple_, scratch_batch_->GetTuple(i));
    }

    // Materialize the top-level slots into the scratch batch column-by-column.
    int last_num_tuples = -1;
    int num_col_readers = column_readers.size();
    for (int c = 0; c < num_col_readers; ++c) {
      ColumnReader* col_reader = column_readers[c];
      if (col_reader->max_rep_level() > 0) {
        continue_execution = col_reader->ReadValueBatch(
            scratch_batch_->mem_pool(), scratch_capacity, tuple_byte_size_,
            scratch_batch_->tuple_mem, &scratch_batch_->num_tuples);
      } else {
        continue_execution = col_reader->ReadNonRepeatedValueBatch(
            scratch_batch_->mem_pool(), scratch_capacity, tuple_byte_size_,
            scratch_batch_->tuple_mem, &scratch_batch_->num_tuples);
      }
      if (UNLIKELY(!continue_execution)) return false;
      // Check that all column readers populated the same number of values.
      if (c != 0) DCHECK_EQ(last_num_tuples, scratch_batch_->num_tuples);
      last_num_tuples = scratch_batch_->num_tuples;
    }

    // Keep transferring scratch tuples to output batches until the scratch batch
    // is empty. CommitRows() creates new output batches as necessary.
    do {
      int num_row_to_commit = TransferScratchTuples();
      parse_status_.MergeStatus(CommitRows(num_row_to_commit));
      if (UNLIKELY(!parse_status_.ok())) return false;
    } while (!scratch_batch_->AtEnd());

    rows_read += scratch_batch_->num_tuples;
    COUNTER_ADD(scan_node_->rows_read_counter(), scratch_batch_->num_tuples);
    continue_execution &= parse_status_.ok();
    continue_execution &= !scan_node_->ReachedLimit() && !context_->cancelled();
  }

  if (column_readers[0]->RowGroupAtEnd() && parse_status_.ok()) {
    parse_status_ = ValidateEndOfRowGroup(column_readers, row_group_idx, rows_read);
    continue_execution &= parse_status_.ok();
  }

  return continue_execution;
}

bool HdfsParquetScanner::AssembleCollection(
    const vector<ColumnReader*>& column_readers, int new_collection_rep_level,
    CollectionValueBuilder* coll_value_builder) {
  DCHECK(!column_readers.empty());
  DCHECK_GE(new_collection_rep_level, 0);
  DCHECK(coll_value_builder != NULL);

  const TupleDescriptor* tuple_desc = &coll_value_builder->tuple_desc();
  Tuple* template_tuple = template_tuple_map_[tuple_desc];
  const vector<ExprContext*> conjunct_ctxs = scanner_conjuncts_map_[tuple_desc->id()];

  int64_t rows_read = 0;
  bool continue_execution = !scan_node_->ReachedLimit() && !context_->cancelled();
  // Note that this will be set to true at the end of the row group or the end of the
  // current collection (if applicable).
  bool end_of_collection = column_readers[0]->rep_level() == -1;
  // We only initialize end_of_collection to true here if we're at the end of the row
  // group (otherwise it would always be true because we're on the "edge" of two
  // collections), and only ProcessSplit() should call AssembleRows() at the end of the
  // row group.
  if (coll_value_builder != NULL) DCHECK(!end_of_collection);

  while (!end_of_collection && continue_execution) {
    MemPool* pool;
    Tuple* tuple;
    TupleRow* row = NULL;

    int64_t num_rows;
    // We're assembling item tuples into an CollectionValue
    parse_status_ =
        GetCollectionMemory(coll_value_builder, &pool, &tuple, &row, &num_rows);
    if (UNLIKELY(!parse_status_.ok())) {
      continue_execution = false;
      break;
    }
    // 'num_rows' can be very high if we're writing to a large CollectionValue. Limit
    // the number of rows we read at one time so we don't spend too long in the
    // 'num_rows' loop below before checking for cancellation or limit reached.
    num_rows = std::min(
        num_rows, static_cast<int64_t>(scan_node_->runtime_state()->batch_size()));

    int num_to_commit = 0;
    int row_idx = 0;
    for (row_idx = 0; row_idx < num_rows && !end_of_collection; ++row_idx) {
      DCHECK(continue_execution);
      // A tuple is produced iff the collection that contains its values is not empty and
      // non-NULL. (Empty or NULL collections produce no output values, whereas NULL is
      // output for the fields of NULL structs.)
      bool materialize_tuple = column_readers[0]->def_level() >=
          column_readers[0]->def_level_of_immediate_repeated_ancestor();
      InitTuple(tuple_desc, template_tuple, tuple);
      continue_execution =
          ReadCollectionItem(column_readers, materialize_tuple, pool, tuple);
      if (UNLIKELY(!continue_execution)) break;
      end_of_collection = column_readers[0]->rep_level() <= new_collection_rep_level;

      if (materialize_tuple) {
        if (ExecNode::EvalConjuncts(&conjunct_ctxs[0], conjunct_ctxs.size(), row)) {
          tuple = next_tuple(tuple_desc->byte_size(), tuple);
          ++num_to_commit;
        }
      }
    }

    rows_read += row_idx;
    COUNTER_ADD(scan_node_->rows_read_counter(), row_idx);
    coll_value_builder->CommitTuples(num_to_commit);
    continue_execution &= !scan_node_->ReachedLimit() && !context_->cancelled();
  }

  if (end_of_collection) {
    // All column readers should report the start of the same collection.
    for (int c = 1; c < column_readers.size(); ++c) {
      FILE_CHECK_EQ(column_readers[c]->rep_level(), column_readers[0]->rep_level());
    }
  }
  return continue_execution;
}

inline bool HdfsParquetScanner::ReadCollectionItem(
    const vector<ColumnReader*>& column_readers,
    bool materialize_tuple, MemPool* pool, Tuple* tuple) const {
  DCHECK(!column_readers.empty());
  bool continue_execution = true;
  int size = column_readers.size();
  for (int c = 0; c < size; ++c) {
    ColumnReader* col_reader = column_readers[c];
    if (materialize_tuple) {
      // All column readers for this tuple should a value to materialize.
      FILE_CHECK_GE(col_reader->def_level(),
                    col_reader->def_level_of_immediate_repeated_ancestor());
      // Fill in position slot if applicable
      if (col_reader->pos_slot_desc() != NULL) col_reader->ReadPosition(tuple);
      continue_execution = col_reader->ReadValue(pool, tuple);
    } else {
      // A containing repeated field is empty or NULL
      FILE_CHECK_LT(col_reader->def_level(),
                    col_reader->def_level_of_immediate_repeated_ancestor());
      continue_execution = col_reader->NextLevels();
    }
    if (UNLIKELY(!continue_execution)) break;
  }
  return continue_execution;
}

Status HdfsParquetScanner::ProcessFooter(bool* eosr) {
  *eosr = false;
  int64_t len = stream_->scan_range()->len();

  // We're processing the scan range issued in IssueInitialRanges(). The scan range should
  // be the last FOOTER_BYTES of the file. !success means the file is shorter than we
  // expect. Note we can't detect if the file is larger than we expect without attempting
  // to read past the end of the scan range, but in this case we'll fail below trying to
  // parse the footer.
  DCHECK_LE(len, FOOTER_SIZE);
  uint8_t* buffer;
  bool success = stream_->ReadBytes(len, &buffer, &parse_status_);
  if (!success) {
    DCHECK(!parse_status_.ok());
    if (parse_status_.code() == TErrorCode::SCANNER_INCOMPLETE_READ) {
      VLOG_QUERY << "Metadata for file '" << filename() << "' appears stale: "
                 << "metadata states file size to be "
                 << PrettyPrinter::Print(stream_->file_desc()->file_length, TUnit::BYTES)
                 << ", but could only read "
                 << PrettyPrinter::Print(stream_->total_bytes_returned(), TUnit::BYTES);
      return Status(TErrorCode::STALE_METADATA_FILE_TOO_SHORT, filename(),
          scan_node_->hdfs_table()->fully_qualified_name());
    }
    return parse_status_;
  }
  DCHECK(stream_->eosr());

  // Number of bytes in buffer after the fixed size footer is accounted for.
  int remaining_bytes_buffered = len - sizeof(int32_t) - sizeof(PARQUET_VERSION_NUMBER);

  // Make sure footer has enough bytes to contain the required information.
  if (remaining_bytes_buffered < 0) {
    return Status(Substitute("File '$0' is invalid. Missing metadata.", filename()));
  }

  // Validate magic file bytes are correct.
  uint8_t* magic_number_ptr = buffer + len - sizeof(PARQUET_VERSION_NUMBER);
  if (memcmp(magic_number_ptr, PARQUET_VERSION_NUMBER,
             sizeof(PARQUET_VERSION_NUMBER)) != 0) {
    return Status(TErrorCode::PARQUET_BAD_VERSION_NUMBER, filename(),
        string(reinterpret_cast<char*>(magic_number_ptr), sizeof(PARQUET_VERSION_NUMBER)),
        scan_node_->hdfs_table()->fully_qualified_name());
  }

  // The size of the metadata is encoded as a 4 byte little endian value before
  // the magic number
  uint8_t* metadata_size_ptr = magic_number_ptr - sizeof(int32_t);
  uint32_t metadata_size = *reinterpret_cast<uint32_t*>(metadata_size_ptr);
  uint8_t* metadata_ptr = metadata_size_ptr - metadata_size;
  // If the metadata was too big, we need to stitch it before deserializing it.
  // In that case, we stitch the data in this buffer.
  vector<uint8_t> metadata_buffer;

  DCHECK(metadata_range_ != NULL);
  if (UNLIKELY(metadata_size > remaining_bytes_buffered)) {
    // In this case, the metadata is bigger than our guess meaning there are
    // not enough bytes in the footer range from IssueInitialRanges().
    // We'll just issue more ranges to the IoMgr that is the actual footer.
    const HdfsFileDesc* file_desc = scan_node_->GetFileDesc(filename());
    DCHECK(file_desc != NULL);
    // The start of the metadata is:
    // file_length - 4-byte metadata size - footer-size - metadata size
    int64_t metadata_start = file_desc->file_length -
      sizeof(int32_t) - sizeof(PARQUET_VERSION_NUMBER) - metadata_size;
    int64_t metadata_bytes_to_read = metadata_size;
    if (metadata_start < 0) {
      return Status(Substitute("File $0 is invalid. Invalid metadata size in file "
          "footer: $1 bytes. File size: $2 bytes.", filename(), metadata_size,
          file_desc->file_length));
    }
    // IoMgr can only do a fixed size Read(). The metadata could be larger
    // so we stitch it here.
    // TODO: consider moving this stitching into the scanner context. The scanner
    // context usually handles the stitching but no other scanner need this logic
    // now.
    metadata_buffer.resize(metadata_size);
    metadata_ptr = &metadata_buffer[0];
    int64_t copy_offset = 0;
    DiskIoMgr* io_mgr = scan_node_->runtime_state()->io_mgr();

    while (metadata_bytes_to_read > 0) {
      int64_t to_read = ::min<int64_t>(io_mgr->max_read_buffer_size(),
          metadata_bytes_to_read);
      DiskIoMgr::ScanRange* range = scan_node_->AllocateScanRange(
          metadata_range_->fs(), filename(), to_read, metadata_start + copy_offset, -1,
          metadata_range_->disk_id(), metadata_range_->try_cache(),
          metadata_range_->expected_local(), file_desc->mtime);

      DiskIoMgr::BufferDescriptor* io_buffer = NULL;
      RETURN_IF_ERROR(io_mgr->Read(scan_node_->reader_context(), range, &io_buffer));
      memcpy(metadata_ptr + copy_offset, io_buffer->buffer(), io_buffer->len());
      io_buffer->Return();

      metadata_bytes_to_read -= to_read;
      copy_offset += to_read;
    }
    DCHECK_EQ(metadata_bytes_to_read, 0);
  }

  // Deserialize file header
  // TODO: this takes ~7ms for a 1000-column table, figure out how to reduce this.
  Status status =
      DeserializeThriftMsg(metadata_ptr, &metadata_size, true, &file_metadata_);
  if (!status.ok()) {
    return Status(Substitute("File $0 has invalid file metadata at file offset $1. "
        "Error = $2.", filename(),
        metadata_size + sizeof(PARQUET_VERSION_NUMBER) + sizeof(uint32_t),
        status.GetDetail()));
  }

  RETURN_IF_ERROR(ValidateFileMetadata());
  // Parse file schema
  RETURN_IF_ERROR(CreateSchemaTree(file_metadata_.schema, &schema_));

  if (scan_node_->IsZeroSlotTableScan()) {
    // There are no materialized slots, e.g. count(*) over the table.  We can serve
    // this query from just the file metadata.  We don't need to read the column data.
    int64_t num_tuples = file_metadata_.num_rows;
    COUNTER_ADD(scan_node_->rows_read_counter(), num_tuples);

    while (num_tuples > 0) {
      MemPool* pool;
      Tuple* tuple;
      TupleRow* current_row;
      int max_tuples = GetMemory(&pool, &tuple, &current_row);
      max_tuples = min<int64_t>(max_tuples, num_tuples);
      num_tuples -= max_tuples;

      int num_to_commit = WriteEmptyTuples(context_, current_row, max_tuples);
      RETURN_IF_ERROR(CommitRows(num_to_commit));
    }

    *eosr = true;
    return Status::OK();
  } else if (file_metadata_.num_rows == 0) {
    // Empty file
    *eosr = true;
    return Status::OK();
  }

  if (file_metadata_.row_groups.empty()) {
    return Status(
        Substitute("Invalid file. This file: $0 has no row groups", filename()));
  }
  if (schema_.children.empty()) {
    return Status(Substitute("Invalid file: '$0' has no columns.", filename()));
  }
  return Status::OK();
}

Status HdfsParquetScanner::ResolvePath(const SchemaPath& path, SchemaNode** node,
    bool* pos_field, bool* missing_field) {
  *missing_field = false;
  // First try two-level array encoding.
  bool missing_field_two_level;
  Status status_two_level =
      ResolvePathHelper(TWO_LEVEL, path, node, pos_field, &missing_field_two_level);
  if (missing_field_two_level) DCHECK(status_two_level.ok());
  if (status_two_level.ok() && !missing_field_two_level) return Status::OK();
  // The two-level resolution failed or reported a missing field, try three-level array
  // encoding.
  bool missing_field_three_level;
  Status status_three_level =
      ResolvePathHelper(THREE_LEVEL, path, node, pos_field, &missing_field_three_level);
  if (missing_field_three_level) DCHECK(status_three_level.ok());
  if (status_three_level.ok() && !missing_field_three_level) return Status::OK();
  // The three-level resolution failed or reported a missing field, try one-level array
  // encoding.
  bool missing_field_one_level;
  Status status_one_level =
      ResolvePathHelper(ONE_LEVEL, path, node, pos_field, &missing_field_one_level);
  if (missing_field_one_level) DCHECK(status_one_level.ok());
  if (status_one_level.ok() && !missing_field_one_level) return Status::OK();
  // None of resolutions yielded a node. Set *missing_field to true if any of the
  // resolutions reported a missing a field.
  if (missing_field_one_level || missing_field_two_level || missing_field_three_level) {
    *node = NULL;
    *missing_field = true;
    return Status::OK();
  }
  // All resolutions failed. Log and return the status from the three-level resolution
  // (which is technically the standard).
  DCHECK(!status_one_level.ok() && !status_two_level.ok() && !status_three_level.ok());
  *node = NULL;
  VLOG_QUERY << status_three_level.msg().msg() << "\n" << GetStackTrace();
  return status_three_level;
}

Status HdfsParquetScanner::ResolvePathHelper(ArrayEncoding array_encoding,
    const SchemaPath& path, SchemaNode** node, bool* pos_field, bool* missing_field) {
  DCHECK(schema_.element != NULL)
      << "schema_ must be initialized before calling ResolvePath()";

  *pos_field = false;
  *missing_field = false;
  *node = &schema_;
  const ColumnType* col_type = NULL;

  // Traverse 'path' and resolve 'node' to the corresponding SchemaNode in 'schema_' (by
  // ordinal), or set 'node' to NULL if 'path' doesn't exist in this file's schema.
  for (int i = 0; i < path.size(); ++i) {
    // Advance '*node' if necessary
    if (i == 0 || col_type->type != TYPE_ARRAY || array_encoding == THREE_LEVEL) {
      *node = NextSchemaNode(col_type, path, i, *node, missing_field);
      if (*missing_field) return Status::OK();
    } else {
      // We just resolved an array, meaning *node is set to the repeated field of the
      // array. Since we are trying to resolve using one- or two-level array encoding, the
      // repeated field represents both the array and the array's item (i.e. there is no
      // explict item field), so we don't advance *node in this case.
      DCHECK(col_type != NULL);
      DCHECK_EQ(col_type->type, TYPE_ARRAY);
      DCHECK(array_encoding == ONE_LEVEL || array_encoding == TWO_LEVEL);
      DCHECK((*node)->is_repeated());
    }

    // Advance 'col_type'
    int table_idx = path[i];
    col_type = i == 0 ? &scan_node_->hdfs_table()->col_descs()[table_idx].type()
               : &col_type->children[table_idx];

    // Resolve path[i]
    if (col_type->type == TYPE_ARRAY) {
      DCHECK_EQ(col_type->children.size(), 1);
      RETURN_IF_ERROR(
          ResolveArray(array_encoding, path, i, node, pos_field, missing_field));
      if (*missing_field || *pos_field) return Status::OK();
    } else if (col_type->type == TYPE_MAP) {
      DCHECK_EQ(col_type->children.size(), 2);
      RETURN_IF_ERROR(ResolveMap(path, i, node, missing_field));
      if (*missing_field) return Status::OK();
    } else if (col_type->type == TYPE_STRUCT) {
      DCHECK_GT(col_type->children.size(), 0);
      // Nothing to do for structs
    } else {
      DCHECK(!col_type->IsComplexType());
      DCHECK_EQ(i, path.size() - 1);
      RETURN_IF_ERROR(ValidateScalarNode(**node, *col_type, path, i));
    }
  }
  DCHECK(*node != NULL);
  return Status::OK();
}

HdfsParquetScanner::SchemaNode* HdfsParquetScanner::NextSchemaNode(
    const ColumnType* col_type, const SchemaPath& path, int next_idx, SchemaNode* node,
    bool* missing_field) {
  DCHECK_LT(next_idx, path.size());
  if (next_idx != 0) DCHECK(col_type != NULL);

  int file_idx;
  int table_idx = path[next_idx];
  bool resolve_by_name = state_->query_options().parquet_fallback_schema_resolution ==
      TParquetFallbackSchemaResolution::NAME;
  if (resolve_by_name) {
    if (next_idx == 0) {
      // Resolve top-level table column by name.
      DCHECK_LT(table_idx, scan_node_->hdfs_table()->col_descs().size());
      const string& name = scan_node_->hdfs_table()->col_descs()[table_idx].name();
      file_idx = FindChildWithName(node, name);
    } else if (col_type->type == TYPE_STRUCT) {
      // Resolve struct field by name.
      DCHECK_LT(table_idx, col_type->field_names.size());
      const string& name = col_type->field_names[table_idx];
      file_idx = FindChildWithName(node, name);
    } else if (col_type->type == TYPE_ARRAY) {
      // Arrays have only one child in the file.
      DCHECK_EQ(table_idx, SchemaPathConstants::ARRAY_ITEM);
      file_idx = table_idx;
    } else {
      DCHECK_EQ(col_type->type, TYPE_MAP);
      // Maps have two values, "key" and "value". These are supposed to be ordered and may
      // not have the right field names, but try to resolve by name in case they're
      // switched and otherwise use the order. See
      // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps for
      // more details.
      DCHECK(table_idx == SchemaPathConstants::MAP_KEY ||
             table_idx == SchemaPathConstants::MAP_VALUE);
      const string& name = table_idx == SchemaPathConstants::MAP_KEY ? "key" : "value";
      file_idx = FindChildWithName(node, name);
      if (file_idx >= node->children.size()) {
        // Couldn't resolve by name, fall back to resolution by position.
        file_idx = table_idx;
      }
    }
  } else {
    // Resolution by position.
    DCHECK_EQ(state_->query_options().parquet_fallback_schema_resolution,
        TParquetFallbackSchemaResolution::POSITION);
    if (next_idx == 0) {
      // For top-level columns, the first index in a path includes the table's partition
      // keys.
      file_idx = table_idx - scan_node_->num_partition_keys();
    } else {
      file_idx = table_idx;
    }
  }

  if (file_idx >= node->children.size()) {
    VLOG_FILE << Substitute(
        "File '$0' does not contain path '$1' (resolving by $2)", filename(),
        PrintPath(path), resolve_by_name ? "name" : "position");
    *missing_field = true;
    return NULL;
  }
  return &node->children[file_idx];
}

int HdfsParquetScanner::FindChildWithName(HdfsParquetScanner::SchemaNode* node,
    const string& name) {
  int idx;
  for (idx = 0; idx < node->children.size(); ++idx) {
    if (node->children[idx].element->name == name) break;
  }
  return idx;
}

// There are three types of array encodings:
//
// 1. One-level encoding
//      A bare repeated field. This is interpreted as a required array of required
//      items.
//    Example:
//      repeated <item-type> item;
//
// 2. Two-level encoding
//      A group containing a single repeated field. This is interpreted as a
//      <list-repetition> array of required items (<list-repetition> is either
//      optional or required).
//    Example:
//      <list-repetition> group <name> {
//        repeated <item-type> item;
//      }
//
// 3. Three-level encoding
//      The "official" encoding according to the parquet spec. A group containing a
//      single repeated group containing the item field. This is interpreted as a
//      <list-repetition> array of <item-repetition> items (<list-repetition> and
//      <item-repetition> are each either optional or required).
//    Example:
//      <list-repetition> group <name> {
//        repeated group list {
//          <item-repetition> <item-type> item;
//        }
//      }
//
// We ignore any field annotations or names, making us more permissive than the
// Parquet spec dictates. Note that in any of the encodings, <item-type> may be a
// group containing more fields, which corresponds to a complex item type. See
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists for
// more details and examples.
//
// This function resolves the array at '*node' assuming one-, two-, or three-level
// encoding, determined by 'array_encoding'. '*node' is set to the repeated field for all
// three encodings (unless '*pos_field' or '*missing_field' are set to true).
Status HdfsParquetScanner::ResolveArray(ArrayEncoding array_encoding,
    const SchemaPath& path, int idx, SchemaNode** node, bool* pos_field,
    bool* missing_field) {
  if (array_encoding == ONE_LEVEL) {
    if (!(*node)->is_repeated()) {
      ErrorMsg msg(TErrorCode::PARQUET_UNRECOGNIZED_SCHEMA, filename(),
          PrintPath(path, idx), "array", (*node)->DebugString());
      return Status::Expected(msg);
    }
  } else {
    // In the multi-level case, we always expect the outer group to contain a single
    // repeated field
    if ((*node)->children.size() != 1 || !(*node)->children[0].is_repeated()) {
      ErrorMsg msg(TErrorCode::PARQUET_UNRECOGNIZED_SCHEMA, filename(),
          PrintPath(path, idx), "array", (*node)->DebugString());
      return Status::Expected(msg);
    }
    // Set *node to the repeated field
    *node = &(*node)->children[0];
  }
  DCHECK((*node)->is_repeated());

  if (idx + 1 < path.size()) {
    if (path[idx + 1] == SchemaPathConstants::ARRAY_POS) {
      // The next index in 'path' is the artifical position field.
      DCHECK_EQ(path.size(), idx + 2) << "position field cannot have children!";
      *pos_field = true;
      *node = NULL;
      return Status::OK();
    } else {
      // The next value in 'path' should be the item index
      DCHECK_EQ(path[idx + 1], SchemaPathConstants::ARRAY_ITEM);
    }
  }
  return Status::OK();
}

// According to the parquet spec, map columns are represented like:
// <map-repetition> group <name> (MAP) {
//   repeated group key_value {
//     required <key-type> key;
//     <value-repetition> <value-type> value;
//   }
// }
// We ignore any field annotations or names, making us more permissive than the
// Parquet spec dictates. See
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps for
// more details.
Status HdfsParquetScanner::ResolveMap(const SchemaPath& path, int idx, SchemaNode** node,
    bool* missing_field) {
  if ((*node)->children.size() != 1 || !(*node)->children[0].is_repeated() ||
      (*node)->children[0].children.size() != 2) {
    ErrorMsg msg(TErrorCode::PARQUET_UNRECOGNIZED_SCHEMA, filename(),
        PrintPath(path, idx), "map", (*node)->DebugString());
    return Status::Expected(msg);
  }
  *node = &(*node)->children[0];

  // The next index in 'path' should be the key or the value.
  if (idx + 1 < path.size()) {
    DCHECK(path[idx + 1] == SchemaPathConstants::MAP_KEY ||
           path[idx + 1] == SchemaPathConstants::MAP_VALUE);
  }
  return Status::OK();
}

Status HdfsParquetScanner::ValidateScalarNode(const SchemaNode& node,
    const ColumnType& col_type, const SchemaPath& path, int idx) {
  if (!node.children.empty()) {
    ErrorMsg msg(TErrorCode::PARQUET_UNRECOGNIZED_SCHEMA, filename(),
        PrintPath(path, idx), col_type.DebugString(), node.DebugString());
    return Status::Expected(msg);
  }
  parquet::Type::type type = IMPALA_TO_PARQUET_TYPES[col_type.type];
  if (type != node.element->type) {
    ErrorMsg msg(TErrorCode::PARQUET_UNRECOGNIZED_SCHEMA, filename(),
        PrintPath(path, idx), col_type.DebugString(), node.DebugString());
    return Status::Expected(msg);
  }
  return Status::OK();
}

Status HdfsParquetScanner::CreateColumnReaders(const TupleDescriptor& tuple_desc,
    vector<ColumnReader*>* column_readers) {
  DCHECK(column_readers != NULL);
  DCHECK(column_readers->empty());

  // Each tuple can have at most one position slot. We'll process this slot desc last.
  SlotDescriptor* pos_slot_desc = NULL;

  for (SlotDescriptor* slot_desc: tuple_desc.slots()) {
    // Skip partition columns
    if (&tuple_desc == scan_node_->tuple_desc() &&
        slot_desc->col_pos() < scan_node_->num_partition_keys()) continue;

    SchemaNode* node = NULL;
    bool pos_field;
    bool missing_field;
    RETURN_IF_ERROR(
        ResolvePath(slot_desc->col_path(), &node, &pos_field, &missing_field));

    if (missing_field) {
      // In this case, we are selecting a column that is not in the file.
      // Update the template tuple to put a NULL in this slot.
      Tuple** template_tuple = &template_tuple_map_[&tuple_desc];
      if (*template_tuple == NULL) {
        *template_tuple = scan_node_->InitEmptyTemplateTuple(tuple_desc);
      }
      (*template_tuple)->SetNull(slot_desc->null_indicator_offset());
      continue;
    }

    if (pos_field) {
      DCHECK(pos_slot_desc == NULL) << "There should only be one position slot per tuple";
      pos_slot_desc = slot_desc;
      continue;
    }

    ColumnReader* col_reader =
        CreateReader(*node, slot_desc->type().IsCollectionType(), slot_desc);
    column_readers->push_back(col_reader);

    if (col_reader->IsCollectionReader()) {
      // Recursively populate col_reader's children
      DCHECK(slot_desc->collection_item_descriptor() != NULL);
      const TupleDescriptor* item_tuple_desc = slot_desc->collection_item_descriptor();
      CollectionColumnReader* collection_reader =
          static_cast<CollectionColumnReader*>(col_reader);
      RETURN_IF_ERROR(CreateColumnReaders(
          *item_tuple_desc, collection_reader->children()));
    }
  }

  if (column_readers->empty()) {
    // This is either a count(*) over a collection type (count(*) over the table is
    // handled in ProcessFooter()), or no materialized columns appear in this file
    // (e.g. due to schema evolution, or if there's only a position slot). Create a single
    // column reader that we will use to count the number of tuples we should output. We
    // will not read any values from this reader.
    ColumnReader* reader;
    RETURN_IF_ERROR(CreateCountingReader(tuple_desc.tuple_path(), &reader));
    column_readers->push_back(reader);
  }

  if (pos_slot_desc != NULL) {
    // 'tuple_desc' has a position slot. Use an existing column reader to populate it.
    DCHECK(!column_readers->empty());
    (*column_readers)[0]->set_pos_slot_desc(pos_slot_desc);
  }

  return Status::OK();
}

Status HdfsParquetScanner::CreateCountingReader(
    const SchemaPath& parent_path, HdfsParquetScanner::ColumnReader** reader) {
  SchemaNode* parent_node;
  bool pos_field;
  bool missing_field;
  RETURN_IF_ERROR(ResolvePath(parent_path, &parent_node, &pos_field, &missing_field));

  if (missing_field) {
    // TODO: can we do anything else here?
    return Status(Substitute(
        "Could not find '$0' in file.", PrintPath(parent_path), filename()));
  }
  DCHECK(!pos_field);
  DCHECK(parent_path.empty() || parent_node->is_repeated());

  if (!parent_node->children.empty()) {
    // Find a non-struct (i.e. collection or scalar) child of 'parent_node', which we will
    // use to create the item reader
    const SchemaNode* target_node = &parent_node->children[0];
    while (!target_node->children.empty() && !target_node->is_repeated()) {
      target_node = &target_node->children[0];
    }

    *reader = CreateReader(*target_node, target_node->is_repeated(), NULL);
    if (target_node->is_repeated()) {
      // Find the closest scalar descendent of 'target_node' via breadth-first search, and
      // create scalar reader to drive 'reader'. We find the closest (i.e. least-nested)
      // descendent as a heuristic for picking a descendent with fewer values, so it's
      // faster to scan.
      // TODO: use different heuristic than least-nested? Fewest values?
      const SchemaNode* node = NULL;
      queue<const SchemaNode*> nodes;
      nodes.push(target_node);
      while (!nodes.empty()) {
        node = nodes.front();
        nodes.pop();
        if (node->children.size() > 0) {
          for (const SchemaNode& child: node->children) nodes.push(&child);
        } else {
          // node is the least-nested scalar descendent of 'target_node'
          break;
        }
      }
      DCHECK(node->children.empty()) << node->DebugString();
      CollectionColumnReader* parent_reader = static_cast<CollectionColumnReader*>(*reader);
      parent_reader->children()->push_back(CreateReader(*node, false, NULL));
    }
  } else {
    // Special case for a repeated scalar node. The repeated node represents both the
    // parent collection and the child item.
    *reader = CreateReader(*parent_node, false, NULL);
  }

  return Status::OK();
}

Status HdfsParquetScanner::InitColumns(
    int row_group_idx, const vector<ColumnReader*>& column_readers) {
  const HdfsFileDesc* file_desc = scan_node_->GetFileDesc(filename());
  DCHECK(file_desc != NULL);
  parquet::RowGroup& row_group = file_metadata_.row_groups[row_group_idx];

  // All the scan ranges (one for each column).
  vector<DiskIoMgr::ScanRange*> col_ranges;
  // Used to validate that the number of values in each reader in column_readers_ is the
  // same.
  int num_values = -1;
  // Used to validate we issued the right number of scan ranges
  int num_scalar_readers = 0;

  for (ColumnReader* col_reader: column_readers) {
    if (col_reader->IsCollectionReader()) {
      CollectionColumnReader* collection_reader =
          static_cast<CollectionColumnReader*>(col_reader);
      collection_reader->Reset();
      // Recursively init child readers
      RETURN_IF_ERROR(InitColumns(row_group_idx, *collection_reader->children()));
      continue;
    }
    ++num_scalar_readers;

    BaseScalarColumnReader* scalar_reader =
        static_cast<BaseScalarColumnReader*>(col_reader);
    const parquet::ColumnChunk& col_chunk = row_group.columns[scalar_reader->col_idx()];
    int64_t col_start = col_chunk.meta_data.data_page_offset;

    if (num_values == -1) {
      num_values = col_chunk.meta_data.num_values;
    } else if (col_chunk.meta_data.num_values != num_values) {
      // TODO for 2.3: improve this error message by saying which columns are different,
      // and also specify column in other error messages as appropriate
      return Status(TErrorCode::PARQUET_NUM_COL_VALS_ERROR, scalar_reader->col_idx(),
          col_chunk.meta_data.num_values, num_values, filename());
    }

    RETURN_IF_ERROR(ValidateColumn(*scalar_reader, row_group_idx));

    if (col_chunk.meta_data.__isset.dictionary_page_offset) {
      // Already validated in ValidateColumnOffsets()
      DCHECK_LT(col_chunk.meta_data.dictionary_page_offset, col_start);
      col_start = col_chunk.meta_data.dictionary_page_offset;
    }
    int64_t col_len = col_chunk.meta_data.total_compressed_size;
    if (col_len <= 0) {
      return Status(Substitute("File '$0' contains invalid column chunk size: $1",
          filename(), col_len));
    }
    int64_t col_end = col_start + col_len;

    // Already validated in ValidateColumnOffsets()
    DCHECK(col_end > 0 && col_end < file_desc->file_length);
    if (file_version_.application == "parquet-mr" && file_version_.VersionLt(1, 2, 9)) {
      // The Parquet MR writer had a bug in 1.2.8 and below where it didn't include the
      // dictionary page header size in total_compressed_size and total_uncompressed_size
      // (see IMPALA-694). We pad col_len to compensate.
      int64_t bytes_remaining = file_desc->file_length - col_end;
      int64_t pad = min<int64_t>(MAX_DICT_HEADER_SIZE, bytes_remaining);
      col_len += pad;
    }

    // TODO: this will need to change when we have co-located files and the columns
    // are different files.
    if (!col_chunk.file_path.empty() && col_chunk.file_path != filename()) {
      return Status(Substitute("Expected parquet column file path '$0' to match "
          "filename '$1'", col_chunk.file_path, filename()));
    }

    const DiskIoMgr::ScanRange* split_range =
        reinterpret_cast<ScanRangeMetadata*>(metadata_range_->meta_data())->original_split;

    // Determine if the column is completely contained within a local split.
    bool column_range_local = split_range->expected_local() &&
                              col_start >= split_range->offset() &&
                              col_end <= split_range->offset() + split_range->len();

    DiskIoMgr::ScanRange* col_range = scan_node_->AllocateScanRange(
        metadata_range_->fs(), filename(), col_len, col_start, scalar_reader->col_idx(),
        split_range->disk_id(), split_range->try_cache(), column_range_local,
        file_desc->mtime);
    col_ranges.push_back(col_range);

    // Get the stream that will be used for this column
    ScannerContext::Stream* stream = context_->AddStream(col_range);
    DCHECK(stream != NULL);

    RETURN_IF_ERROR(scalar_reader->Reset(&col_chunk.meta_data, stream));

    const SlotDescriptor* slot_desc = scalar_reader->slot_desc();
    if (slot_desc == NULL || !slot_desc->type().IsStringType() ||
        col_chunk.meta_data.codec != parquet::CompressionCodec::UNCOMPRESSED) {
      // Non-string types are always compact.  Compressed columns don't reference data in
      // the io buffers after tuple materialization.  In both cases, we can set compact to
      // true and recycle buffers more promptly.
      stream->set_contains_tuple_data(false);
    }
  }
  DCHECK_EQ(col_ranges.size(), num_scalar_readers);

  // Issue all the column chunks to the io mgr and have them scheduled immediately.
  // This means these ranges aren't returned via DiskIoMgr::GetNextRange and
  // instead are scheduled to be read immediately.
  RETURN_IF_ERROR(scan_node_->runtime_state()->io_mgr()->AddScanRanges(
      scan_node_->reader_context(), col_ranges, true));

  return Status::OK();
}

Status HdfsParquetScanner::CreateSchemaTree(const vector<parquet::SchemaElement>& schema,
    HdfsParquetScanner::SchemaNode* node) const {
  int idx = 0;
  int col_idx = 0;
  return CreateSchemaTree(schema, 0, 0, 0, &idx, &col_idx, node);
}

Status HdfsParquetScanner::CreateSchemaTree(
    const vector<parquet::SchemaElement>& schema, int max_def_level, int max_rep_level,
    int ira_def_level, int* idx, int* col_idx, HdfsParquetScanner::SchemaNode* node)
    const {
  if (*idx >= schema.size()) {
    return Status(Substitute("File $0 corrupt: could not reconstruct schema tree from "
            "flattened schema in file metadata", filename()));
  }
  node->element = &schema[*idx];
  ++(*idx);

  if (node->element->num_children == 0) {
    // node is a leaf node, meaning it's materialized in the file and appears in
    // file_metadata_.row_groups.columns
    node->col_idx = *col_idx;
    ++(*col_idx);
  }

  // def_level_of_immediate_repeated_ancestor does not include this node, so set before
  // updating ira_def_level
  node->def_level_of_immediate_repeated_ancestor = ira_def_level;

  if (node->element->repetition_type == parquet::FieldRepetitionType::OPTIONAL) {
    ++max_def_level;
  } else if (node->element->repetition_type == parquet::FieldRepetitionType::REPEATED) {
    ++max_rep_level;
    // Repeated fields add a definition level. This is used to distinguish between an
    // empty list and a list with an item in it.
    ++max_def_level;
    // node is the new most immediate repeated ancestor
    ira_def_level = max_def_level;
  }
  node->max_def_level = max_def_level;
  node->max_rep_level = max_rep_level;

  node->children.resize(node->element->num_children);
  for (int i = 0; i < node->element->num_children; ++i) {
    RETURN_IF_ERROR(CreateSchemaTree(schema, max_def_level, max_rep_level, ira_def_level,
        idx, col_idx, &node->children[i]));
  }
  return Status::OK();
}

HdfsParquetScanner::FileVersion::FileVersion(const string& created_by) {
  string created_by_lower = created_by;
  std::transform(created_by_lower.begin(), created_by_lower.end(),
      created_by_lower.begin(), ::tolower);
  is_impala_internal = false;

  vector<string> tokens;
  split(tokens, created_by_lower, is_any_of(" "), token_compress_on);
  // Boost always creates at least one token
  DCHECK_GT(tokens.size(), 0);
  application = tokens[0];

  if (tokens.size() >= 3 && tokens[1] == "version") {
    string version_string = tokens[2];
    // Ignore any trailing nodextra characters
    int n = version_string.find_first_not_of("0123456789.");
    string version_string_trimmed = version_string.substr(0, n);

    vector<string> version_tokens;
    split(version_tokens, version_string_trimmed, is_any_of("."));
    version.major = version_tokens.size() >= 1 ? atoi(version_tokens[0].c_str()) : 0;
    version.minor = version_tokens.size() >= 2 ? atoi(version_tokens[1].c_str()) : 0;
    version.patch = version_tokens.size() >= 3 ? atoi(version_tokens[2].c_str()) : 0;

    if (application == "impala") {
      if (version_string.find("-internal") != string::npos) is_impala_internal = true;
    }
  } else {
    version.major = 0;
    version.minor = 0;
    version.patch = 0;
  }
}

bool HdfsParquetScanner::FileVersion::VersionLt(int major, int minor, int patch) const {
  if (version.major < major) return true;
  if (version.major > major) return false;
  DCHECK_EQ(version.major, major);
  if (version.minor < minor) return true;
  if (version.minor > minor) return false;
  DCHECK_EQ(version.minor, minor);
  return version.patch < patch;
}

bool HdfsParquetScanner::FileVersion::VersionEq(int major, int minor, int patch) const {
  return version.major == major && version.minor == minor && version.patch == patch;
}

Status HdfsParquetScanner::ValidateFileMetadata() {
  if (file_metadata_.version > PARQUET_CURRENT_VERSION) {
    stringstream ss;
    ss << "File: " << filename() << " is of an unsupported version. "
       << "file version: " << file_metadata_.version;
    return Status(ss.str());
  }

  // Parse out the created by application version string
  if (file_metadata_.__isset.created_by) {
    file_version_ = FileVersion(file_metadata_.created_by);
  }
  return Status::OK();
}

bool IsEncodingSupported(parquet::Encoding::type e) {
  switch (e) {
    case parquet::Encoding::PLAIN:
    case parquet::Encoding::PLAIN_DICTIONARY:
    case parquet::Encoding::BIT_PACKED:
    case parquet::Encoding::RLE:
      return true;
    default:
      return false;
  }
}

Status HdfsParquetScanner::ValidateColumn(
    const BaseScalarColumnReader& col_reader, int row_group_idx) {
  int col_idx = col_reader.col_idx();
  const parquet::SchemaElement& schema_element = col_reader.schema_element();
  parquet::ColumnChunk& file_data =
      file_metadata_.row_groups[row_group_idx].columns[col_idx];

  // Check the encodings are supported.
  vector<parquet::Encoding::type>& encodings = file_data.meta_data.encodings;
  for (int i = 0; i < encodings.size(); ++i) {
    if (!IsEncodingSupported(encodings[i])) {
      stringstream ss;
      ss << "File '" << filename() << "' uses an unsupported encoding: "
         << PrintEncoding(encodings[i]) << " for column '" << schema_element.name
         << "'.";
      return Status(ss.str());
    }
  }

  // Check the compression is supported.
  if (file_data.meta_data.codec != parquet::CompressionCodec::UNCOMPRESSED &&
      file_data.meta_data.codec != parquet::CompressionCodec::SNAPPY &&
      file_data.meta_data.codec != parquet::CompressionCodec::GZIP) {
    stringstream ss;
    ss << "File '" << filename() << "' uses an unsupported compression: "
        << file_data.meta_data.codec << " for column '" << schema_element.name
        << "'.";
    return Status(ss.str());
  }

  // Validation after this point is only if col_reader is reading values.
  const SlotDescriptor* slot_desc = col_reader.slot_desc();
  if (slot_desc == NULL) return Status::OK();

  parquet::Type::type type = IMPALA_TO_PARQUET_TYPES[slot_desc->type().type];
  DCHECK_EQ(type, file_data.meta_data.type)
      << "Should have been validated in ResolvePath()";

  // Check the decimal scale in the file matches the metastore scale and precision.
  // We fail the query if the metadata makes it impossible for us to safely read
  // the file. If we don't require the metadata, we will fail the query if
  // abort_on_error is true, otherwise we will just log a warning.
  bool is_converted_type_decimal = schema_element.__isset.converted_type &&
      schema_element.converted_type == parquet::ConvertedType::DECIMAL;
  if (slot_desc->type().type == TYPE_DECIMAL) {
    // We require that the scale and byte length be set.
    if (schema_element.type != parquet::Type::FIXED_LEN_BYTE_ARRAY) {
      stringstream ss;
      ss << "File '" << filename() << "' column '" << schema_element.name
         << "' should be a decimal column encoded using FIXED_LEN_BYTE_ARRAY.";
      return Status(ss.str());
    }

    if (!schema_element.__isset.type_length) {
      stringstream ss;
      ss << "File '" << filename() << "' column '" << schema_element.name
         << "' does not have type_length set.";
      return Status(ss.str());
    }

    int expected_len = ParquetPlainEncoder::DecimalSize(slot_desc->type());
    if (schema_element.type_length != expected_len) {
      stringstream ss;
      ss << "File '" << filename() << "' column '" << schema_element.name
         << "' has an invalid type length. Expecting: " << expected_len
         << " len in file: " << schema_element.type_length;
      return Status(ss.str());
    }

    if (!schema_element.__isset.scale) {
      stringstream ss;
      ss << "File '" << filename() << "' column '" << schema_element.name
         << "' does not have the scale set.";
      return Status(ss.str());
    }

    if (schema_element.scale != slot_desc->type().scale) {
      // TODO: we could allow a mismatch and do a conversion at this step.
      stringstream ss;
      ss << "File '" << filename() << "' column '" << schema_element.name
         << "' has a scale that does not match the table metadata scale."
         << " File metadata scale: " << schema_element.scale
         << " Table metadata scale: " << slot_desc->type().scale;
      return Status(ss.str());
    }

    // The other decimal metadata should be there but we don't need it.
    if (!schema_element.__isset.precision) {
      ErrorMsg msg(TErrorCode::PARQUET_MISSING_PRECISION, filename(),
          schema_element.name);
      RETURN_IF_ERROR(LogOrReturnError(msg));
    } else {
      if (schema_element.precision != slot_desc->type().precision) {
        // TODO: we could allow a mismatch and do a conversion at this step.
        ErrorMsg msg(TErrorCode::PARQUET_WRONG_PRECISION, filename(), schema_element.name,
            schema_element.precision, slot_desc->type().precision);
        RETURN_IF_ERROR(LogOrReturnError(msg));
      }
    }

    if (!is_converted_type_decimal) {
      // TODO: is this validation useful? It is not required at all to read the data and
      // might only serve to reject otherwise perfectly readable files.
      ErrorMsg msg(TErrorCode::PARQUET_BAD_CONVERTED_TYPE, filename(),
          schema_element.name);
      RETURN_IF_ERROR(LogOrReturnError(msg));
    }
  } else if (schema_element.__isset.scale || schema_element.__isset.precision ||
      is_converted_type_decimal) {
    ErrorMsg msg(TErrorCode::PARQUET_INCOMPATIBLE_DECIMAL, filename(),
        schema_element.name, slot_desc->type().DebugString());
    RETURN_IF_ERROR(LogOrReturnError(msg));
  }
  return Status::OK();
}

Status HdfsParquetScanner::ValidateEndOfRowGroup(
    const vector<ColumnReader*>& column_readers, int row_group_idx, int64_t rows_read) {
  DCHECK(!column_readers.empty());
  DCHECK(parse_status_.ok()) << "Don't overwrite parse_status_"
      << parse_status_.GetDetail();

  if (column_readers[0]->max_rep_level() == 0) {
    // These column readers materialize table-level values (vs. collection values). Test
    // if the expected number of rows from the file metadata matches the actual number of
    // rows read from the file.
    int64_t expected_rows_in_group = file_metadata_.row_groups[row_group_idx].num_rows;
    if (rows_read != expected_rows_in_group) {
      return Status(TErrorCode::PARQUET_GROUP_ROW_COUNT_ERROR, filename(), row_group_idx,
          expected_rows_in_group, rows_read);
    }
  }

  // Validate scalar column readers' state
  int num_values_read = -1;
  for (int c = 0; c < column_readers.size(); ++c) {
    if (column_readers[c]->IsCollectionReader()) continue;
    BaseScalarColumnReader* reader =
        static_cast<BaseScalarColumnReader*>(column_readers[c]);
    // All readers should have exhausted the final data page. This could fail if one
    // column has more values than stated in the metadata, meaning the final data page
    // will still have unread values.
    if (reader->num_buffered_values_ != 0) {
      return Status(Substitute("Corrupt parquet metadata in file '$0': metadata reports "
          "'$1' more values in data page than actually present", filename(),
          reader->num_buffered_values_));
    }
    // Sanity check that the num_values_read_ value is the same for all readers. All
    // readers should have been advanced in lockstep (the above check is more likely to
    // fail if this not the case though, since num_values_read_ is only updated at the end
    // of a data page).
    if (num_values_read == -1) num_values_read = reader->num_values_read_;
    DCHECK_EQ(reader->num_values_read_, num_values_read);
    // ReadDataPage() uses metadata_->num_values to determine when the column's done
    DCHECK(reader->num_values_read_ == reader->metadata_->num_values ||
        !state_->abort_on_error());
  }
  return Status::OK();
}

string PrintRepetitionType(const parquet::FieldRepetitionType::type& t) {
  switch (t) {
    case parquet::FieldRepetitionType::REQUIRED: return "required";
    case parquet::FieldRepetitionType::OPTIONAL: return "optional";
    case parquet::FieldRepetitionType::REPEATED: return "repeated";
    default: return "<unknown>";
  }
}

string PrintParquetType(const parquet::Type::type& t) {
  switch (t) {
    case parquet::Type::BOOLEAN: return "boolean";
    case parquet::Type::INT32: return "int32";
    case parquet::Type::INT64: return "int64";
    case parquet::Type::INT96: return "int96";
    case parquet::Type::FLOAT: return "float";
    case parquet::Type::DOUBLE: return "double";
    case parquet::Type::BYTE_ARRAY: return "byte_array";
    case parquet::Type::FIXED_LEN_BYTE_ARRAY: return "fixed_len_byte_array";
    default: return "<unknown>";
  }
}

string HdfsParquetScanner::SchemaNode::DebugString(int indent) const {
  stringstream ss;
  for (int i = 0; i < indent; ++i) ss << " ";
  ss << PrintRepetitionType(element->repetition_type) << " ";
  if (element->num_children > 0) {
    ss << "struct";
  } else {
    ss << PrintParquetType(element->type);
  }
  ss << " " << element->name << " [i:" << col_idx << " d:" << max_def_level
     << " r:" << max_rep_level << "]";
  if (element->num_children > 0) {
    ss << " {" << endl;
    for (int i = 0; i < element->num_children; ++i) {
      ss << children[i].DebugString(indent + 2) << endl;
    }
    for (int i = 0; i < indent; ++i) ss << " ";
    ss << "}";
  }
  return ss.str();
}
