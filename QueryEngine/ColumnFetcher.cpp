/*
 * Copyright 2017 MapD Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "QueryEngine/ColumnFetcher.h"

#include <memory>

#include "QueryEngine/ErrorHandling.h"
#include "QueryEngine/Execute.h"

namespace {

inline const ColumnarResults* columnarize_result(
    std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner,
    const ResultSetPtr& result,
    const size_t thread_idx,
    const int frag_id) {
  INJECT_TIMER(columnarize_result);
  CHECK_EQ(0, frag_id);

  std::vector<SQLTypeInfo> col_types;
  for (size_t i = 0; i < result->colCount(); ++i) {
    col_types.push_back(get_logical_type_info(result->getColType(i)));
  }
  return new ColumnarResults(
      row_set_mem_owner, *result, result->colCount(), col_types, thread_idx);
}

}  // namespace

ColumnFetcher::ColumnFetcher(Executor* executor, const ColumnCacheMap& column_cache)
    : executor_(executor), columnarized_table_cache_(column_cache) {}

//! Gets a column fragment chunk on CPU or on GPU depending on the effective
//! memory level parameter. For temporary tables, the chunk will be copied to
//! the GPU if needed. Returns a buffer pointer and an element count.
std::pair<const int8_t*, size_t> ColumnFetcher::getOneColumnFragment(
    Executor* executor,
    const Analyzer::ColumnVar& hash_col,
    const Fragmenter_Namespace::FragmentInfo& fragment,
    const Data_Namespace::MemoryLevel effective_mem_lvl,
    const int device_id,
    DeviceAllocator* device_allocator,
    const size_t thread_idx,
    std::vector<std::shared_ptr<Chunk_NS::Chunk>>& chunks_owner,
    ColumnCacheMap& column_cache) {
  static std::mutex columnar_conversion_mutex;
  auto timer = DEBUG_TIMER(__func__);
  if (fragment.isEmptyPhysicalFragment()) {
    return {nullptr, 0};
  }
  const auto table_id = hash_col.get_table_id();
  const auto& catalog = *executor->getCatalog();
  const auto cd =
      get_column_descriptor_maybe(hash_col.get_column_id(), table_id, catalog);
  CHECK(!cd || !(cd->isVirtualCol));
  const int8_t* col_buff = nullptr;
  if (cd) {  // real table
    /* chunk_meta_it is used here to retrieve chunk numBytes and
       numElements. Apparently, their values are often zeros. If we
       knew how to predict the zero values, calling
       getChunkMetadataMap could be avoided to skip
       synthesize_metadata calls. */
    auto chunk_meta_it = fragment.getChunkMetadataMap().find(hash_col.get_column_id());
    CHECK(chunk_meta_it != fragment.getChunkMetadataMap().end());
    ChunkKey chunk_key{catalog.getCurrentDB().dbId,
                       fragment.physicalTableId,
                       hash_col.get_column_id(),
                       fragment.fragmentId};
    const auto chunk = Chunk_NS::Chunk::getChunk(
        cd,
        &catalog.getDataMgr(),
        chunk_key,
        effective_mem_lvl,
        effective_mem_lvl == Data_Namespace::CPU_LEVEL ? 0 : device_id,
        chunk_meta_it->second->numBytes,
        chunk_meta_it->second->numElements);
    chunks_owner.push_back(chunk);
    CHECK(chunk);
    auto ab = chunk->getBuffer();
    CHECK(ab->getMemoryPtr());
    col_buff = reinterpret_cast<int8_t*>(ab->getMemoryPtr());
  } else {  // temporary table
    const ColumnarResults* col_frag{nullptr};
    {
      std::lock_guard<std::mutex> columnar_conversion_guard(columnar_conversion_mutex);
      const auto frag_id = fragment.fragmentId;
      if (column_cache.empty() || !column_cache.count(table_id)) {
        column_cache.insert(std::make_pair(
            table_id, std::unordered_map<int, std::shared_ptr<const ColumnarResults>>()));
      }
      auto& frag_id_to_result = column_cache[table_id];
      if (frag_id_to_result.empty() || !frag_id_to_result.count(frag_id)) {
        frag_id_to_result.insert(
            std::make_pair(frag_id,
                           std::shared_ptr<const ColumnarResults>(columnarize_result(
                               executor->row_set_mem_owner_,
                               get_temporary_table(executor->temporary_tables_, table_id),
                               thread_idx,
                               frag_id))));
      }
      col_frag = column_cache[table_id][frag_id].get();
    }
    col_buff = transferColumnIfNeeded(
        col_frag,
        hash_col.get_column_id(),
        &catalog.getDataMgr(),
        effective_mem_lvl,
        effective_mem_lvl == Data_Namespace::CPU_LEVEL ? 0 : device_id,
        device_allocator);
  }
  return {col_buff, fragment.getNumTuples()};
}

//! makeJoinColumn() creates a JoinColumn struct containing a array of
//! JoinChunk structs, col_chunks_buff, malloced in CPU memory. Although
//! the col_chunks_buff array is in CPU memory here, each JoinChunk struct
//! contains an int8_t* pointer from getOneColumnFragment(), col_buff,
//! that can point to either CPU memory or GPU memory depending on the
//! effective_mem_lvl parameter. See also the fetchJoinColumn() function
//! where col_chunks_buff is copied into GPU memory if needed. The
//! malloc_owner parameter will have the malloced array appended. The
//! chunks_owner parameter will be appended with the chunks.
JoinColumn ColumnFetcher::makeJoinColumn(
    Executor* executor,
    const Analyzer::ColumnVar& hash_col,
    const std::vector<Fragmenter_Namespace::FragmentInfo>& fragments,
    const Data_Namespace::MemoryLevel effective_mem_lvl,
    const int device_id,
    DeviceAllocator* device_allocator,
    const size_t thread_idx,
    std::vector<std::shared_ptr<Chunk_NS::Chunk>>& chunks_owner,
    std::vector<std::shared_ptr<void>>& malloc_owner,
    ColumnCacheMap& column_cache) {
  CHECK(!fragments.empty());

  size_t col_chunks_buff_sz = sizeof(struct JoinChunk) * fragments.size();
  // TODO: needs an allocator owner
  auto col_chunks_buff = reinterpret_cast<int8_t*>(
      malloc_owner.emplace_back(checked_malloc(col_chunks_buff_sz), free).get());
  auto join_chunk_array = reinterpret_cast<struct JoinChunk*>(col_chunks_buff);

  size_t num_elems = 0;
  size_t num_chunks = 0;
  for (auto& frag : fragments) {
    if (g_enable_non_kernel_time_query_interrupt && check_interrupt()) {
      throw QueryExecutionError(Executor::ERR_INTERRUPTED);
    }
    auto [col_buff, elem_count] = getOneColumnFragment(
        executor,
        hash_col,
        frag,
        effective_mem_lvl,
        effective_mem_lvl == Data_Namespace::CPU_LEVEL ? 0 : device_id,
        device_allocator,
        thread_idx,
        chunks_owner,
        column_cache);
    if (col_buff != nullptr) {
      num_elems += elem_count;
      join_chunk_array[num_chunks] = JoinChunk{col_buff, elem_count};
    } else {
      continue;
    }
    ++num_chunks;
  }

  int elem_sz = hash_col.get_type_info().get_size();
  CHECK_GT(elem_sz, 0);

  return {col_chunks_buff,
          col_chunks_buff_sz,
          num_chunks,
          num_elems,
          static_cast<size_t>(elem_sz)};
}

const int8_t* ColumnFetcher::getOneTableColumnFragment(
    const int table_id,
    const int frag_id,
    const int col_id,
    const std::map<int, const TableFragments*>& all_tables_fragments,
    std::list<std::shared_ptr<Chunk_NS::Chunk>>& chunk_holder,
    std::list<ChunkIter>& chunk_iter_holder,
    const Data_Namespace::MemoryLevel memory_level,
    const int device_id,
    DeviceAllocator* allocator) const {
  // TODO: refactor
  const auto& cat = *executor_->getCatalog();

  VLOG(2) << "DEBUG LOG we are fetching column table: " << table_id
          << " frag: " << frag_id << " col:" << col_id;
  Data_Namespace::DataMgr* dataMgr = &cat.getDataMgr();
  auto data_provider = dataMgr->getDataProvider();
  int real_table_id = table_id;
  int real_frag_id = frag_id;
  if (data_provider) {
    // int32_t my_table_id = data_provider->getTableId();
    // int32_t my_fragment_id = data_provider->getFragmentId();
    // VLOG(2) << "DataProvider table: " << my_table_id << " fragment: " << my_fragment_id;
    real_table_id = data_provider->getTableId();
    real_frag_id = data_provider->getFragmentId();
  }

  static std::mutex varlen_chunk_mutex;  // TODO(alex): remove
  static std::mutex chunk_list_mutex;
  const auto fragments_it = all_tables_fragments.find(real_table_id);
  CHECK(fragments_it != all_tables_fragments.end());
  const auto fragments = fragments_it->second;
  const auto& fragment = (*fragments)[real_frag_id];
  if (fragment.isEmptyPhysicalFragment()) {
    return nullptr;
  }
  std::shared_ptr<Chunk_NS::Chunk> chunk;
  auto chunk_meta_it = fragment.getChunkMetadataMap().find(col_id);
  CHECK(chunk_meta_it != fragment.getChunkMetadataMap().end());
  CHECK(real_table_id > 0);
  auto cd = get_column_descriptor(col_id, real_table_id, cat);
  CHECK(cd);
  const auto col_type =
      get_column_type(col_id, real_table_id, cd, executor_->temporary_tables_);
  const bool is_real_string =
      col_type.is_string() && col_type.get_compression() == kENCODING_NONE;
  const bool is_varlen =
      is_real_string ||
      col_type.is_array();  // TODO: should it be col_type.is_varlen_array() ?
  {
    ChunkKey chunk_key{
        cat.getCurrentDB().dbId, fragment.physicalTableId, col_id, fragment.fragmentId};
    std::unique_ptr<std::lock_guard<std::mutex>> varlen_chunk_lock;
    if (is_varlen) {
      varlen_chunk_lock.reset(new std::lock_guard<std::mutex>(varlen_chunk_mutex));
    }
    chunk = Chunk_NS::Chunk::getChunk(
        cd,
        &cat.getDataMgr(),
        chunk_key,
        memory_level,
        memory_level == Data_Namespace::CPU_LEVEL ? 0 : device_id,
        chunk_meta_it->second->numBytes,
        chunk_meta_it->second->numElements);
    std::lock_guard<std::mutex> chunk_list_lock(chunk_list_mutex);
    chunk_holder.push_back(chunk);
  }
  if (is_varlen) {
    CHECK_GT(real_table_id, 0);
    CHECK(chunk_meta_it != fragment.getChunkMetadataMap().end());
    chunk_iter_holder.push_back(chunk->begin_iterator(chunk_meta_it->second));
    auto& chunk_iter = chunk_iter_holder.back();
    if (memory_level == Data_Namespace::CPU_LEVEL) {
      return reinterpret_cast<int8_t*>(&chunk_iter);
    } else {
      auto ab = chunk->getBuffer();
      ab->pin();
      auto& row_set_mem_owner = executor_->getRowSetMemoryOwner();
      row_set_mem_owner->addVarlenInputBuffer(ab);
      CHECK_EQ(Data_Namespace::GPU_LEVEL, memory_level);
      CHECK(allocator);
      auto chunk_iter_gpu = allocator->alloc(sizeof(ChunkIter));
      allocator->copyToDevice(
          chunk_iter_gpu, reinterpret_cast<int8_t*>(&chunk_iter), sizeof(ChunkIter));
      return chunk_iter_gpu;
    }
  } else {
    auto ab = chunk->getBuffer();
    CHECK(ab->getMemoryPtr());
    return ab->getMemoryPtr();  // @TODO(alex) change to use ChunkIter
  }
}

const int8_t* ColumnFetcher::getAllTableColumnFragments(
    const int table_id,
    const int col_id,
    const std::map<int, const TableFragments*>& all_tables_fragments,
    const Data_Namespace::MemoryLevel memory_level,
    const int device_id,
    DeviceAllocator* device_allocator,
    const size_t thread_idx) const {
  const auto fragments_it = all_tables_fragments.find(table_id);
  CHECK(fragments_it != all_tables_fragments.end());
  const auto fragments = fragments_it->second;
  const auto frag_count = fragments->size();
  std::vector<std::unique_ptr<ColumnarResults>> column_frags;
  const ColumnarResults* table_column = nullptr;
  const InputColDescriptor col_desc(col_id, table_id, int(0));
  CHECK(col_desc.getScanDesc().getSourceType() == InputSourceType::TABLE);
  {
    std::lock_guard<std::mutex> columnar_conversion_guard(columnar_fetch_mutex_);
    auto column_it = columnarized_scan_table_cache_.find(col_desc);
    if (column_it == columnarized_scan_table_cache_.end()) {
      for (size_t frag_id = 0; frag_id < frag_count; ++frag_id) {
        if (g_enable_non_kernel_time_query_interrupt && check_interrupt()) {
          throw QueryExecutionError(Executor::ERR_INTERRUPTED);
        }
        std::list<std::shared_ptr<Chunk_NS::Chunk>> chunk_holder;
        std::list<ChunkIter> chunk_iter_holder;
        const auto& fragment = (*fragments)[frag_id];
        if (fragment.isEmptyPhysicalFragment()) {
          continue;
        }
        auto chunk_meta_it = fragment.getChunkMetadataMap().find(col_id);
        CHECK(chunk_meta_it != fragment.getChunkMetadataMap().end());
        auto col_buffer = getOneTableColumnFragment(table_id,
                                                    static_cast<int>(frag_id),
                                                    col_id,
                                                    all_tables_fragments,
                                                    chunk_holder,
                                                    chunk_iter_holder,
                                                    Data_Namespace::CPU_LEVEL,
                                                    int(0),
                                                    device_allocator);
        column_frags.push_back(
            std::make_unique<ColumnarResults>(executor_->row_set_mem_owner_,
                                              col_buffer,
                                              fragment.getNumTuples(),
                                              chunk_meta_it->second->sqlType,
                                              thread_idx));
      }
      auto merged_results =
          ColumnarResults::mergeResults(executor_->row_set_mem_owner_, column_frags);
      table_column = merged_results.get();
      columnarized_scan_table_cache_.emplace(col_desc, std::move(merged_results));
    } else {
      table_column = column_it->second.get();
    }
  }
  return ColumnFetcher::transferColumnIfNeeded(table_column,
                                               0,
                                               &executor_->getCatalog()->getDataMgr(),
                                               memory_level,
                                               device_id,
                                               device_allocator);
}

const int8_t* ColumnFetcher::getResultSetColumn(
    const InputColDescriptor* col_desc,
    const Data_Namespace::MemoryLevel memory_level,
    const int device_id,
    DeviceAllocator* device_allocator,
    const size_t thread_idx) const {
  CHECK(col_desc);
  const auto table_id = col_desc->getScanDesc().getTableId();
  return getResultSetColumn(get_temporary_table(executor_->temporary_tables_, table_id),
                            table_id,
                            col_desc->getColId(),
                            memory_level,
                            device_id,
                            device_allocator,
                            thread_idx);
}

const int8_t* ColumnFetcher::linearizeColumnFragments(
    const int table_id,
    const int col_id,
    const std::map<int, const TableFragments*>& all_tables_fragments,
    std::list<std::shared_ptr<Chunk_NS::Chunk>>& chunk_holder,
    std::list<ChunkIter>& chunk_iter_holder,
    const Data_Namespace::MemoryLevel memory_level,
    const int device_id,
    DeviceAllocator* device_allocator,
    const size_t thread_idx) const {
  // todo(yoonmin): True varlen col linearization
  const auto fragments_it = all_tables_fragments.find(table_id);
  CHECK(fragments_it != all_tables_fragments.end());
  const auto fragments = fragments_it->second;
  const auto frag_count = fragments->size();
  const InputColDescriptor col_desc(col_id, table_id, int(0));
  const auto& cat = *executor_->getCatalog();
  auto cd = get_column_descriptor(col_id, table_id, cat);
  CHECK(cd);
  CHECK(col_desc.getScanDesc().getSourceType() == InputSourceType::TABLE);
  CHECK_GT(table_id, 0);
  size_t total_num_tuples = 0;
  size_t total_data_buf_size = 0;
  size_t total_idx_buf_size = 0;

  std::lock_guard<std::mutex> linearize_guard(columnar_fetch_mutex_);
  auto linearized_iter_it = linearized_multi_frag_chunk_iter_cache_.find(col_desc);
  if (linearized_iter_it != linearized_multi_frag_chunk_iter_cache_.end()) {
    if (memory_level == CPU_LEVEL) {
      return getChunkiter(col_desc, 0);
    } else {
      // todo(yoonmin): D2D copy of merged chunk and its iter?
      if (linearized_iter_it->second.find(device_id) !=
          linearized_iter_it->second.end()) {
        auto chunk_iter_gpu = device_allocator->alloc(sizeof(ChunkIter));
        device_allocator->copyToDevice(
            chunk_iter_gpu, getChunkiter(col_desc, device_id), sizeof(ChunkIter));
        return chunk_iter_gpu;
      }
    }
  }

  // collect target fragments
  // in GPU execution, we first load chunks in CPU, and only merge them in GPU
  std::shared_ptr<Chunk_NS::Chunk> chunk;
  std::list<std::shared_ptr<Chunk_NS::Chunk>> local_chunk_holder;
  std::list<ChunkIter> local_chunk_iter_holder;
  for (size_t frag_id = 0; frag_id < frag_count; ++frag_id) {
    const auto& fragment = (*fragments)[frag_id];
    if (fragment.isEmptyPhysicalFragment()) {
      continue;
    }
    auto chunk_meta_it = fragment.getChunkMetadataMap().find(col_id);
    CHECK(chunk_meta_it != fragment.getChunkMetadataMap().end());
    ChunkKey chunk_key{
        cat.getCurrentDB().dbId, fragment.physicalTableId, col_id, fragment.fragmentId};
    chunk = Chunk_NS::Chunk::getChunk(cd,
                                      &cat.getDataMgr(),
                                      chunk_key,
                                      Data_Namespace::CPU_LEVEL,
                                      0,
                                      chunk_meta_it->second->numBytes,
                                      chunk_meta_it->second->numElements);
    local_chunk_holder.push_back(chunk);
    local_chunk_iter_holder.push_back(chunk->begin_iterator(chunk_meta_it->second));
    total_num_tuples += fragment.getNumTuples();
    total_data_buf_size += chunk->getBuffer()->size();
    if (chunk->getIndexBuf()) {
      total_idx_buf_size += chunk->getIndexBuf()->size();
    }
  }
  // linearize collected fragments
  // todo(yoonmin): parallelize this step
  auto merged_chunk_buffer =
      cat.getDataMgr().alloc(memory_level,
                             memory_level == Data_Namespace::CPU_LEVEL ? 0 : device_id,
                             total_data_buf_size);
  size_t sum_chunk_sizes = 0;
  for (auto chunk_holder_it = local_chunk_holder.begin();
       chunk_holder_it != local_chunk_holder.end();
       chunk_holder_it++) {
    if (g_enable_non_kernel_time_query_interrupt && check_interrupt()) {
      cat.getDataMgr().free(merged_chunk_buffer);
      throw QueryExecutionError(Executor::ERR_INTERRUPTED);
    }
    auto target_chunk = chunk_holder_it->get();
    auto target_chunk_buffer = target_chunk->getBuffer();
    merged_chunk_buffer->append(
        target_chunk_buffer->getMemoryPtr(),
        target_chunk_buffer->size(),
        Data_Namespace::CPU_LEVEL,
        memory_level == Data_Namespace::CPU_LEVEL ? 0 : device_id);
    sum_chunk_sizes += target_chunk_buffer->size();
  }
  // check whether each chunk's data buffer is clean under chunk merging
  CHECK_EQ(total_data_buf_size, sum_chunk_sizes);

  // make ChunkIter for the linearized chunk
  // todo(yoonmin): cache for merged chunk?
  auto merged_chunk = std::make_shared<Chunk_NS::Chunk>(merged_chunk_buffer, nullptr, cd);
  auto merged_chunk_iter = prepareChunkIter(
      merged_chunk_buffer, *(chunk_iter_holder.begin()), total_num_tuples);
  chunk_holder.push_back(merged_chunk);
  chunk_iter_holder.push_back(merged_chunk_iter);
  auto merged_chunk_iter_ptr = reinterpret_cast<int8_t*>(&(chunk_iter_holder.back()));
  if (memory_level == MemoryLevel::CPU_LEVEL) {
    addMergedChunk(col_desc, 0, merged_chunk, merged_chunk_iter_ptr);
    return merged_chunk_iter_ptr;
  } else {
    CHECK_EQ(Data_Namespace::GPU_LEVEL, memory_level);
    CHECK(device_allocator);
    auto ab = merged_chunk->getBuffer();
    ab->pin();
    addMergedChunk(col_desc, device_id, merged_chunk, merged_chunk_iter_ptr);
    auto chunk_iter_gpu = device_allocator->alloc(sizeof(ChunkIter));
    device_allocator->copyToDevice(
        chunk_iter_gpu, merged_chunk_iter_ptr, sizeof(ChunkIter));
    return chunk_iter_gpu;
  }
}

const int8_t* ColumnFetcher::transferColumnIfNeeded(
    const ColumnarResults* columnar_results,
    const int col_id,
    Data_Namespace::DataMgr* data_mgr,
    const Data_Namespace::MemoryLevel memory_level,
    const int device_id,
    DeviceAllocator* device_allocator) {
  if (!columnar_results) {
    return nullptr;
  }
  const auto& col_buffers = columnar_results->getColumnBuffers();
  CHECK_LT(static_cast<size_t>(col_id), col_buffers.size());
  if (memory_level == Data_Namespace::GPU_LEVEL) {
    const auto& col_ti = columnar_results->getColumnType(col_id);
    const auto num_bytes = columnar_results->size() * col_ti.get_size();
    CHECK(device_allocator);
    auto gpu_col_buffer = device_allocator->alloc(num_bytes);
    device_allocator->copyToDevice(gpu_col_buffer, col_buffers[col_id], num_bytes);
    return gpu_col_buffer;
  }
  return col_buffers[col_id];
}

void ColumnFetcher::addMergedChunk(const InputColDescriptor col_desc,
                                   const int device_id,
                                   std::shared_ptr<Chunk_NS::Chunk> chunk_ptr,
                                   int8_t* chunk_iter_ptr) const {
  // 1. merged_chunk_ptr
  auto chunk_it = linearized_multi_frag_table_cache_.find(col_desc);
  if (chunk_it != linearized_multi_frag_table_cache_.end()) {
    auto chunk_device_it = chunk_it->second.find(device_id);
    if (chunk_device_it == chunk_it->second.end()) {
      VLOG(2) << "Additional merged chunk for col_desc (tbl: "
              << col_desc.getScanDesc().getTableId() << ", col: " << col_desc.getColId()
              << "), device: " << device_id;
      chunk_it->second.emplace(device_id, chunk_ptr);
    }
  } else {
    DeviceMergedChunkMap chunk_m;
    chunk_m.emplace(device_id, chunk_ptr);
    VLOG(2) << "New merged chunk for col_desc (tbl: "
            << col_desc.getScanDesc().getTableId() << ", col: " << col_desc.getColId()
            << "), device: " << device_id;
    linearized_multi_frag_table_cache_.emplace(col_desc, chunk_m);
  }

  // 2. merged_chunk_iter_ptr
  auto iter_it = linearized_multi_frag_chunk_iter_cache_.find(col_desc);
  if (iter_it != linearized_multi_frag_chunk_iter_cache_.end()) {
    auto iter_device_it = iter_it->second.find(device_id);
    if (iter_device_it == iter_it->second.end()) {
      VLOG(2) << "Additional merged chunk_iter for col_desc (tbl: "
              << col_desc.getScanDesc().getTableId() << ", col: " << col_desc.getColId()
              << "), device: " << device_id;
      iter_it->second.emplace(device_id, chunk_iter_ptr);
    }
  } else {
    DeviceMergedChunkIterMap iter_m;
    iter_m.emplace(device_id, chunk_iter_ptr);
    VLOG(2) << "New merged chunk_iter for col_desc (tbl: "
            << col_desc.getScanDesc().getTableId() << ", col: " << col_desc.getColId()
            << "), device: " << device_id;
    linearized_multi_frag_chunk_iter_cache_.emplace(col_desc, iter_m);
  }
}

const int8_t* ColumnFetcher::getChunkiter(const InputColDescriptor col_desc,
                                          const int device_id) const {
  auto linearized_chunk_iter_it = linearized_multi_frag_chunk_iter_cache_.find(col_desc);
  if (linearized_chunk_iter_it != linearized_multi_frag_chunk_iter_cache_.end()) {
    auto dev_iter_map_it = linearized_chunk_iter_it->second.find(device_id);
    if (dev_iter_map_it != linearized_chunk_iter_it->second.end()) {
      VLOG(2) << "Recycle merged chunk_iter for col_desc (tbl: "
              << col_desc.getScanDesc().getTableId() << ", col: " << col_desc.getColId()
              << "), device: " << device_id;
      return dev_iter_map_it->second;
    }
  }
  return nullptr;
}

ChunkIter ColumnFetcher::prepareChunkIter(AbstractBuffer* merged,
                                          ChunkIter& chunk_iter,
                                          const size_t total_num_tuples) const {
  ChunkIter merged_chunk_iter;
  merged_chunk_iter.start_pos = merged->getMemoryPtr();
  merged_chunk_iter.current_pos = merged_chunk_iter.start_pos;
  merged_chunk_iter.end_pos = merged->getMemoryPtr() + merged->size();
  merged_chunk_iter.num_elems = total_num_tuples;
  merged_chunk_iter.skip = chunk_iter.skip;
  merged_chunk_iter.skip_size = chunk_iter.skip_size;
  merged_chunk_iter.type_info = chunk_iter.type_info;
  return merged_chunk_iter;
}

const int8_t* ColumnFetcher::getResultSetColumn(
    const ResultSetPtr& buffer,
    const int table_id,
    const int col_id,
    const Data_Namespace::MemoryLevel memory_level,
    const int device_id,
    DeviceAllocator* device_allocator,
    const size_t thread_idx) const {
  const ColumnarResults* result{nullptr};
  {
    std::lock_guard<std::mutex> columnar_conversion_guard(columnar_fetch_mutex_);
    if (columnarized_table_cache_.empty() || !columnarized_table_cache_.count(table_id)) {
      columnarized_table_cache_.insert(std::make_pair(
          table_id, std::unordered_map<int, std::shared_ptr<const ColumnarResults>>()));
    }
    auto& frag_id_to_result = columnarized_table_cache_[table_id];
    int frag_id = 0;
    if (frag_id_to_result.empty() || !frag_id_to_result.count(frag_id)) {
      frag_id_to_result.insert(std::make_pair(
          frag_id,
          std::shared_ptr<const ColumnarResults>(columnarize_result(
              executor_->row_set_mem_owner_, buffer, thread_idx, frag_id))));
    }
    CHECK_NE(size_t(0), columnarized_table_cache_.count(table_id));
    result = columnarized_table_cache_[table_id][frag_id].get();
  }
  CHECK_GE(col_id, 0);
  return transferColumnIfNeeded(result,
                                col_id,
                                &executor_->getCatalog()->getDataMgr(),
                                memory_level,
                                device_id,
                                device_allocator);
}
