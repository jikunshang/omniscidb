#include "TestHelpers.h"

#include "../ImportExport/Importer.h"
#include "../Parser/parser.h"
#include "../QueryEngine/ArrowResultSet.h"
#include "../QueryEngine/CgenState.h"
#include "../QueryEngine/Descriptors/RelAlgExecutionDescriptor.h"
#include "../QueryEngine/Execute.h"
#include "../QueryEngine/ExpressionRange.h"
#include "../QueryEngine/RelAlgExecutionUnit.h"
#include "../QueryEngine/ResultSetReductionJIT.h"
#include "../QueryRunner/QueryRunner.h"
#include "../Shared/DateConverters.h"
#include "../Shared/StringTransform.h"
#include "../Shared/scope.h"
#include "../SqliteConnector/SqliteConnector.h"
#include "ClusterTester.h"
#include "DistributedLoader.h"

#include <gtest/gtest.h>
#include <boost/algorithm/string.hpp>
#include <boost/any.hpp>
#include <boost/program_options.hpp>

#include <cmath>
#include <cstdio>
#include <string>
#include <vector>

#ifndef BASE_PATH
#define BASE_PATH "./tmp"
#endif

// RelAlgExecutionUnit buildFakeRelAlgEU() {
//
// }

TEST(BuildEU, EU1) {
  ASSERT_TRUE(true);
  auto executor = Executor::getExecutor(Executor::UNITARY_EXECUTOR_ID);
  CompilationResult compilation_result;
  std::unique_ptr<QueryMemoryDescriptor> query_mem_desc;

  // needed parameters
  std::vector<InputTableInfo> query_infos;
  PlanState::DeletedColumnsMap deleted_cols_map;
  RelAlgExecutionUnit ra_exe_unit{};
  CompilationOptions co;
  ExecutionOptions eo;
  CudaMgr_Namespace::CudaMgr* cuda_mgr = nullptr;
  bool allow_lazy_fetch{false};
  std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner = nullptr;
  size_t max_groups_buffer_entry_guess {0};
  int8_t crt_min_byte_width {0} ;
  bool has_cardinality_estimation{false};
  ColumnCacheMap column_cache;
  RenderInfo* render_info = nullptr;

  // this method will fail currently :(
  std::tie(compilation_result, query_mem_desc) =
      executor->compileWorkUnit(query_infos,
                                deleted_cols_map,
                                ra_exe_unit,
                                co,
                                eo,
                                cuda_mgr,
                                allow_lazy_fetch,
                                row_set_mem_owner,
                                max_groups_buffer_entry_guess,
                                crt_min_byte_width,
                                has_cardinality_estimation,
                                column_cache,
                                render_info);
}

int main() {
  int err = 0;
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }

  return err;
}
