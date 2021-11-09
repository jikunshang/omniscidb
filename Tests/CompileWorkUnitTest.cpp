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

 RelAlgExecutionUnit buildFakeRelAlgEU() {


   int table_id = 100;
   int column_id_0 = 1;
   int column_id_1 = 2;

   // input_descs
   std::vector<InputDescriptor> input_descs;
   InputDescriptor input_desc_0(table_id, 0);
   input_descs.push_back(input_desc_0);

   // input_col_descs
   std::list<std::shared_ptr<const InputColDescriptor>> input_col_descs;
   std::shared_ptr<const InputColDescriptor> input_col_desc_0 =
       std::make_shared<const InputColDescriptor>(column_id_0, table_id, 0);
   std::shared_ptr<const InputColDescriptor> input_col_desc_1 =
       std::make_shared<const InputColDescriptor>(column_id_1, table_id, 0);
   input_col_descs.push_back(input_col_desc_0);
   input_col_descs.push_back(input_col_desc_1);

   // simple_quals
   SQLTypes sqlTypes{SQLTypes::kBOOLEAN};
   SQLTypes subtypes{SQLTypes::kNULLT};
   SQLTypes dateSqlType{SQLTypes::kDATE};
   SQLTypes longTypes{SQLTypes::kBIGINT};

   SQLTypeInfo date_col_info(dateSqlType, 0,0,false,EncodingType::kENCODING_DATE_IN_DAYS, 0, subtypes);
   std::shared_ptr<Analyzer::ColumnVar> col1 = std::make_shared<Analyzer::ColumnVar>(date_col_info, table_id, column_id_0, 0);
   SQLTypeInfo ti_boolean(sqlTypes, 0, 0, false, EncodingType::kENCODING_NONE, 0, subtypes);
   SQLTypeInfo ti_long(longTypes, 0, 0, false, EncodingType::kENCODING_NONE, 0, subtypes);

   std::shared_ptr<Analyzer::Expr> leftExpr = std::make_shared<Analyzer::UOper>(date_col_info, false, SQLOps::kCAST, col1);
   Datum d;
   d.bigintval = 757382400;
   std::shared_ptr<Analyzer::Expr> rightExpr = std::make_shared<Analyzer::Constant>(dateSqlType, false, d);
   std::shared_ptr<Analyzer::Expr> simple_qual_0 =
       std::make_shared<Analyzer::BinOper>(ti_boolean, false, SQLOps::kGE, SQLQualifier::kONE, leftExpr, rightExpr);
   std::list<std::shared_ptr<Analyzer::Expr>> simple_quals;
   simple_quals.push_back(simple_qual_0);

   std::vector<Analyzer::Expr*> target_exprs;
   Analyzer::ColumnVar* target_expr_0 = new Analyzer::ColumnVar(ti_long, table_id, column_id_1, 0);

   target_exprs.push_back(target_expr_0);

   std::list<std::shared_ptr<Analyzer::Expr>> groupby_exprs;
   groupby_exprs.push_back(nullptr);

//   ra_exe_unit.input_descs = input_descs;
//   ra_exe_unit.input_col_descs = input_col_descs;
//   ra_exe_unit.simple_quals = simple_quals;
//   ra_exe_unit.groupby_exprs = groupby_exprs;
//   ra_exe_unit.target_exprs = target_exprs;

   RelAlgExecutionUnit ra_exe_unit{input_descs,
                                   input_col_descs,
                                   simple_quals,
                                   std::list<std::shared_ptr<Analyzer::Expr>>(),
                                   JoinQualsPerNestingLevel(),
                                   groupby_exprs,
                                   target_exprs
   };

   return ra_exe_unit;
 }

TEST(BuildEU, EU1) {
  ASSERT_TRUE(true);
  auto executor = Executor::getExecutor(Executor::UNITARY_EXECUTOR_ID);  // step1
  CompilationResult compilation_result;
  std::unique_ptr<QueryMemoryDescriptor> query_mem_desc;

  // needed parameters
  std::vector<InputTableInfo> query_infos;
  Fragmenter_Namespace::FragmentInfo fi_0;
  fi_0.fragmentId = 0;
  fi_0.shadowNumTuples = 20;
  fi_0.physicalTableId = 100;
  fi_0.setPhysicalNumTuples(20);

  Fragmenter_Namespace::TableInfo ti_0 ;
  ti_0.fragments = {fi_0};
  ti_0.setPhysicalNumTuples(20);

  InputTableInfo iti_0{100, ti_0};
  query_infos.push_back(iti_0);

  PlanState::DeletedColumnsMap deleted_cols_map;
  CompilationOptions co = CompilationOptions::defaults(ExecutorDeviceType::CPU);
  ExecutionOptions eo;
  CudaMgr_Namespace::CudaMgr* cuda_mgr = nullptr;
  bool allow_lazy_fetch{false};
  std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner = nullptr;
  size_t max_groups_buffer_entry_guess {0};
  int8_t crt_min_byte_width {MAX_BYTE_WIDTH_SUPPORTED} ;
  bool has_cardinality_estimation{false};
  ColumnCacheMap column_cache;
  RenderInfo* render_info = nullptr;

  RelAlgExecutionUnit ra_exe_unit = buildFakeRelAlgEU();

  // this method will fail currently :(
  std::tie(compilation_result, query_mem_desc) =   // step 2
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

  using agg_query = void (*)(const int8_t***,  // col_buffers
      const uint64_t*,  // num_fragments
      const int8_t*,    // literals
      const int64_t*,   // num_rows
      const uint64_t*,  // frag_row_offsets
      const int32_t*,   // max_matched
      int32_t*,         // total_matched
      const int64_t*,   // init_agg_value
      int64_t**,        // out
      int32_t*,         // error_code
      const uint32_t*,  // num_tables
      const int64_t*);  // join_hash_tables_ptr

  const int8_t*** multi_col_buffer = (const int8_t***) std::malloc(sizeof(int8_t**) * 1); // we have only one fragment

  int8_t** col_buffer_frag_0 = (int8_t**)std::malloc(sizeof(int8_t*) * 2); // we have two columns
  multi_col_buffer[0] = (const int8_t**) col_buffer_frag_0;

  int32_t* date_col_id_0 = (int32_t*)std::malloc(sizeof(int32_t) * 10);
  int64_t* long_col_id_1 = (int64_t*)std::malloc(sizeof(int64_t) * 10);

  for(int i=0; i < 5; i++)  {
    date_col_id_0[i] = 8777;
    long_col_id_1[i] = i;
  }

  for(int i=5; i < 10; i++)  {
    date_col_id_0[i] = 8700;
    long_col_id_1[i] = i;
  }

  col_buffer_frag_0[0] = reinterpret_cast<int8_t*>(date_col_id_0);
  col_buffer_frag_0[1] = reinterpret_cast<int8_t*>(long_col_id_1);


//  const int8_t*** col_buffers = nullptr;
  uint64_t num_fragments = 1;
  std::vector<int8_t> literal_vec = executor->serializeLiterals(compilation_result.literal_values, 0);
  int8_t* literals = literal_vec.data();
  int64_t num_rows = 10;
  uint64_t frag_row_offsets = 0;
  int32_t max_matched = 10;
  int32_t total_matched = 0;
  int64_t init_agg_value = 0;
  int64_t** out = (int64_t**) std::malloc(sizeof(int64_t**) * 1);
  int64_t* out_col_0 = (int64_t*) std::malloc(sizeof(int64_t*)  *  10);
  std::memset(out_col_0, 0, sizeof(int64_t*) * 10);
  out[0] = out_col_0;

  int32_t error_code = 0;
  uint32_t num_tables  = 1;
  int64_t* join_hash_tables_ptr = nullptr;

  std::shared_ptr<CpuCompilationContext> ccc =
    std::dynamic_pointer_cast<CpuCompilationContext>(compilation_result.generated_code);
  reinterpret_cast<agg_query>(ccc->func())(multi_col_buffer,
                                           &num_fragments,
                                           literals,
                                           &num_rows,
                                           &frag_row_offsets,
                                           &max_matched,
                                           &total_matched,
                                           &init_agg_value,
                                           out,
                                           &error_code,
                                           &num_tables,
                                           join_hash_tables_ptr);

  std::cout<< "total match " << total_matched <<  " rows, they are ";
  for(int i = 0; i < total_matched; i++) {
    std::cout << "row " << out_col_0[i] << ", ";
  }
  std::cout << std::endl;
}

int main(int argc, char** argv) {
  int err = 0;

  namespace po = boost::program_options;

  po::options_description desc("Options");

  desc.add_options()("disable-shared-mem-group-by",
      po::value<bool>(&g_enable_smem_group_by)
      ->default_value(g_enable_smem_group_by)
      ->implicit_value(false),
      "Enable/disable using GPU shared memory for GROUP BY.");
  desc.add_options()("enable-columnar-output",
      po::value<bool>(&g_enable_columnar_output)
      ->default_value(g_enable_columnar_output)
      ->implicit_value(true),
      "Enable/disable using columnar output format.");
  desc.add_options()("enable-bump-allocator",
      po::value<bool>(&g_enable_bump_allocator)
      ->default_value(g_enable_bump_allocator)
      ->implicit_value(true),
      "Enable the bump allocator for projection queries on GPU.");

  logger::LogOptions log_options(argv[0]);
  log_options.severity_ = logger::Severity::DEBUG1;
  log_options.severity_clog_ = logger::Severity::DEBUG1;
  log_options.set_options();

  logger::init(log_options);


  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }

  return err;
}
