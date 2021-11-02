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
  RelAlgExecutionUnit ra_exe_unit{};

   int table_id = 100;
   int column_id_0 = 1;
   int column_id_1 = 2;

   // input_descs
   std::vector<InputDescriptor> input_descs;
   InputDescriptor input_desc_0(table_id, 0);
   input_descs.push_back(input_desc_0);
   ra_exe_unit.input_descs = input_descs;

   // input_col_descs
   std::list<std::shared_ptr<const InputColDescriptor>> input_col_descs;
   std::shared_ptr<const InputColDescriptor> input_col_desc_0 =
       std::make_shared<const InputColDescriptor>(column_id_0, table_id, 0);
   std::shared_ptr<const InputColDescriptor> input_col_desc_1 =
       std::make_shared<const InputColDescriptor>(column_id_1, table_id, 0);
   input_col_descs.push_back(input_col_desc_0);
   input_col_descs.push_back(input_col_desc_1);
   ra_exe_unit.input_col_descs = input_col_descs;

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
   d.intval = 757382400;
   std::shared_ptr<Analyzer::Expr> rightExpr = std::make_shared<Analyzer::Constant>(dateSqlType, false, d);
   std::shared_ptr<Analyzer::Expr> simple_qual_0 =
       std::make_shared<Analyzer::BinOper>(ti_boolean, false, SQLOps::kGE, SQLQualifier::kONE, leftExpr, rightExpr);
   std::list<std::shared_ptr<Analyzer::Expr>> simple_quals;
   simple_quals.push_back(simple_qual_0);

   ra_exe_unit.simple_quals = simple_quals;

   std::vector<Analyzer::Expr*> target_exprs;
   Analyzer::ColumnVar* target_expr_0 = new Analyzer::ColumnVar(ti_long, table_id, column_id_1, 0);

   target_exprs.push_back(target_expr_0);

   ra_exe_unit.target_exprs = target_exprs;
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
  long l = 0;
  // step 3
//  createKernel()
//
//  runWithInput()
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
