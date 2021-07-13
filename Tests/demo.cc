#include "TestHelpers.h"

#include "Logger/Logger.h"

#include "../QueryEngine/CiderArrowResultProvider.h"
#include "../QueryEngine/CiderResultProvider.h"
#include "../QueryEngine/Descriptors/RelAlgExecutionDescriptor.h"
#include "../QueryEngine/Execute.h"
#include "../QueryRunner/QueryRunner.h"

#include <array>
#include <future>
#include <string>
#include <vector>

#ifndef BASE_PATH
#define BASE_PATH "./tmp"
#endif

using QR = QueryRunner::QueryRunner;

using namespace TestHelpers;

inline void run_ddl_statement(const std::string& input_str) {
  QR::get()->runDDLStatement(input_str);
}

// we should build a schema via input.
const char* schema = R"(
    (
        i64 BIGINT,
        i32 INT,
        i16 SMALLINT,
        i8 TINYINT,
        d DOUBLE,
        f FLOAT,
        i1 BOOLEAN,
        str TEXT ENCODING DICT(32),
        arri64 BIGINT[]
    ) WITH (FRAGMENT_SIZE=100);
)";

std::string getSchemaViaInput(std::string inputInfo) {
  return inputInfo;
}

void build_table(const std::string& table_name, std::string& table_schema) {
  run_ddl_statement("DROP TABLE IF EXISTS " + table_name + ";");

  run_ddl_statement("CREATE TABLE " + table_name + " " + table_schema);
}

std::shared_ptr<BufferCiderDataProvider> build_data_provider() {
  // it's very tricky since we didn't know the database ID
  std::vector<int8_t*> buffers;
  int64_t buffer0[20] = {10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
                         10, 10, 10, 10, 10, 10, 10, 10, 10, 10};
  int32_t buffer1[20] = {10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
                         50, 50, 50, 50, 50, 0,  10, 20, 30, 40};
  buffers.resize(9);  // we have 9 columns total
  buffers[0] = (int8_t*)buffer0;
  buffers[1] = (int8_t*)buffer1;

  auto dp = std::make_shared<BufferCiderDataProvider>(9, 0, buffers, 20);
  return dp;
}

TargetValue run(const std::string& query_str,
                std::shared_ptr<BufferCiderDataProvider> dp) {
  auto rp = std::make_shared<CiderArrowResultProvider>();
  auto res_itr = QR::get()->ciderExecute(query_str,
                                         ExecutorDeviceType::CPU,
                                         /*hoist_literals=*/true,
                                         /*allow_loop_joins=*/false,
                                         /*just_explain=*/false,
                                         dp,
                                         rp);
  auto res = res_itr->next(/* dummy size = */ 100);
  auto crt_row = res->getRows()->getNextRow(true, true);
  std::shared_ptr<arrow::RecordBatch> record_batch =
      std::any_cast<std::shared_ptr<arrow::RecordBatch>>(rp->convert());
  CHECK_EQ(size_t(1), record_batch->num_rows()) << query_str;
  CHECK_EQ(size_t(1), crt_row.size()) << query_str;
  return crt_row[0];
}

int main() {
  // jni call 1, pass table schema
  {
    // TODO: need call initdb in a tmp dir?
    std::string table_name = "poc_test";
    std::string table_schema = getSchemaViaInput(schema);
    build_table(table_name, table_schema);
  }
  
  // jni call 2, pass plan and store locally
  {
    // or use json. table name must match!!!
    std::string query_str = "SELECT COUNT(*) FROM " + table_name + " where i32 < 50";
  }

  // jni call 2-N, process batch data for each call.
  {
    std::shared_ptr<BufferCiderDataProvider> dp = build_data_provider();

    auto result = run(query_str, dp);
    //todo: result.convert.return;
  }

  // jni call close, drop table remove tmp files.


  return 0;
}
