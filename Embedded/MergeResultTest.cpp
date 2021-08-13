#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <exception>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include "DBEngine.h"
#include "QueryEngine/ResultSet.h"

#include <arrow/api.h>
#include <arrow/csv/reader.h>
#include <arrow/io/file.h>
#include "Shared/ArrowUtil.h"

using namespace EmbeddedDatabase;

using BatchResult = std::vector<std::shared_ptr<Cursor>>;

std::shared_ptr<arrow::Table> importTable() {
  auto memory_pool = arrow::default_memory_pool();
  auto arrow_parse_options = arrow::csv::ParseOptions::Defaults();
  auto arrow_read_options = arrow::csv::ReadOptions::Defaults();
  auto arrow_convert_options = arrow::csv::ConvertOptions::Defaults();
  std::shared_ptr<arrow::io::ReadableFile> inp;
  auto file_result = arrow::io::ReadableFile::Open("/tmp/example_2.csv");
  ARROW_THROW_NOT_OK(file_result.status());
  inp = file_result.ValueOrDie();
  auto table_reader_result = arrow::csv::TableReader::Make(
      memory_pool, inp, arrow_read_options, arrow_parse_options, arrow_convert_options);
  ARROW_THROW_NOT_OK(table_reader_result.status());
  auto table_reader = table_reader_result.ValueOrDie();
  std::shared_ptr<arrow::Table> arrowTable;
  auto arrow_table_result = table_reader->Read();
  ARROW_THROW_NOT_OK(arrow_table_result.status());
  arrowTable = arrow_table_result.ValueOrDie();
  std::cout << arrowTable->schema()->ToString() << std::endl;

  return arrowTable;
}

bool replace(std::string& str, const std::string& from, const std::string& to) {
  size_t start_pos = str.find(from);
  if (start_pos == std::string::npos)
    return false;
  str.replace(start_pos, from.length(), to);
  return true;
}


// Multi thread will core dump :(
int main() {
  int thread_num = 4;
  int iterate_num = 10;
  std::string partial_agg_sql = "select sum(int1) from TABLE_NAME;";
  std::string merge_agg_sql = "";
  BatchResult results(thread_num);
  std::vector<std::shared_ptr<arrow::Table>> arrow_tables(thread_num);
  // init batch result map
  std::map<std::string, BatchResult> map;
  std::vector<std::string> ids;
  for (int i = 0; i < thread_num; i++) {
    ids.push_back("id" + std::to_string(i));
  }
  for (auto id : ids) {
    if (map.find(id) == map.end()) {
      // auto batchResult = std::make_shared<BatchResult>();
      BatchResult batchResult;
      map.emplace(std::make_pair(id, batchResult));
    }
  }

  // init DBEngine and arrow table
  std::string opt_str = "--data /tmp/1 --calcite-port=5555";
  auto dbe = DBEngine::create(opt_str);
  if (!dbe) {
    std::cerr << "create dbe error, exit";
    return -1;
  }
  for (int i = 0; i < thread_num; i++) {
    arrow_tables[i] = importTable();
  }
  // auto arrowTable = importTable();

  std::cout << "arrowTable read done" << std::endl;

  // start 4 threads and run 10 iterate for each thread. keep all batch result in map.
  std::vector<std::thread> threads(thread_num);
  for (int i = 0; i < thread_num; i++) {
    threads[i] = std::thread([&, i] {
      for (int j = 0; j < iterate_num; j++) {
        std::cout << "---------iteration: " << j << "----------" << std::endl;
        std::string tmp_table_name =
            "test_table_" + std::to_string(i) + "_" + std::to_string(j);
        std::cout << "thread " << i << " importing table ";
        dbe->importArrowTable(tmp_table_name, arrow_tables[i]);
        std::cout << "thread " << i << " import table done";
        std::string tmp_sql = partial_agg_sql;
        replace(tmp_sql, "TABLE_NAME", tmp_table_name);
        // replace table name in sql string
        auto res = dbe->executeDML(tmp_sql);
        map[ids[i]].push_back(res);
      }
    });
  }
  for (int i = 0; i < thread_num; i++) {
    threads[i].join();
  }

  // for each thread, we will do a final merge.
  std::vector<std::shared_ptr<arrow::Table>> tables;
  for (auto it = map.begin(); it != map.end(); it++) {
    std::cout << "------------------------------- "
              << "????????" << it->first << std::endl;
    auto cursorVec = it->second;
    std::vector<std::shared_ptr<arrow::RecordBatch>> recordBatchVec;
    for (auto res : cursorVec) {
      recordBatchVec.push_back(res->getArrowRecordBatch());
    }
    auto table = arrow::Table::FromRecordBatches(recordBatchVec).ValueOrDie();
    tables.push_back(table);
    std::cout << "------------------------------- " << table->schema()->ToString()
              << std::endl;
  }

  for (int i = 0; i < thread_num; i++) {
    threads[i] = std::thread([&, i] {
      std::cout << "import table start" << std::endl;
      std::string tmp_table_name = "test_table_final_" + std::to_string(i);
      dbe->importArrowTable(tmp_table_name, tables[i]);
      std::cout << "import table done" << std::endl;

      // replace table name in sql string
      auto res = dbe->executeDML("SELECT SUM(EXPR$0) from " + tmp_table_name + ";");
      std::cout << res->getArrowRecordBatch()->ToString() << std::endl;
      results[i] = res;
    });
  }
  for (int i = 0; i < thread_num; i++) {
    threads[i].join();
  }

  return 0;
}