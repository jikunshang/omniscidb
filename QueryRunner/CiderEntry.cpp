#include "CiderEntry.h"

#include <stdio.h>
#include <boost/filesystem.hpp>
#include <random>

#include "Logger/Logger.h"
#include "QueryRunner/QueryRunner.h"

using QR = QueryRunner::QueryRunner;

namespace util {

std::string gen_random_string(std::size_t length) {
  const std::string CHARACTERS =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

  std::random_device random_device;
  std::mt19937 generator(random_device());
  std::uniform_int_distribution<> distribution(0, CHARACTERS.size() - 1);

  std::string random_string;

  for (std::size_t i = 0; i < length; ++i) {
    random_string += CHARACTERS[distribution(generator)];
  }

  return random_string;
}

inline void run_ddl_statement(const std::string& input_str) {
  QR::get()->runDDLStatement(input_str);
}

void build_table(const std::string& table_name, std::string& table_schema) {
  run_ddl_statement("DROP TABLE IF EXISTS " + table_name + ";");

  run_ddl_statement("CREATE TABLE " + table_name + " " + table_schema);
}

}  // namespace util

CiderEntry::CiderEntry() {
  const char* initdb_path = std::getenv("INITDB_PATH");
  do {
    tmp_path_ = "/tmp/" + util::gen_random_string(10);
  } while (boost::filesystem::exists(boost::filesystem::path(tmp_path_.c_str())));
  LOG(DEBUG1) << "create dir path " << tmp_path_;
  // create tmp_path use boost::fs
  boost::filesystem::path path{tmp_path_.c_str()};
  CHECK(boost::filesystem::create_directory(path));
  std::string cmd = std::string(initdb_path) + " --data " + tmp_path_;

  // call initdb
  system(cmd.c_str());
  QR::init(tmp_path_.c_str());
}

void CiderEntry::build_table(const std::string& table_name,
                             const std::string& table_schema) {
  table_name_ = table_name;
  table_schema_ = table_schema;
  util::build_table(table_name_, table_schema_);
}

void CiderEntry::set_query_info(const std::string& query_info) {
  query_info_ = query_info;
}

CiderEntry::~CiderEntry() {
  // drop table
  util::run_ddl_statement("DROP TABLE IF EXISTS " + table_name_ + ";");

  // delete dir
  boost::filesystem::remove_all(boost::filesystem::path(tmp_path_.c_str()));
  LOG(DEBUG1) << "deconstructor done";
}

int CiderEntry::run_query(std::shared_ptr<BufferCiderDataProvider> dp,
                          std::shared_ptr<CiderArrowResultProvider> rp) {
  auto res_itr = QR::get()->ciderExecute(query_info_,
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
  return crt_row.size();
}