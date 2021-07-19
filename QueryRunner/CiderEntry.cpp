#include "CiderEntry.h"

#include <stdio.h>
#include <boost/filesystem.hpp>
#include <random>

#include "Logger/Logger.h"
#include "QueryRunner/QueryRunner.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <thread>
#include <chrono>

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

std::string type_convertor(std::string in) {
  if (in.compare("int") == 0)
    return "INT";
  if (in.compare("long") == 0)
    return "BIGINT";
  if (in.compare("short") == 0)
    return "SMALLINT";
  if (in.compare("byte") == 0)
    return "TINYINT";
  if (in.compare("bool") == 0)
    return "BOOLEAN";
  if (in.compare("float") == 0)
    return "FLOAT";
  if (in.compare("double") == 0)
    return "DOUBLE";
  LOG(WARNING) << "Type " << in << "not support yet!";
  return "";
}

std::string parse_schema(std::string json_schema) {
  rapidjson::Document d;
  if (d.Parse(json_schema).HasParseError() || !d.HasMember("Columns")) {
    LOG(ERROR) << "invalid json for schema!";
  }
  const rapidjson::Value& columns = d["Columns"];
  std::string schema;
  for (rapidjson::Value::ConstValueIterator v_iter = columns.Begin();
       v_iter != columns.End();
       ++v_iter) {
    const rapidjson::Value& field = *v_iter;
    for (rapidjson::Value::ConstMemberIterator m_iter = field.MemberBegin();
         m_iter != field.MemberEnd();
         ++m_iter) {
      const char* col_name = m_iter->name.GetString();
      const char* col_type = m_iter->value.GetString();
      schema += std::string(col_name) + " " + type_convertor(col_type) + ",";
    }
  }
  // remove last ","
  schema = schema.substr(0, schema.size() - 1);
  schema = "( " + schema + " );";

  return schema;
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
  LOG(INFO) << "init db done!";

  std::this_thread::sleep_for(std::chrono::seconds(2));

  QR::init(tmp_path_.c_str());
  LOG(INFO) << "init QR done!";

}

void CiderEntry::build_table(const std::string& table_name,
                             const std::string& table_schema) {
  table_name_ = table_name;
  table_schema_ = util::parse_schema(table_schema);
  LOG(INFO) << "schema is " << table_schema_;
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
                          std::shared_ptr<CiderResultProvider> rp) {
  auto res_itr = QR::get()->ciderExecute(query_info_,
                                         ExecutorDeviceType::CPU,
                                         /*hoist_literals=*/true,
                                         /*allow_loop_joins=*/false,
                                         /*just_explain=*/false,
                                         dp,
                                         rp);
  auto res = res_itr->next(/* dummy size = */dp->getNumRows());
  return rp->getRows()->rowCount();
}
