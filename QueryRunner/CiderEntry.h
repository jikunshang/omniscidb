#ifndef CIDER_ENTRY_H
#define CIDER_ENTRY_H

#include <string>
#include "QueryEngine/CiderArrowResultProvider.h"
#include "QueryRunner/QueryRunner.h"

class CiderEntry {
 public:
  // call initdb in constructor.
  CiderEntry();

  // set table info, will do create table related things.
  void build_table(const std::string& table_name, const std::string& table_schema);

  // idealy this API should also do compile/codegen work. while we havn't impl yet.
  void set_query_info(const std::string& query_info);

  // return result row size.
  int run_query(std::shared_ptr<BufferCiderDataProvider> dp,
                std::shared_ptr<CiderArrowResultProvider> rp);

  ~CiderEntry();


 private:
  std::string query_info_;  // this should be a json string
  std::string table_name_;
  std::string table_schema_;
  std::string tmp_path_;
};

#endif
