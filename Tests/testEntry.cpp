
#include <stdio.h>
#include "TestHelpers.h"

#include "QueryRunner/CiderEntry.h"

int main(int argc, char* argv[]) {
  TestHelpers::init_logger_stderr_only(argc, argv);
  testing::InitGoogleTest(&argc, argv);
  logger::LogOptions log_options(argv[0]);
  log_options.max_files_ = 0;  // stderr only by default
  {
    CiderEntry* ciderEntry = new CiderEntry();

    char* schema_ = "{\"Columns\":[{\"c_customer_sk\":\"int\"},{\"c_customer_id\":\"long\"}]}";
    std::string schema(schema_);
    ciderEntry->build_table("customer", schema);

    // ciderEntry->set_query_info("execute relalg " + query);
    // ciderEntry->run_query(nullptr, nullptr);

    delete ciderEntry;
  }

  return 0;
}