
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
    char* schema_ = R"(
      (i64 BIGINT,
       i32 INT,
       i16 SMALLINT,
       i8 TINYINT,
       d DOUBLE,
       f FLOAT,
       i1 BOOLEAN) WITH (FRAGMENT_SIZE = 100);
      )";
    std::string schema(schema_);
    ciderEntry->build_table("tmp", schema);
    delete ciderEntry;
  }

  return 0;
}