#include <stdio.h>
#include <string.h>
#include <string>
#include <vector>

#include "Embedded/DBEngine.h"

#include <arrow/api.h>
#include "Shared/ArrowUtil.h"

namespace util {

enum MERGE_FLAG { Normal = 0, Partial_Agg, Merge_Agg };

bool replace(std::string& str, const std::string& from, const std::string& to) {
  size_t start_pos = str.find(from);
  if (start_pos == std::string::npos)
    return false;
  str.replace(start_pos, from.length(), to);
  return true;
}

std::string random_string(std::size_t length) {
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

std::pair<std::shared_ptr<arrow::Schema>, std::string> parse_to_schema(
    const std::string& json_schema) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::string table_name = "";
  rapidjson::Document d;
  if (d.Parse(json_schema).HasParseError() || !d.HasMember("Columns")) {
    LOG(ERROR) << "invalid json for schema!";
  }
  table_name = d["Table"].GetString();
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
      std::string col_type_str(col_type);
      std::shared_ptr<arrow::DataType> type;
      if (col_type_str.compare("INT") == 0)
        type = arrow::int32();
      else if (col_type_str.compare("LONG") == 0)
        type = arrow::int64();
      else if (col_type_str.compare("FLOAT") == 0)
        type = arrow::float32();
      else if (col_type_str.compare("DOUBLE") == 0)
        type = arrow::float64();
      else
        LOG(WARNING) << "unsupported type: " << col_type_str;

      auto f = std::make_shared<arrow::Field>(std::string(col_name), type);
      fields.push_back(f);
    }
  }
  std::shared_ptr<arrow::Schema> s = std::make_shared<arrow::Schema>(fields);

  return std::make_pair(s, table_name);
}

void convertToArrowTable(std::shared_ptr<arrow::Table>& table,
                         int64_t* dataBuffers,
                         int64_t* nullBuffers,
                         const std::string& schema,
                         int rowCount,
                         std::string& table_name) {
  // todo:: null buffer???
  LOG(INFO) << "start convert " << rowCount;
  auto [s, table_name_] = parse_to_schema(schema);
  table_name = table_name_;
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  for (int i = 0; i < s->fields().size(); i++) {
    LOG(INFO) << "convert " << i << " name: " << s->fields()[i]->name();
    arrow::Type::type dataType = s->fields()[i]->type()->id();
    int length = 0;
    switch (dataType) {
      case arrow::Type::INT32:
      case arrow::Type::FLOAT:
        length = 4;
        break;
      case arrow::Type::INT64:
      case arrow::Type::DOUBLE:
        length = 8;
        break;
      default:
        LOG(FATAL) << "Unsupported type";
        break;
    }

    auto dataBuffer =
        std::make_shared<arrow::Buffer>((uint8_t*)(dataBuffers[i]), rowCount * length);
    auto nullBuffer =
        std::make_shared<arrow::Buffer>((uint8_t*)(nullBuffers[i]), rowCount);
    long ptr = (long)(dataBuffers[i]);

    std::shared_ptr<arrow::Array> array;
    if (dataType == arrow::Type::INT32) {
      array = std::make_shared<arrow::Int32Array>(rowCount, dataBuffer);
      LOG(DEBUG1) << "convert result, data: " << array->ToString();
      for (int j = 0; j < rowCount; j++) {
        // LOG(INFO) << "data: " << *((int32_t*)ptr + j);
      }
    } else if (dataType == arrow::Type::INT64) {
      array = std::make_shared<arrow::Int64Array>(rowCount, dataBuffer);
    } else if (dataType == arrow::Type::FLOAT) {
      array = std::make_shared<arrow::FloatArray>(rowCount, dataBuffer);
    } else if (dataType == arrow::Type::DOUBLE) {
      array = std::make_shared<arrow::FloatArray>(rowCount, dataBuffer);
    } else {
      LOG(WARNING) << "not supported type!";
    }
    arrays.push_back(array);
  }

  table = arrow::Table::Make(s, arrays, rowCount);
  LOG(DEBUG1) << "convert done " << table->ToString();

  return;
}

void convertResult(const std::shared_ptr<EmbeddedDatabase::Cursor>& res,
                   const int resultValuesLen,
                   const long* resultValuesPtr,
                   const long* resultNullsPtr) {
  auto rp = std::make_shared<CiderPrestoResultProvider>();
  rp->registerResultSet(res->getResultSet());
  std::vector<PrestoResult> result =
      std::any_cast<std::vector<PrestoResult>>(rp->convert());
  CHECK(resultValuesLen == result.size());
  for (int i = 0; i < resultValuesLen; i++) {
    int8_t* valueSrc = result[i].valueBuffer;
    int8_t* valueDst = (int8_t*)resultValuesPtr[i];
    int valueBufferLength = result[i].valueBufferLength;
    memcpy(valueDst, valueSrc, valueBufferLength);
    // int8_t* nullSrc = result[i].nullBuffer;
    int8_t* nullDst = (int8_t*)resultNullsPtr[i];
    int nullBufferLength = result[i].nullBufferLength;
    // memcpy(nullDst, nullSrc, nullBufferLength);
    memset(nullDst, 0x01, nullBufferLength);
  }

  rp->release();
}

void mergeBatchTable(
    const std::vector<std::shared_ptr<EmbeddedDatabase::Cursor>>& resultVec,
    std::shared_ptr<arrow::Table>& arrowTable) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> recordBatchVec;
  for (auto res : resultVec) {
    recordBatchVec.push_back(res->getArrowRecordBatch());
  }
  arrowTable = arrow::Table::FromRecordBatches(recordBatchVec).ValueOrDie();
}

}  // namespace util
