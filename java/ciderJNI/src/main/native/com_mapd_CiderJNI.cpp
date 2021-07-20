/*
 * Copyright 2020 OmniSci, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "com_mapd_CiderJNI.h"
#include <stdio.h>
#include <string.h>
#include <random>

#include "Logger/Logger.h"
#include "QueryEngine/CiderArrowResultProvider.h"
#include "QueryEngine/CiderPrestoResultProvider.h"
#include "QueryEngine/CiderResultProvider.h"
#include "QueryEngine/Descriptors/RelAlgExecutionDescriptor.h"
#include "QueryRunner/CiderEntry.h"
#include "QueryRunner/QueryRunner.h"

#include "Embedded/DBEngine.h"

#include <arrow/api.h>
#include "Shared/ArrowUtil.h"

namespace util {

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

std::shared_ptr<arrow::Schema> parse_to_schema(const std::string& json_schema) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
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
      std::string col_type_str(col_type);
      std::shared_ptr<arrow::DataType> type;
      if (col_type_str.compare("int") == 0)
        type = arrow::int32();
      else if (col_type_str.compare("long") == 0)
        type = arrow::int64();
      else if (col_type_str.compare("float") == 0)
        type = arrow::float32();
      else if (col_type_str.compare("double") == 0)
        type = arrow::float64();
      else
        LOG(WARNING) << "unsupported type: " << col_type_str;

      auto f = std::make_shared<arrow::Field>(std::string(col_name), type);
      fields.push_back(f);
    }
  }
  std::shared_ptr<arrow::Schema> s = std::make_shared<arrow::Schema>(fields);

  return s;
}

void convertToArrowTable(std::shared_ptr<arrow::Table>& table,
                         int64_t* dataBuffers,
                         int64_t* nullBuffers,
                         const std::string& schema,
                         int rowCount) {
  // todo:: null buffer???
  LOG(INFO) << "start convert " << rowCount;
  auto s = parse_to_schema(schema);
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  for (int i = 0; i < s->fields().size(); i++) {
    LOG(INFO) << "convert " << i << " type: " << s->fields()[i]->name();

    auto dataBuffer =
        std::make_shared<arrow::Buffer>((uint8_t*)(dataBuffers + i), rowCount);
    auto nullBuffer =
        std::make_shared<arrow::Buffer>((uint8_t*)(nullBuffers + i), rowCount);
    LOG(INFO) << "1111111 ";

    std::shared_ptr<arrow::Array> array;
    if (s->fields()[i]->type()->id() == arrow::Type::INT32) {
      array = std::make_shared<arrow::Int32Array>(rowCount, dataBuffer, nullBuffer);
    } else if (s->fields()[i]->type()->id() == arrow::Type::INT64) {
      array = std::make_shared<arrow::Int64Array>(rowCount, dataBuffer, nullBuffer);
    } else if (s->fields()[i]->type()->id() == arrow::Type::FLOAT) {
      array = std::make_shared<arrow::FloatArray>(rowCount, dataBuffer, nullBuffer);
    } else if (s->fields()[i]->type()->id() == arrow::Type::DOUBLE) {
      array = std::make_shared<arrow::FloatArray>(rowCount, dataBuffer, nullBuffer);
    } else {
      LOG(WARNING) << "not supported type!";
    }
    arrays.push_back(array);
  }

  table = arrow::Table::Make(s, arrays, rowCount);
  LOG(INFO) << "convert done";

  return;
}

}  // namespace util
/*
 * Class:     com_mapd_CiderJNI
 * Method:    processBlocks
 * Signature: (Ljava/lang/String;Ljava/lang/String;[J[J[J[JI)I
 */
JNIEXPORT jint JNICALL Java_com_mapd_CiderJNI_processBlocks(JNIEnv* env,
                                                            jclass cls,
                                                            jstring sql,
                                                            jstring schema,
                                                            jlongArray dataValues,
                                                            jlongArray dataNulls,
                                                            jlongArray resultValues,
                                                            jlongArray resultNulls,
                                                            jint rowCount) {
  logger::LogOptions log_options("");
  log_options.severity_ = logger::Severity::DEBUG4;
  log_options.severity_clog_ = logger::Severity::DEBUG4;
  logger::init(log_options);

  std::string opt_str = "/tmp/" + util::random_string(10) + " --calcite-port 5555";
  auto dbe = EmbeddedDatabase::DBEngine::create(opt_str);

  jsize dataValuesLen = env->GetArrayLength(dataValues);
  jsize dataNullsLen = env->GetArrayLength(dataNulls);

  jlong* dataValuesPtr = env->GetLongArrayElements(dataValues, 0);
  jlong* dataNullsPtr = env->GetLongArrayElements(dataNulls, 0);

  jsize resultValuesLen = env->GetArrayLength(resultValues);
  jsize resultNullsLen = env->GetArrayLength(resultNulls);

  jlong* resultValuesPtr = env->GetLongArrayElements(resultValues, 0);
  jlong* resultNullsPtr = env->GetLongArrayElements(resultNulls, 0);

  std::shared_ptr<arrow::Table> arrowTable;
  const char* schemaPtr = env->GetStringUTFChars(schema, nullptr);
  std::string tableSchema(schemaPtr);
  util::convertToArrowTable(arrowTable, dataValuesPtr, dataNullsPtr, schemaPtr, rowCount);
  dbe->importArrowTable("test", arrowTable);

  const char* sqlPtr = env->GetStringUTFChars(sql, nullptr);
  // std::string queryInfo = "execute relalg " + std::string(sqlPtr);
  std::string queryInfo = std::string(sqlPtr);
  auto res = dbe->executeRA(queryInfo);

  int count = res->getRowCount();

  // converting result
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
    int8_t* nullSrc = result[i].nullBuffer;
    int8_t* nullDst = (int8_t*)resultNullsPtr[i];
    int nullBufferLength = result[i].nullBufferLength;
    memcpy(nullDst, nullSrc, nullBufferLength);
  }

  rp->release();

  return count;
}
