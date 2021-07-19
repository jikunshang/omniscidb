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

#include "Logger/Logger.h"
#include "QueryEngine/CiderArrowResultProvider.h"
#include "QueryEngine/CiderResultProvider.h"
#include "QueryEngine/CiderPrestoResultProvider.h"
#include "QueryEngine/Descriptors/RelAlgExecutionDescriptor.h"
#include "QueryRunner/CiderEntry.h"
#include "QueryRunner/QueryRunner.h"

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

  // new a CiderEntry
  CiderEntry* ciderEntry = new CiderEntry();
  int64_t ciderEntryPtr = reinterpret_cast<int64_t>(ciderEntry);
  // return ciderEntryPtr;

  // build table based on schema.
  const char* schemaPtr = env->GetStringUTFChars(schema, nullptr);
  std::string tableSchema(schemaPtr);
  std::string tableName("tmp_table");
  ciderEntry->build_table(tableName, tableSchema);
  env->ReleaseStringUTFChars(schema, schemaPtr);

  // set query info
  const char* sqlPtr = env->GetStringUTFChars(sql, nullptr);
  std::string queryInfo = "execute relalg " + std::string(sqlPtr);
  ciderEntry->set_query_info(queryInfo);
  env->ReleaseStringUTFChars(schema, schemaPtr);
  env->ReleaseStringUTFChars(sql, sqlPtr);

  jsize dataValuesLen = env->GetArrayLength(dataValues);
  jsize dataNullsLen = env->GetArrayLength(dataNulls);

  jlong* dataValuesPtr = env->GetLongArrayElements(dataValues, 0);
  jlong* dataNullsPtr = env->GetLongArrayElements(dataNulls, 0);

  jsize resultValuesLen = env->GetArrayLength(resultValues);
  jsize resultNullsLen = env->GetArrayLength(resultNulls);

  jlong* resultValuesPtr = env->GetLongArrayElements(resultValues, 0);
  jlong* resultNullsPtr = env->GetLongArrayElements(resultNulls, 0);

  printf("processing within JNI...\n");
  std::vector<int8_t*> dataBuffers;
  for (int i = 0; i < dataValuesLen; i++) {
    dataBuffers.push_back((int8_t*)(dataValuesPtr[i]));
  }
  // TODO: convert NULL values.

  auto dp =
      std::make_shared<BufferCiderDataProvider>(dataValuesLen, 0, dataBuffers, rowCount);
  auto rp = std::make_shared<CiderPrestoResultProvider>();
  int ret = ciderEntry->run_query(dp, rp);
  std::vector<PrestoResult> result = std::any_cast<std::vector<PrestoResult>>(rp->convert());
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

  return ret;
}
