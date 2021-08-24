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
#include "Utils.h"

/*
 * Class:     com_mapd_CiderJNI
 * Method:    init
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_com_mapd_CiderJNI_init(JNIEnv* env, jclass cls) {
  logger::LogOptions log_options("");
  log_options.severity_ = logger::Severity::DEBUG4;
  log_options.severity_clog_ = logger::Severity::DEBUG4;
  logger::init(log_options);

  std::string opt_str = "/tmp/" + util::random_string(10) + " --calcite-port 5555";
  // todo: this is a shared_ptr, will it dispose?
  std::shared_ptr<EmbeddedDatabase::DBEngine>* pDbe =
      new std::shared_ptr<EmbeddedDatabase::DBEngine>;
  *pDbe = EmbeddedDatabase::DBEngine::create(opt_str);

  return reinterpret_cast<jlong>(pDbe);
}

/*
 * Class:     com_mapd_CiderJNI
 * Method:    close
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_mapd_CiderJNI_close(JNIEnv* env,
                                                    jclass cls,
                                                    jlong dbePtr) {
  std::shared_ptr<EmbeddedDatabase::DBEngine>* pDbe =
      reinterpret_cast<std::shared_ptr<EmbeddedDatabase::DBEngine>*>(dbePtr);
  (*pDbe).reset();
  delete pDbe;
}

/*
 * Class:     com_mapd_CiderJNI
 * Method:    processBlocks
 * Signature: (Ljava/lang/String;Ljava/lang/String;[J[J[J[JI)I
 */
JNIEXPORT jint JNICALL Java_com_mapd_CiderJNI_processBlocks(JNIEnv* env,
                                                            jclass cls,
                                                            jlong dbePtr,
                                                            jstring sql,
                                                            jstring schema,
                                                            jlongArray dataValues,
                                                            jlongArray dataNulls,
                                                            jlongArray resultValues,
                                                            jlongArray resultNulls,
                                                            jint rowCount,
                                                            jlong threadId,
                                                            jint mergeFlag) {
  if (dbePtr == 0) {
    LOG(FATAL) << "ERROR: NULLPTR ";
  }
  std::shared_ptr<EmbeddedDatabase::DBEngine>* pDbe =
      reinterpret_cast<std::shared_ptr<EmbeddedDatabase::DBEngine>*>(dbePtr);

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

  auto map = (*pDbe)->getResultMap();
  if (map->find(threadId) == map->end()) {
    LOG(INFO) << "create map";
    std::vector<std::shared_ptr<EmbeddedDatabase::Cursor>> resultVec;
    map->emplace(std::make_pair(threadId, resultVec));
  }
  std::string table_name;
  LOG(INFO) << "mergeFlag " << mergeFlag;
  if (mergeFlag == util::MERGE_FLAG::Partial_Agg ||
      mergeFlag == util::MERGE_FLAG::Normal) {
    util::convertToArrowTable(
        arrowTable, dataValuesPtr, dataNullsPtr, schemaPtr, rowCount, table_name);
  } else if (mergeFlag == util::MERGE_FLAG::Merge_Agg) {
    auto resultVec = map->operator[](threadId);
    if (resultVec.size() == 0) {
      LOG(WARNING) << "Some unexcpted case.";
      return 0;
    }
    LOG(INFO) << "resultVec size " << resultVec.size() << " map size " << map->size()
              << " thread id " << threadId;
    util::mergeBatchTable(resultVec, arrowTable);
    table_name = "TEST_TABLE";
  }

  std::string random_table_name = table_name + util::random_string(10);
  const char* sqlPtr = env->GetStringUTFChars(sql, nullptr);
  std::string queryInfo = "execute relalg " + std::string(sqlPtr);
  // std::string queryInfo = "select a from " + table_name + " where c > 13;";
  auto ret = util::replace(queryInfo, table_name, random_table_name);
  if (!ret) {
    LOG(FATAL) << "table name not match!";
  }

  (*pDbe)->importArrowTable(random_table_name, arrowTable);

  auto res = (*pDbe)->executeRA(queryInfo);

  if (mergeFlag == util::MERGE_FLAG::Partial_Agg) {
    LOG(INFO) << "thread id " << threadId;
    map->operator[](threadId).push_back(res);
    LOG(INFO) << "push size: " << map->operator[](threadId).size();
  }
  int count = res->getRowCount();
  LOG(INFO) << "return row count: " << count;
  // always do result converting even though partial agg may not use it.
  util::convertResult(res, resultValuesLen, resultValuesPtr, resultNullsPtr);

  return count;
}
