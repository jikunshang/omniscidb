/*
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

#ifndef OMNISCI_CIDEREXECUTIONKERNEL_H
#define OMNISCI_CIDEREXECUTIONKERNEL_H

#include <memory>
#include <vector>

#include <QueryEngine/InputMetadata.h>
#include <QueryEngine/RelAlgExecutionUnit.h>

#include "CiderDataProvider.h"
#include "CiderExecutionKernel.h"

class CiderExecutionKernel {
 public:
  virtual ~CiderExecutionKernel(){};
  void runWithDataMultiFrag(const int8_t*** multi_col_buffers,
                            const int64_t* num_rows,
                            int64_t** out,
                            int32_t* matched_num,
                            int32_t* err_code,
                            int64_t* init_agg_vals);

  void runWithData(const int8_t** col_buffers,
                   const int64_t* num_rows,
                   int64_t** out,
                   int32_t* matched_num,
                   int32_t* err_code,
                   int64_t* init_agg_vals);

  void compileWorkUnit(const RelAlgExecutionUnit& ra_exe_unit,
                       const std::vector<InputTableInfo>& query_infos);

  virtual std::string getLlvmIR();

  // todo: add PlanProvider, SchemaProvider
  static std::shared_ptr<CiderExecutionKernel> create(
      std::shared_ptr<CiderDataProvider>,
      std::shared_ptr<CiderResultConvertor>);

  void processBatch() {
    // provider should have update input data
    auto input = ciderDataProvider_->getData();

    // todo: refactor runWithData
    auto result = runWithData(input);

    ciderResultConvertor_->update(result);
    // ResultConvertor should convert to target format later.
  }

 protected:
  std::shared_ptr<CiderDataProvider> ciderDataProvider_;
  std::shared_ptr<CiderResultConvertor> ciderResultConvertor_;
  CiderExecutionKernel(){};
  CiderExecutionKernel(const CiderExecutionKernel&) = delete;
  CiderExecutionKernel& operator=(const CiderExecutionKernel&) = delete;
};

#endif  // OMNISCI_CIDEREXECUTIONKERNEL_H