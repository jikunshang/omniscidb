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

class CiderExecutionKernel {
 public:
  CiderExecutionKernel(std::shared_ptr<Executor> executor) : executor_(executor){};
  CiderExecutionKernel() {
    executor_ = Executor::getExecutor(Executor::UNITARY_EXECUTOR_ID);
  }

  void runWithDataMultiFrag(const int8_t*** multi_col_buffers,
                            const int64_t* num_rows,
                            int64_t** out,
                            int32_t* matched_num,
                            int32_t* err_code);

  void runWithData(const int8_t** col_buffers,
                   const int64_t* num_rows,
                   int64_t** out,
                   int32_t* matched_num,
                   int32_t* err_code);

  void compileWorkUnit(const RelAlgExecutionUnit& ra_exe_unit,
                       const std::vector<InputTableInfo>& query_infos);

 private:
  // this class hold Executor to call compile and serializeLiterals method, could be
  // removed.
  std::shared_ptr<Executor> executor_;
  CompilationResult compilationResult_;
};

#endif  // OMNISCI_CIDEREXECUTIONKERNEL_H
