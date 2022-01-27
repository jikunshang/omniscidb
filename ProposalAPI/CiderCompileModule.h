/*
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

#ifndef OMNISCI_CIDERCOMPILEMODULE_H
#define OMNISCI_CIDERCOMPILEMODULE_H

class CiderCompileModule {
 public:
  CiderCompileModule(std::shared_ptr<CiderPlanProvider> planProvider,
                     std::shared_ptr<CiderSchemaProvider> schemaProvider)
      : planProvider_(planProvider), schemaProvider_(schemaProvider) {}

  void compile();

  bool isCompiled() { return isCompiled_; }

  std::string getLLVMIR();

  // todo: w/ and wo/ group by may be two kind of function/ function ptr
  CiderFunction getFunction();

 protected:
  std::shared_ptr<CiderPlanProvider> planProvider_;
  std::shared_ptr<CiderSchemaProvider> schemaProvider_;
  bool isCompiled_ = false;
};

#endif  // OMNISCI_CIDERCOMPILEMODULE_H
