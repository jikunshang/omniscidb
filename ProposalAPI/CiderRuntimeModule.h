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

#ifndef OMNISCI_CIDERRUNTIMEMODULE_H
#define OMNISCI_CIDERRUNTIMEMODULE_H

#include "CiderCompileModule.h"

// Runtime API
// should have a compileModule and input data, produce output data

class CiderRuntimeModule {
 public:
  // could use an existing compile module
  CiderRuntimeModule(std::shared_ptr<CiderCompileModule> compileModule,
                     std::shared_ptr<CiderDataProvider> dataProvider,
                     std::shared_ptr<CiderResultConvertor> resultConvertor)
      : compileModule_(compileModule)
      , dataProvider_(dataProvider)
      , resultConvertor_(resultConvertor){};

  // also could help user do compilation
  CiderRuntimeModule(std::shared_ptr<CiderPlanProvider> planProvider,
                     std::shared_ptr<CiderSchemaProvider> schemaProvider,
                     std::shared_ptr<CiderDataProvider> dataProvider,
                     std::shared_ptr<CiderResultProvider> resultProvider)
      : compileModule_(planProvider, schemaProvider)
      , dataProvider_(dataProvider)
      , resultProvider_(resultProvider){};

  CiderRuntimeModule(std::shared_ptr<CiderCompileModule> compileModule,
                     std::shared_ptr<CiderInputConvertor> inputConvertor,
                     std::shared_ptr<CiderOutputConvertor> outputConvertor)
      : compileModule_(compileModule)
      , dataProvider_(inputConvertor)
      , resultProvider_(inputConvertor) {}

  void processNextBatch() {
    if (!compileModule_->isCompiled()) {
      compileModule_->compile();
    }
    auto input = dataProvider_->nextBatch();
    auto function = compileModule_->getFunction();
    auto result = function->run(input);
    resultProvider_->updateResult(result);
  }

  // placeholder, do some clean up work
  void finish();

 private:
  std::shared_ptr<CiderCompileModule> compileModule_;
  std::shared_ptr<CiderDataProvider> dataProvider_;
  std::shared_ptr<CiderResultProvider> resultProvider_;
};
#endif  // OMNISCI_CIDERRUNTIMEMODULE_H
