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

int main() {
  velox::DataSource dataSource;
  velox::Result operatorResult;
  std::shared<velox::core::PlanNode> planNode;

  auto planProvider = std::make_shared<Velox2CiderPlanProvider>(planNode);
  auto schemaProvider = std::make_shared<Velox2CiderSchemProvider>();

  // compile module API
  auto ciderCompileModule =
      std::make_shared<CiderCompileModule>(planProvider, schemaProvider);
  ciderCompileModule->compile();
  CHECK(ciderCompileModule->isCompiled());

  auto veloxInputConvertor = std::make_shared<Velox2CiderInputConvertor>();
  auto veloxOutputConvertor = std::make_shared<Cider2VeloxOutputConvertor>();

  // runtime module API
  auto ciderRuntimeModule = std::make_shared<CiderRuntimeModule>(
      ciderCompileModule, veloxInputConvertor, veloxOutputConvertor);

  while (dataSource.hasData) {
    veloxInputConvertor->addInput(dataSource.nextBatch());
    ciderRuntimeModule->processNextBatch();
    velox::VectorPtr batchResult = veloxOutputConvertor->getBatchResult();
    operatorResult.append(batchResult);
  }

  // consumed all input data, for some special/blocking operator, we may need some clean
  // up work
  {
    ciderRuntimModule->finish();
    velox::VectorPtr finalResult veloxOutputConvertor->getFinalResult();
    operatorResult.append(finalResult);
  }

  return 0;
}