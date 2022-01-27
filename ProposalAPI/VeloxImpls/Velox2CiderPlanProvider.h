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

#ifndef OMNISCI_VELOXPLANPROVIDER_H
#define OMNISCI_VELOXPLANPROVIDER_H

class Velox2CiderPlanProvider : public CiderPlanProvider {
 public:
  VeloxPlanProvider(std::shared_ptr<velox::core::PlanNode> planNode)
      : planNode_(planNode) {}
  SubStraitPlanNode getSubStraitPlan() override { return convert(planNode_) }

 private:
  SubStraitPlanNode convert(std::shared_ptr<velox::core::PlanNode> planNode);
  std::shared_ptr<velox::core::PlanNode> planNode_;
};
#endif  // OMNISCI_VELOXPLANPROVIDER_H
