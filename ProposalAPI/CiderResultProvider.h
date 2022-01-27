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

#ifndef OMNISCI_CIDERRESULTPROVIDER_H
#define OMNISCI_CIDERRESULTPROVIDER_H

// TODO: this class should rename to something like notifier
class CiderResultProvider {
 public:
  CiderResultProvider(std::shared_ptr<CiderOutputConvertor> outputConvertor)
      : outputConvertor_(outputConvertor) {}

  void updateResult(CiderTable result) { outputConvertor_->updateResult(result); }

 private:
  std::shared_ptr<CiderOutputConvertor> outputConvertor_;
};
#endif  // OMNISCI_CIDERRESULTPROVIDER_H
