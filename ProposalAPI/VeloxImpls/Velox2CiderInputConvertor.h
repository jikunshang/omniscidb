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

#ifndef OMNISCI_VELOXDATACONVERTOR_H
#define OMNISCI_VELOXDATACONVERTOR_H

#include "ProposalAPI/CiderInputConvertor.h"
class Velox2CiderInputConvertor : public CiderInputConvertor {
 public:
  CiderTable getNextBatch() override { return ciderTable_; }

  void addInput(RowVectorPtr input) {
    // concern: Ownership?
    data_ = std::move(input);
    ciderTable_ = doConversion(data_);
  }

 private:
  RowVectorPtr data_;
  CiderTable doConversion(data_) {
    // data_ -> output
  }
  CiderTable ciderTable_;
};

#endif  // OMNISCI_VELOXDATACONVERTOR_H
