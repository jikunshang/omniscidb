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

#ifndef OMNISCI_CIDERDATAPROVIDER_H
#define OMNISCI_CIDERDATAPROVIDER_H

#include "DataConvertorInterface.h"

#include <memory>

// This class may extend DataProvider class(if have) and override
class CiderDataProvider {
 public:
  CiderDataProvider(std::shared_ptr<DataConvertorInterface> dataConvertor)
      : dataConvertor_(dataConvertor) {}

  // could be override method
  void fetchBuffer(const ChunkKey& key,
                   AbstractBuffer* destBuffer,
                   const size_t numBytes = 0){};

  //
  void getData(int8_t** output) {
    dataConvertor_->getNextBatchData(output);
  }

 private:
  std::shared_ptr<DataConvertorInterface> dataConvertor_;
};

#endif  // OMNISCI_CIDERDATAPROVIDER_H
