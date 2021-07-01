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

#ifndef DATAPROVIDER_H
#define DATAPROVIDER_H

#include <stdint.h>
#include <string>

// emmm, it will be very similar to ChunkKey?
class CiderDataProvider {
 public:
  CiderDataProvider(int32_t table_id, int32_t fragment_id)
      : table_id_(table_id), fragment_id_(fragment_id) {}

  int32_t getTableId() { return table_id_; }
  int32_t getFragmentId() { return fragment_id_; }

 protected:
  int32_t table_id_;
  // int32_t column_id; // we should NOT provide column id info since this will depend on
  // query
  int32_t fragment_id_;
};

class FileCiderDataProvider : public CiderDataProvider {
 public:
  FileCiderDataProvider(int32_t table_id, int32_t fragment_id, std::string file_name)
      : CiderDataProvider(table_id, fragment_id), file_name_(file_name){};
  std::string getFilename() { return file_name_; }

 protected:
  std::string file_name_;
};

#endif