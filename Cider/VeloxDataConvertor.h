//
// Created by sparkuser on 1/24/22.
//

#ifndef OMNISCI_VELOXDATACONVERTOR_H
#define OMNISCI_VELOXDATACONVERTOR_H

#include "DataConvertorInterface.h"
class VeloxDataConvertor : public DataConvertorInterface {
 public:
  void getNextBatchData(int8_t* output) override {
    doConversion(output);
  }

  void updateData(RowVectorPtr input) {
    data_ = std::move(input)
  }

 private:
  RowVectorPtr data_;
  void doConversion(int8_t** output) {
    // data_ -> output
  }
};


#endif  // OMNISCI_VELOXDATACONVERTOR_H
