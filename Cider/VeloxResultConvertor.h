//
// Created by sparkuser on 1/24/22.
//

#ifndef OMNISCI_VELOXRESULTCONVERTOR_H
#define OMNISCI_VELOXRESULTCONVERTOR_H

#include ""

class VeloxResultConvertor : public CiderResultConvertor {
 public:
  RowVectorPtr getRowVectorPtr() {
    return doCovnersion(result_);
  }
 private:
  RowVectorPtr doCovnersion(ExecutionResult);
};

#endif  // OMNISCI_VELOXRESULTCONVERTOR_H
