//
// Created by sparkuser on 1/24/22.
//

#ifndef OMNISCI_CIDERRESULTPROVIDER_H
#define OMNISCI_CIDERRESULTPROVIDER_H

class CiderResultConvertor {
 public:
  void updateResult(ExecutionResult result) {
    result_ = result;
  }

 protected:
  ExecutionResult result_;
};

#endif  // OMNISCI_CIDERRESULTPROVIDER_H
