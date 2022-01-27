//
// Created by sparkuser on 1/27/22.
//

#ifndef OMNISCI_CIDERTABLE_H
#define OMNISCI_CIDERTABLE_H

// this class will be data/table format interface
class CiderTable{

 private:
  int32_t columnNum_;
  int32_t rowNum_;
  std::vector<std::string> columnNames_;
  std::vector<type> schema_;
  int8_t** tablePtr_;
};

#endif  // OMNISCI_CIDERTABLE_H
