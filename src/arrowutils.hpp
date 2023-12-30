#ifndef ARROW_UTILS_INCLUDED
#define ARROW_UTILS_INCLUDED

#include <arrow/api.h>

struct data_row {
  int64_t parameterId;
  std::optional<double> additionValue;
  std::optional<double> subtractionValue;
  std::optional<double> multiplicationValue;
  std::optional<double> divisionValue;
  std::optional<double> ceilingValue;
};

arrow::Result<std::vector<data_row>> ColumnarTableToVector(
    const std::shared_ptr<arrow::Table>& table);


arrow::Result<std::shared_ptr<arrow::Array>> fieldToArrow(long numberOfPoints, 
        double *fieldValues, 
        bool replaceMissingWithNull);


#endif /* ARROW_UTILS_INCLUDED */