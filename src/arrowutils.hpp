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


arrow::Result<std::shared_ptr<arrow::Array>> doubleFieldToArrow(long numberOfPoints, 
        double *fieldValues, 
        bool replaceMissingWithNull);

arrow::Result<std::shared_ptr<arrow::Array>> fieldToArrow(long numberOfPoints, long value );
arrow::Result<std::shared_ptr<arrow::Array>> fieldToArrow(long numberOfPoints, u_int32_t value ) ;
arrow::Result<std::shared_ptr<arrow::Array>> fieldToArrow(long numberOfPoints, uint8_t value );

#endif /* ARROW_UTILS_INCLUDED */