#include <arrow/api.h>
#include <arrow/result.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/dataset/file_ipc.h>
#include <arrow/compute/expression.h>
#include <cstdint>
#include <iomanip>
#include <chrono>
#include <sstream>
#include <time.h>
#include <iostream>
#include <vector>
#include "arrowutils.hpp"

namespace cp = arrow::compute;

using arrow::DoubleBuilder;
using arrow::Int64Builder;
using arrow::ListBuilder;

// While we want to use columnar data structures to build efficient operations, we
// often receive data in a row-wise fashion from other systems. In the following,
// we want give a brief introduction into the classes provided by Apache Arrow by
// showing how to transform row-wise data into a columnar table.
//
// The table contains an id for a product, the number of components in the product
// and the cost of each component.
//
// The data in this example is stored in the following struct:



arrow::Result<std::vector<data_row>> ColumnarTableToVector(
    const std::shared_ptr<arrow::Table>& table) {

    //Converts the table to a row wise representation
    //Note we have already checked the table column names and cast and data types
    //so the schema should be fine

    //TODO - should we really be just using chunk(0) ?

  auto parameterIds = std::static_pointer_cast<arrow::Int64Array>(table->GetColumnByName("parameterId")->chunk(0));
  auto additionValues =
      std::static_pointer_cast<arrow::DoubleArray>(table->GetColumnByName("addition_value")->chunk(0));
  auto subtractionValues =
      std::static_pointer_cast<arrow::DoubleArray>(table->GetColumnByName("subtraction_value")->chunk(0));
  auto multiplicationValues =
      std::static_pointer_cast<arrow::DoubleArray>(table->GetColumnByName("multiplication_value")->chunk(0));
  auto divisionValues =
      std::static_pointer_cast<arrow::DoubleArray>(table->GetColumnByName("division_value")->chunk(0));
  auto ceilingValues =
      std::static_pointer_cast<arrow::DoubleArray>(table->GetColumnByName("ceiling_value")->chunk(0));

  std::vector<data_row> rows;
  for (int64_t i = 0; i < table->num_rows(); i++) {
    // Another simplification in this example is that we assume that there are
    // no null entries, e.g. each row is fill with valid values.
    int64_t parameterId = parameterIds->Value(i);
    auto additionValue = additionValues->IsNull(i) ? std::nullopt : std::optional<double> {additionValues->Value(i)};
    auto subtractionValue = subtractionValues->IsNull(i) ? std::nullopt : std::optional<double> {subtractionValues->Value(i)};
    auto multiplicationValue = multiplicationValues->IsNull(i) ? std::nullopt : std::optional<double> {multiplicationValues->Value(i)};
    auto divisionValue = divisionValues->IsNull(i) ? std::nullopt : std::optional<double> {divisionValues->Value(i)};
    auto ceilingValue = ceilingValues->IsNull(i) ? std::nullopt : std::optional<double> {ceilingValues->Value(i)};

    rows.push_back({parameterId, additionValue, subtractionValue, multiplicationValue,divisionValue, ceilingValue });
  }

  return rows;
}


arrow::Result<std::shared_ptr<arrow::Array>> doubleFieldToArrow(long numberOfPoints, 
            double *fieldValues, 
            bool replaceMissingWithNull) {

    arrow::DoubleBuilder valuesBuilder;

    if (replaceMissingWithNull) {

        uint8_t *nullFlags = (uint8_t*)malloc(numberOfPoints * sizeof(uint8_t));

        for( size_t i = 0 ; i < numberOfPoints ; i++ ) {
            nullFlags[i] = fieldValues[i] != 9999;
        }

        ARROW_RETURN_NOT_OK(valuesBuilder.AppendValues(fieldValues, numberOfPoints, nullFlags));
    } 
    else {
        ARROW_RETURN_NOT_OK(valuesBuilder.AppendValues(fieldValues, numberOfPoints));
    }

    std::shared_ptr<arrow::Array> arrayValues;
    ARROW_ASSIGN_OR_RAISE(arrayValues, valuesBuilder.Finish());
    return arrayValues;

//return arrow::Status::OK();
}  


arrow::Result<std::shared_ptr<arrow::Array>> fieldToArrow(long numberOfPoints, long value ) {


    arrow::UInt64Builder valuesBuilder;

    std::vector<u_int64_t> column_values;
    for(auto i =0 ; i <  numberOfPoints; ++i) {
        column_values.emplace_back(value);
    }

    ARROW_RETURN_NOT_OK(valuesBuilder.AppendValues(column_values));
    std::shared_ptr<arrow::Array> arrayValues;
    ARROW_ASSIGN_OR_RAISE(arrayValues,valuesBuilder.Finish());
    return arrayValues;


//return arrow::Status::OK();
}  

arrow::Result<std::shared_ptr<arrow::Array>> fieldToArrow(long numberOfPoints, u_int32_t value ) {


    arrow::UInt32Builder valuesBuilder;

    std::vector<u_int32_t> column_values;
    for(auto i =0 ; i <  numberOfPoints; ++i) {
        column_values.emplace_back(value);
    }

    ARROW_RETURN_NOT_OK(valuesBuilder.AppendValues(column_values));
    std::shared_ptr<arrow::Array> arrayValues;
    ARROW_ASSIGN_OR_RAISE(arrayValues,valuesBuilder.Finish());
    return arrayValues;


//return arrow::Status::OK();
}  

arrow::Result<std::shared_ptr<arrow::Array>> fieldToArrow(long numberOfPoints, uint8_t value ) {


    arrow::UInt8Builder valuesBuilder;

    std::vector<uint8_t> column_values;
    for(auto i =0 ; i <  numberOfPoints; ++i) {
        column_values.emplace_back(value);
    }

    ARROW_RETURN_NOT_OK(valuesBuilder.AppendValues(column_values));
    std::shared_ptr<arrow::Array> arrayValues;
    ARROW_ASSIGN_OR_RAISE(arrayValues,valuesBuilder.Finish());
    return arrayValues;


//return arrow::Status::OK();
}  

arrow::Result<std::shared_ptr<arrow::Array>> fieldToArrow(long numberOfPoints, std::chrono::system_clock::time_point  value ) {

    auto timeSinceEpoch = (int64_t) std::chrono::duration_cast<std::chrono::microseconds>(value.time_since_epoch()).count() ;

    std::vector<int64_t> column_values;
    for(auto i =0 ; i <  numberOfPoints; ++i) {
        column_values.emplace_back(timeSinceEpoch);
    }

    auto timeType = arrow::timestamp(arrow::TimeUnit::MICRO);
    arrow::TimestampBuilder valuesBuilder(timeType, arrow::default_memory_pool());
    
    ARROW_RETURN_NOT_OK(valuesBuilder.AppendValues(column_values));
    std::shared_ptr<arrow::Array> arrayValues;
    ARROW_ASSIGN_OR_RAISE(arrayValues,valuesBuilder.Finish());
    return arrayValues;

//return arrow::Status::OK();
}