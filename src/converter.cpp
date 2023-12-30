#include "converter.hpp"
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <iostream>

using namespace std;

Converter::Converter(std::function<arrow::Result<arrow::Datum>(arrow::Datum, arrow::Datum)> conversionFunc,
                    double conversionValue) : conversionFunc(conversionFunc), conversionValue(conversionValue) { }

arrow::Result<std::shared_ptr<arrow::Array>> Converter::operator () (std::shared_ptr<arrow::Array> valuesArray) {

    shared_ptr<arrow::Scalar> operand(new arrow::DoubleScalar(conversionValue));
    arrow::Datum datum;

    ARROW_ASSIGN_OR_RAISE(datum,
             conversionFunc(valuesArray, operand));

    std::shared_ptr<arrow::Array> new_values = std::move(datum).make_array();

    return new_values;
            
}