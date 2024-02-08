#include <arrow/api.h>
#include <cstdint>
#include <iomanip>
#include <chrono>
#include <sstream>
#include <time.h>
#include <iostream>
#include <vector>
#include "gribhelpers.hpp"

std::unordered_map<std::string, std::shared_ptr<arrow::DataType>>  getConversionFieldDefinitions() {

    std::unordered_map<std::string, std::shared_ptr<arrow::DataType>> fieldTypes;
    fieldTypes.emplace(make_pair("parameterId", arrow::int64()));
    fieldTypes.emplace(make_pair("addition_value", arrow::float64()));
    fieldTypes.emplace(make_pair("subtraction_value", arrow::float64()));
    fieldTypes.emplace(make_pair("multiplication_value", arrow::float64()));
    fieldTypes.emplace(make_pair("division_value", arrow::float64()));
    fieldTypes.emplace(make_pair("ceiling_value", arrow::float64()));

    return fieldTypes;

}