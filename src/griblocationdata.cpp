#include "griblocationdata.hpp"
#include <arrow/api.h>

        GribLocationData::GribLocationData(long numberOfPoints,
                     int* indexes,
                        arrow::Result<std::shared_ptr<arrow::Array>> latsArray,
                        arrow::Result<std::shared_ptr<arrow::Array>> lonsArray,
                        arrow::Result<std::shared_ptr<arrow::Array>> distanceArray,
                        arrow::Result<std::shared_ptr<arrow::Array>> outlatsArray,
                        arrow::Result<std::shared_ptr<arrow::Array>> outlonsArray,
                        std::shared_ptr<arrow::RecordBatch> tableData) : 
                        numberOfPoints(numberOfPoints), 
                        indexes(indexes),
                        latsArray(latsArray),
                        lonsArray(lonsArray),
                        distanceArray(distanceArray),
                        outlatsArray(outlatsArray),
                        outlonsArray(outlonsArray),
                        tableData(tableData) {}