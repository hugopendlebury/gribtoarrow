#ifndef GRIB_LOCATION_H_INCLUDED
#define GRIB_LOCATION_H_INCLUDED

#include <utility>
#include <memory>
#include <iostream>
#include <chrono>
#include <sstream>
#include <time.h>
#include <arrow/api.h>
#include <arrow/python/pyarrow.h>
#include "eccodes.h"
#include "gribreader.hpp"
#include "caster.hpp"


using namespace std;

class GribLocationData
{

    public:

        long numberOfPoints;
        std::unique_ptr<int> indexes;
        arrow::Result<std::shared_ptr<arrow::Array>> latsArray;
        arrow::Result<std::shared_ptr<arrow::Array>> lonsArray;
        arrow::Result<std::shared_ptr<arrow::Array>> distanceArray;
        arrow::Result<std::shared_ptr<arrow::Array>> outlatsArray;
        arrow::Result<std::shared_ptr<arrow::Array>> outlonsArray;
        std::shared_ptr<arrow::RecordBatch> tableData;


        GribLocationData(long numberOfPoints,
                        int* indexes,
                        arrow::Result<std::shared_ptr<arrow::Array>> latsArray,
                        arrow::Result<std::shared_ptr<arrow::Array>> lonsArray,
                        arrow::Result<std::shared_ptr<arrow::Array>> distanceArray,
                        arrow::Result<std::shared_ptr<arrow::Array>> outlatsArray,
                        arrow::Result<std::shared_ptr<arrow::Array>> outlonsArray,
                        std::shared_ptr<arrow::RecordBatch> tableData);
        

    private:

   
};

#endif /*GRIB_LOCATION_H_INCLUDED*/