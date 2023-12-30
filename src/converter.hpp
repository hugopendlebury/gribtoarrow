#ifndef CONVERTER_INCLUDED
#define CONVERTER_INCLUDED

#include <arrow/api.h>

class Converter
{

    
public:
    double conversionValue;
    std::function<arrow::Result<arrow::Datum>(arrow::Datum, arrow::Datum)> conversionFunc;

    Converter(std::function<arrow::Result<arrow::Datum>(arrow::Datum, arrow::Datum)> conversionFunc, double conversionValue);
 
    // This operator overloading enables calling
    // operator function () on objects of increment
    arrow::Result<std::shared_ptr<arrow::Array>> operator () (std::shared_ptr<arrow::Array> valuesArray);
};

#endif /* CONVERTER_INCLUDED */