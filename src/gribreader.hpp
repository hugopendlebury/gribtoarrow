#pragma once

#include <iterator>
#include <memory>
#include <utility>
#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/python/pyarrow.h>
#include "gridarea.hpp"
#include "gribmessageiterator.hpp"
#include "caster.hpp"
#include "griblocationdata.hpp"



using namespace std;

class GribLocationData;

class Converter;

class GribReader 
{

public:

    FILE* fin           = NULL;


    GribReader(string filepath);

    GribReader withLocations(std::shared_ptr<arrow::Table> locations);
    GribReader withLocations(std::string path);
    GribReader withConversions(std::shared_ptr<arrow::Table> conversions);
    GribReader withConversions(std::string path);
    GribReader withRepeatableIterator(bool repeatable);

    Iterator begin();
    Iterator end();

    //TODO Refactor this to use optional
    bool hasLocations();
    std::shared_ptr<arrow::Table> getLocations(std::unique_ptr<GridArea>& area);

    std::optional<std::function<arrow::Result<std::shared_ptr<arrow::Array>>(std::shared_ptr<arrow::Array>)>> getConversions(long parameterId);

    std::optional<GribLocationData*> getLocationDataFromCache(std::unique_ptr<GridArea>& area);
    GribLocationData* addLocationDataToCache(std::unique_ptr<GridArea>& area, GribLocationData* locationData);

    private:
        string filepath;
        int err             = 0;
        bool isRepeatable = false;
        bool isExhausted  = false;
        std::shared_ptr<arrow::Table> shared_locations;
        std::unordered_map<GridArea, std::shared_ptr<arrow::Table>> locations_in_area;
        std::unordered_map<GridArea, GribLocationData*> location_cache;
        std::unordered_map<int64_t, Converter*> conversion_funcs;
        GribMessage*        m_endMessage;
        std::shared_ptr<arrow::Table> getTableFromCsv(std::string path, arrow::csv::ConvertOptions convertOptions);
        arrow::Result<std::shared_ptr<arrow::Array>> createSurrogateKeyCol(long numberOfRows);
        void validateConversionFields(std::shared_ptr<arrow::Table> conversions, std::string table_name);
        std::shared_ptr<arrow::Table> castTableFields(std::shared_ptr<arrow::Table> arrow_table,
                                                             std::string table_name,
                                                             std::unordered_map<std::string, std::shared_ptr<arrow::DataType>> fieldTypes);
        void validateLocationFields(std::shared_ptr<arrow::Table> locations, std::string table_name) ;
        std::shared_ptr<arrow::Table> enrichLocationsWithSurrogateKey(std::shared_ptr<arrow::Table> locations) ;
        arrow::ArrayVector* castColumn(std::shared_ptr<arrow::Table> locations, 
                            std::string colName,
                            std::shared_ptr<arrow::DataType> fieldType) ;


};