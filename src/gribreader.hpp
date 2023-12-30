#ifndef GRIB_READER_H_INCLUDED
#define GRIB_READER_H_INCLUDED

#include <iterator>
#include <memory>
#include <utility>
#include <arrow/api.h>
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

    GribReader withStations(std::shared_ptr<arrow::Table> stations) ;
    GribReader withConversions(std::shared_ptr<arrow::Table> conversions) ;

    Iterator begin();
    Iterator end();

    //TODO Refactor this to use optional
    bool hasStations();
    arrow::Table* getStations(std::unique_ptr<GridArea>& area);

    std::optional<std::function<arrow::Result<std::shared_ptr<arrow::Array>>(std::shared_ptr<arrow::Array>)>> getConversions(long parameterId);

    std::optional<GribLocationData*> getLocationDataFromCache(std::unique_ptr<GridArea>& area);
    GribLocationData* addLocationDataToCache(std::unique_ptr<GridArea>& area, GribLocationData* locationData);

    private:
        string filepath;
        int err             = 0;
        arrow::Table*       stations = NULLPTR;
        arrow::Table*       conversions = NULLPTR;
        std::unordered_map<GridArea, arrow::Table*> stations_in_area;
        std::unordered_map<GridArea, GribLocationData*> location_cache;
        //std::unordered_map<int64_t, std::function<arrow::Result<std::shared_ptr<arrow::Array>>(std::shared_ptr<arrow::Array>)>> conversion_funcs;
        std::unordered_map<int64_t, Converter*> conversion_funcs;
        GribMessage*        m_endMessage;
 



};
#endif /*GRIB_READER_H_INCLUDED*/