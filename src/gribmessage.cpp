#include "eccodes.h"
#include <utility>
#include <memory>
#include <iostream>
#include <chrono>
#include <sstream>
#include <time.h>
#include <arrow/api.h>
#include <arrow/python/pyarrow.h>
#include <Python.h>
#include "caster.hpp"
#include "gribmessage.hpp"
#include "arrowutils.hpp"



using namespace std;


    GribMessage::GribMessage(GribReader* reader, 
                            codes_handle* codes_handle,
                             long message_id) : 
                                        reader(reader), 
                                                h(codes_handle),
                                                message_id(message_id) { }

    long GribMessage::getGribMessageId() {
        return message_id;
    }

    string GribMessage::getCodesHandleAddress() {
        std::ostringstream address;
        address << (void const *)h;
        string name = address.str();
        return name;
    }

    string GribMessage::getObjectAddress() {
        std::ostringstream address;
        address << (void const *)this;
        string name = address.str();
        return name;
    }

    double GribMessage::getLatitudeOfFirstPoint() {
        return getDoubleParameter("latitudeOfFirstGridPointInDegrees");
    }

    double GribMessage::getLongitudeOfFirstPoint() {
        return getDoubleParameter("longitudeOfFirstGridPointInDegrees");
    }

    double GribMessage::getLatitudeOfLastPoint() {
        return getDoubleParameter("latitudeOfLastGridPointInDegrees");
    }

    double GribMessage::getLongitudeOfLastPoint() {
        return getDoubleParameter("longitudeOfLastGridPointInDegrees");
    }

    string GribMessage::getShortName() { 
        return getStringParameter("shortName");
    }

    string GribMessage::getDate() { 
        return getStringParameter("date");
    }

    chrono::system_clock::time_point GribMessage::getChronoDate() {
        tm tm = {};
        string date = to_string(getDateNumeric());

        //stringstream ss(date);
        strptime(date.c_str(), "%Y%m%d", &tm);
        //ss >> get_time(&tm, "%Y%m%d");
        return std::chrono::system_clock::from_time_t(std::mktime(&tm));
    }

    string GribMessage::getTime() { 
        return getStringParameter("time");
    }

    long GribMessage::getDateNumeric() { 
        return getNumericParameter("date");
    }

    long GribMessage::getTimeNumeric() { 
        return getNumericParameter("time");
    }

    long GribMessage::getParameterId() {
        return getNumericParameter("paramId");
    }

    long GribMessage::getModelNumber() {
        return getNumericParameter("number");
    }

    long GribMessage::getStep() {
        return getNumericParameter("step");
    }

    string GribMessage::getStepUnits() {
        return getStringParameter("stepUnits");
    }

    long GribMessage::getStepRange() { 
        return getNumericParameter("stepRange");
    }

    long GribMessage::getHourOffset() {
        return getStep() * getStepRange();
    }

    long GribMessage::getNumberOfPoints() {
        return getNumericParameter("numberOfPoints");
    }

    std::shared_ptr<arrow::RecordBatch> GribMessage::getData() {

        double *lats, *lons, *values; /* arrays */
        long numberOfPoints = getNumberOfPoints();

        lats = (double*)malloc(numberOfPoints * sizeof(double));
        if (!lats) {
            fprintf(stderr, "Error: unable to allocate %ld bytes\n", (long)(numberOfPoints * sizeof(double)));
        }
        lons = (double*)malloc(numberOfPoints * sizeof(double));
        if (!lons) {
            fprintf(stderr, "Error: unable to allocate %ld bytes\n", (long)(numberOfPoints * sizeof(double)));
            free(lats);
        }
        values = (double*)malloc(numberOfPoints * sizeof(double));
        if (!values) {
            fprintf(stderr, "Error: unable to allocate %ld bytes\n", (long)(numberOfPoints * sizeof(double)));
            free(lats);
            free(lons);
            //return 1;
        }
        
        CODES_CHECK(codes_grib_get_data(h, lats, lons, values), 0);

        auto valuesArray = fieldToArrow(numberOfPoints, values, false);
        auto latsArray = fieldToArrow(numberOfPoints, lats, false);
        auto lonsArray = fieldToArrow(numberOfPoints, lons, false);

        std::shared_ptr<arrow::Field> field_lats, field_lons, field_values;
        std::shared_ptr<arrow::Schema> schema;

        // Every field needs its name and data type.
        field_lats = arrow::field("Latitudes", arrow::float64());
        field_lons = arrow::field("Longitudes", arrow::float64());
        field_values = arrow::field("Values", arrow::float64());

        // The schema can be built from a vector of fields, and we do so here.
        schema = arrow::schema({field_lats, field_lons, field_values});
        std::shared_ptr<arrow::RecordBatch> rbatch;
        // The RecordBatch needs the schema, length for columns, which all must match,
        // and the actual data itself.
        rbatch = arrow::RecordBatch::Make(schema, numberOfPoints, {latsArray.ValueOrDie(), 
                                lonsArray.ValueOrDie(), valuesArray.ValueOrDie()});

        //std::cout << rbatch->ToString();

        return rbatch;
    }

    GribMessage::~GribMessage() {
        //printf("Destuctor called on handle %p\n", h);
        codes_handle_delete(h);
    }

 

    string GribMessage::getStringParameter(string parameterName) {
        size_t parameterNameLength;
        auto  parameterNameC = parameterName.c_str();
        codes_get_length(h, parameterNameC, &parameterNameLength);
        auto short_name = new char[parameterNameLength];
        codes_get_string(h, parameterNameC, short_name, &parameterNameLength);
        string short_name_std = short_name;
        delete [] short_name;
        return short_name_std;
    }

    long GribMessage::getNumericParameter(string parameterName) {
        long parameterId;
        codes_get_long(h, parameterName.c_str(), &parameterId);
        return parameterId;
    }

    double GribMessage::getDoubleParameter(string parameterName) {
        double parameterId;
        codes_get_double(h, parameterName.c_str(), &parameterId);
        return parameterId;
    }

    unique_ptr<GridArea> GribMessage::getGridArea() {
        auto lat1 = getLatitudeOfFirstPoint();
        auto lon1 = getLongitudeOfFirstPoint();
        auto lat2 = getLatitudeOfLastPoint();
        auto lon2 = getLongitudeOfLastPoint();
        //std::unique_ptr<LargeObject> pLarge(new LargeObject());
        return std::unique_ptr<GridArea>  (new GridArea(lat1, lon1, lat2, lon2));
    }

    std::vector<double> GribMessage::colToVector(std::shared_ptr<arrow::ChunkedArray> columnArray) {
        std::vector<double> double_vector;
        auto array = columnArray.get();
        for(int i=0; i < array->num_chunks(); i++) {
            auto chunk = array->chunk(i);
            auto arrow_double_array = std::static_pointer_cast<arrow::DoubleArray>(chunk);

            for (int64_t j = 0; j < array->length(); ++j) 
            {
                double_vector.push_back(arrow_double_array->Value(j));
            }
        }
        return double_vector;

    }

    GribLocationData* GribMessage::getLocationData(std::unique_ptr<GridArea> gridArea) {

        auto cache_results = reader->getLocationDataFromCache(gridArea);

        if(cache_results.has_value()) {
            return cache_results.value();
        } 
        else {
            auto stations = reader->getStations(gridArea);
            //Ok we have an arrow table - get the pointers
            auto lats = stations->GetColumnByName("lat");
            auto lats_vector = colToVector(lats);
            auto lons = stations->GetColumnByName("lon");
            auto lons_vector = colToVector(lons);

            double* inlats = &lats_vector[0];
            double* inlons = &lons_vector[0];

            long numberOfPoints = lats_vector.size();
            double *outlats, *outlons, *outvalues, *distances;
            int *indexes; /* arrays */

            outlats = (double*)malloc(numberOfPoints * sizeof(double));
            if (!outlats) {
                fprintf(stderr, "Error: unable to allocate %ld bytes\n", (long)(numberOfPoints * sizeof(double)));
            }
            outlons = (double*)malloc(numberOfPoints * sizeof(double));
            if (!outlons) {
                fprintf(stderr, "Error: unable to allocate %ld bytes\n", (long)(numberOfPoints * sizeof(double)));
                free(outlats);
            }
            outvalues = (double*)malloc(numberOfPoints * sizeof(double));
            if (!outvalues) {
                fprintf(stderr, "Error: unable to allocate %ld bytes\n", (long)(numberOfPoints * sizeof(double)));
                free(outlats);
                free(outlons);
            }
            distances = (double*)malloc(numberOfPoints * sizeof(double));
            if (!distances) {
                fprintf(stderr, "Error: unable to allocate %ld bytes\n", (long)(numberOfPoints * sizeof(double)));
                free(outlats);
                free(outlons);
                free(outvalues);
            }
            indexes = (int*)malloc(numberOfPoints * sizeof(int));
            if (!indexes) {
                fprintf(stderr, "Error: unable to allocate %ld bytes\n", (long)(numberOfPoints * sizeof(int)));
                free(outlats);
                free(outlons);
                free(outvalues);
                free(distances);
            }

            grib_nearest_find_multiple(h,0, inlats, inlons, 
                                    numberOfPoints, outlats, outlons, outvalues, distances, indexes);

            auto latsArray = fieldToArrow(numberOfPoints, inlats, false);
            auto lonsArray = fieldToArrow(numberOfPoints, inlons, false);
            auto outvaluesArray = fieldToArrow(numberOfPoints, outvalues, true);
            auto distanceArray = fieldToArrow(numberOfPoints, distances, false);
            auto outlatsArray = fieldToArrow(numberOfPoints, outlats, false);
            auto outlonsArray = fieldToArrow(numberOfPoints, outlons, false);

            auto cache_data = new GribLocationData(numberOfPoints, 
                                                    indexes,
                                                    latsArray,
                                                    lonsArray,
                                                    distanceArray,
                                                    outlatsArray,
                                                    outlonsArray);

            auto result = reader->addLocationDataToCache(gridArea, cache_data);

            printf("set values in cache with numberOfPoints = %ld", numberOfPoints);

            return result;

        }
    }


   std::shared_ptr<arrow::RecordBatch> GribMessage::getDataWithStations() {

        if (reader->hasStations()) {

            auto gridArea = getGridArea();

            auto location_data = getLocationData(std::move(gridArea));
            long numberOfPoints = location_data->numberOfPoints;
            auto indexes = location_data->indexes.get();
            double *doubleValues;

            doubleValues = (double*)malloc(numberOfPoints * sizeof(double));
            if (!doubleValues) {
                //fprintf(stderr, "Error: unable to allocate %ld bytes\n", (long)(numberOfPoints * sizeof(double)));
            }

            codes_get_double_elements(h, "values", indexes, numberOfPoints, doubleValues);

            auto valuesArray = fieldToArrow(numberOfPoints, doubleValues, true);

            //apply any conversion
            auto parameterId = getParameterId();
            auto conversionFunc = reader->getConversions(parameterId);

            if(conversionFunc.has_value()) {
                auto func = conversionFunc.value();
                std::cout << "PRE CONV" << valuesArray.ValueOrDie()->ToString();
                valuesArray = func(valuesArray.ValueOrDie());
                std::cout << "POST CONV" << valuesArray.ValueOrDie()->ToString();
            }

            auto schema = arrow::schema({
                arrow::field("latitude", arrow::float64()),
                arrow::field("longitude", arrow::float64()),
                arrow::field("distance", arrow::float64()),
                arrow::field("outlatitude", arrow::float64()),
                arrow::field("outlongitude", arrow::float64()),
                arrow::field("value", arrow::float64())
            });
            
            auto rbatch = arrow::RecordBatch::Make(schema, numberOfPoints, 
                                    {
                                        location_data->latsArray.ValueOrDie(),
                                        location_data->lonsArray.ValueOrDie(),
                                        location_data->distanceArray.ValueOrDie(),
                                        location_data->outlatsArray.ValueOrDie(),
                                        location_data->outlonsArray.ValueOrDie(),
                                        valuesArray.ValueOrDie(),
                                    });

            //std::cout << rbatch->ToString();

            return rbatch;

        }
    }
