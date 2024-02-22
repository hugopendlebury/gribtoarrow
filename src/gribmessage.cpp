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
#include "exceptions/gribexception.hpp"



using namespace std;


    GribMessage::GribMessage(GribReader* reader, 
                            codes_handle* codes_handle,
                             long message_id) : 
                                        _reader(reader), 
                                        h(codes_handle),
                                        _message_id(message_id) 
                                        { 
        //auto gd = this->getGridDefinitionTemplateNumber();
       // std::cout << "Template number is " << gd << std::endl;
        //gridDefinitionTemplateNumber = 10l;
        //gridDefinitionTemplateNumber = this->getGridDefinitionTemplateNumber();
    }

    long GribMessage::getGribMessageId() {
        return _message_id;
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

    double GribMessage::getStandardisedLongitudeOfFirstPoint() {
        //TODO UNDERSTAND MORE ABOUT HOW THIS CHANGES
        auto longitude = this->getLongitudeOfFirstPoint();
        if (longitude == 0) {
            longitude -= 180;
        }

        return longitude;
    }
    double GribMessage::getStandardisedLongitudeOfLastPoint() {
        //TODO UNDERSTAND MORE ABOUT HOW THIS CHANGES
        auto longitude = this->getLongitudeOfLastPoint();
        if (longitude == 359.5) {
            longitude -= 180;
        }

        return longitude;
    }

    string GribMessage::getShortName() { 
        return getStringParameter("shortName");
    }

    string GribMessage::getDate() { 
        return getStringParameter("date");
    }

    string GribMessage::getDataType() {
        return getStringParameter("dataType");
    }

    chrono::system_clock::time_point GribMessage::getChronoDate() {
        tm tm = {};
        string date = to_string(getDateNumeric());

        //stringstream ss(date);
        strptime(date.c_str(), "%Y%m%d", &tm);
        //ss >> get_time(&tm, "%Y%m%d");
        
        return std::chrono::system_clock::from_time_t(std::mktime(&tm));
    }

    chrono::system_clock::time_point GribMessage::getObsDate() {
        auto dt = getChronoDate();
        //TODO - this is flakey but follows current logic
        //amend to use the stepUnit and not assume all steps are unit "h"
        auto hours = getStep();
        dt += std::chrono::hours(hours);
        return dt;
        
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
        //Had some issues with this key missing 
        //maybe it isn't mandatory so use this method
        return getNumericParameterOrDefault("number", 0l);
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

    long GribMessage::getEditionNumber() { 
        return getNumericParameter("editionNumber");
    }

    long GribMessage::getNumberOfPoints() {
        return getNumericParameter("numberOfPoints");
    }

    long GribMessage::getGridDefinitionTemplateNumber() {
        return getNumericParameterOrDefault("gridDefinitionTemplateNumber", -1);
    }

    bool GribMessage::iScansNegatively() {
        return (int)getNumericParameter("iScansNegatively") == 1 ;
    }
    
    bool GribMessage::jScansPositively() {
        return (int)getNumericParameter("jScansPositively") == 1 ;
    }

    std::shared_ptr<arrow::Table> GribMessage::getData() {

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

        auto valuesArray = doubleFieldToArrow(numberOfPoints, values, false);
        auto latsArray = doubleFieldToArrow(numberOfPoints, lats, false);
        auto lonsArray = doubleFieldToArrow(numberOfPoints, lons, false);

        std::shared_ptr<arrow::Field> field_lats, field_lons, field_values;
        std::shared_ptr<arrow::Schema> schema;

        // Every field needs its name and data type.
        field_lats = arrow::field("Latitudes", arrow::float64());
        field_lons = arrow::field("Longitudes", arrow::float64());
        field_values = arrow::field("Values", arrow::float64());

        // The schema can be built from a vector of fields, and we do so here.
        schema = arrow::schema({field_lats, field_lons, field_values});

        /*
        std::shared_ptr<arrow::RecordBatch> rbatch;
        // The RecordBatch needs the schema, length for columns, which all must match,
        // and the actual data itself.
        rbatch = arrow::RecordBatch::Make(schema, numberOfPoints, {latsArray.ValueOrDie(), 
                                lonsArray.ValueOrDie(), valuesArray.ValueOrDie()});

        */
        //std::cout << rbatch->ToString();

        auto table = arrow::Table::Make(schema, {latsArray.ValueOrDie(), 
                                lonsArray.ValueOrDie(), valuesArray.ValueOrDie()}, numberOfPoints);

        return table;
    }

    GribMessage::~GribMessage() {
        //printf("Destuctor called on handle %p\n", h);
        //codes_grib_nearest_delete()
        codes_handle_delete(h);
    }

 

    string GribMessage::getStringParameter(string parameterName) {
        size_t parameterNameLength;
        auto  parameterNameC = parameterName.c_str();
        auto err = codes_get_length(h, parameterNameC, &parameterNameLength);
        if(err !=0 ) {
            std::ostringstream oss;
            oss << "Error calling codes_get_length whilst trying to access string key with name " <<
             parameterName << "got error code " << err << " whilst processing message id " << _message_id
             << " whilst processing file " << _reader->getFilePath();

            throw GribException (oss.str());
        }
        auto short_name = new char[parameterNameLength];
        err = codes_get_string(h, parameterNameC, short_name, &parameterNameLength);
        if(err !=0 ) {
            std::ostringstream oss;
            oss << "Error calling codes_get_string whilst trying to access string key with name " <<
             parameterName << "got error code " << err << " whilst processing message id " << _message_id
             << " whilst processing file " << _reader->getFilePath();

            throw GribException (oss.str());
        }
        string short_name_std = short_name;
        delete [] short_name;
        return short_name_std;
    }

    long GribMessage::getNumericParameter(string parameterName) {
        long parameterId;
        auto err = codes_get_long(h, parameterName.c_str(), &parameterId);
        if(err !=0 ) {
            std::ostringstream oss;
            oss << "Error calling codes_get_double got error code " << err
             << " Whilst trying to get key" << parameterName << " in message id " << _message_id
             << " whilst processing file " << _reader->getFilePath();

            throw GribException (oss.str());
        }
        return parameterId;
    }

    long GribMessage::getNumericParameterOrDefault(string parameterName, long defaultValue) {
        long parameterId = defaultValue;
        auto ret = codes_get_long(h, parameterName.c_str(), &parameterId);   
        return ret == 0 ? parameterId : defaultValue;
    }

    double GribMessage::getDoubleParameter(string parameterName) {
        double parameterId;
        auto err = codes_get_double(h, parameterName.c_str(), &parameterId);
        if(err !=0 ) {
            std::ostringstream oss;
            oss << "Error calling codes_get_double got error code " << err
            << "Whilst trying to get key" << parameterName << " in message id " << _message_id
             << " whilst processing file " << _reader->getFilePath();

            throw GribException (oss.str());
        }
        return parameterId;
    }

    unique_ptr<GridArea> GribMessage::getGridArea() {
        //Note here we are using a standardised version of longitude
        auto lat1 = getLatitudeOfFirstPoint();
        auto lon1 = getStandardisedLongitudeOfFirstPoint();
        auto lat2 = getLatitudeOfLastPoint();
        auto lon2 = getStandardisedLongitudeOfLastPoint();
        auto iDirection = iScansNegatively();
        auto jDirection = jScansPositively();

        return std::unique_ptr<GridArea>  (new GridArea(lat1, lon1, lat2, lon2, iDirection, jDirection));
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

        auto cache_results = _reader->getLocationDataFromCache(gridArea);

        if(cache_results.has_value()) {
            return cache_results.value();
        } 
        else {
            auto locations_shared = _reader->getLocations(gridArea);
            auto locations = locations_shared.get();
            //Ok we have an arrow table - get the pointers
            auto lats = locations->GetColumnByName("lat");
            auto lats_vector = colToVector(lats);
            auto lons = locations->GetColumnByName("lon");
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

             
            

            grib_nearest_find_multiple(h,1, inlats, inlons, 
                                    numberOfPoints, outlats, outlons, outvalues, distances, indexes);

            //std::cout << "status of grib_find_nearest_multiple is " << status << std::endl;

            auto latsArray = doubleFieldToArrow(numberOfPoints, inlats, false);
            auto lonsArray = doubleFieldToArrow(numberOfPoints, inlons, false);
            auto outvaluesArray = doubleFieldToArrow(numberOfPoints, outvalues, true);
            auto distanceArray = doubleFieldToArrow(numberOfPoints, distances, false);
            auto outlatsArray = doubleFieldToArrow(numberOfPoints, outlats, false);
            auto outlonsArray = doubleFieldToArrow(numberOfPoints, outlons, false);

            auto cache_data = new GribLocationData(numberOfPoints, 
                                                    indexes,
                                                    latsArray,
                                                    lonsArray,
                                                    distanceArray,
                                                    outlatsArray,
                                                    outlonsArray,
                                                    locations_shared.get()->CombineChunksToBatch().ValueOrDie());

            auto result = _reader->addLocationDataToCache(gridArea, cache_data);

            printf("set values in cache with numberOfPoints = %ld", numberOfPoints);

            return result;

        }
    }


   std::shared_ptr<arrow::Table> GribMessage::getDataWithLocations() {

        if (_reader->hasLocations()) {

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

            auto valuesArray = doubleFieldToArrow(numberOfPoints, doubleValues, true);

            //apply any conversions
            auto parameterId = getParameterId();
            auto conversionFunc = _reader->getConversions(parameterId);

            if(conversionFunc.has_value()) {
                auto func = conversionFunc.value();
                //std::cout << "PRE CONV" << valuesArray.ValueOrDie()->ToString();
                valuesArray = func(valuesArray.ValueOrDie());
                //std::cout << "POST CONV" << valuesArray.ValueOrDie()->ToString();
            }

            //Add all the fields from our lookup table first
            arrow::FieldVector fields;
            
            auto locationFieldsVector = location_data->tableData.get()->schema().get()->fields();
            for(auto f : locationFieldsVector) {
                auto field = f.get();
                fields.push_back(arrow::field(field->name(), field->type()));
            }
        
            fields.push_back(arrow::field("parameterId", arrow::int32()));
            fields.push_back(arrow::field("modelNo", arrow::uint8()));
            fields.push_back(arrow::field("forecast_date", arrow::timestamp(arrow::TimeUnit::SECOND)));
            fields.push_back(arrow::field("datetime", arrow::timestamp(arrow::TimeUnit::SECOND)));
            //Now add the data from the lookups and grib data.
            fields.push_back(arrow::field("distance", arrow::float64()));
            fields.push_back(arrow::field("nearestlatitude", arrow::float64()));
            fields.push_back(arrow::field("nearestlongitude", arrow::float64()));
            fields.push_back(arrow::field("value", arrow::float64()));

            auto schema = arrow::schema(fields);

            std::vector<std::shared_ptr<arrow::Array>> resultsArray;

            
            for (auto column : location_data->tableData.get()->columns()) {
                resultsArray.push_back(column);
            }

            resultsArray.push_back(fieldToArrow(numberOfPoints, (u_int32_t)parameterId).ValueOrDie());
            resultsArray.push_back(fieldToArrow(numberOfPoints, (u_int8_t) getModelNumber()).ValueOrDie());
            resultsArray.push_back(fieldToArrow(numberOfPoints, getChronoDate()).ValueOrDie());
            resultsArray.push_back(fieldToArrow(numberOfPoints, getObsDate()).ValueOrDie());
            resultsArray.push_back(location_data->distanceArray.ValueOrDie());
            resultsArray.push_back(location_data->outlatsArray.ValueOrDie());
            resultsArray.push_back(location_data->outlonsArray.ValueOrDie());
            resultsArray.push_back(valuesArray.ValueOrDie());

            //auto rbatch = arrow::RecordBatch::Make(schema, numberOfPoints, resultsArrayNew);

            auto table = arrow::Table::Make(schema, resultsArray, numberOfPoints);


            //std::cout << rbatch->ToString();

            return table;

        }
    }
