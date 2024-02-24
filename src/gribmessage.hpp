#ifndef GRIB_MESSAGE_H_INCLUDED
#define GRIB_MESSAGE_H_INCLUDED

#include <cmath>
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

class GribReader;
class GridArea;
class GribLocationData;

class GribMessage
{

    public:

        GribMessage(GribReader* reader, 
                    codes_handle* handle, 
                    long message_id);

        long getGribMessageId();
        double getLatitudeOfFirstPoint();
        double getLongitudeOfFirstPoint();
        double getLatitudeOfLastPoint();
        double getLongitudeOfLastPoint();

        //In some instances different coordinate systems appear to be used
        //eg sometime it's -180 / 180 (Mercator)
        //   othertimes it's 0 / 360
        double getStandardisedLongitudeOfFirstPoint();
        double getStandardisedLongitudeOfLastPoint();

        string getShortName();
        string getCodesHandleAddress();
        string getObjectAddress();
        string getDate();
        chrono::system_clock::time_point getChronoDate();
        string getTime();
        chrono::system_clock::time_point getObsDate();
        long getDateNumeric();
        long getTimeNumeric();
        long getParameterId();
        long getModelNumber();
        long getStep();
        string getStepUnits();
        string getDataType();
        long getStepRange();
        long getHourOffset();
        long getEditionNumber();
        long getNumberOfPoints();
        long getGridDefinitionTemplateNumber();
        bool iScansNegatively();
        bool jScansPositively();
        long getNumericParameterOrDefault(string parameterName, long defaultValue = -9999);
        double getDoubleParameterOrDefault(string parameterName, double defaultValue = std::nan(""));
        string getStringParameterOrDefault(string parameterName, string defaultValue = "");

        template <typename T> 
        T tryGetParameter(string parameterName, T defaultValue = nullptr);

        template <typename T> 
        T tryGetKey(string parameterName);

        std::shared_ptr<arrow::Table> getData();
        std::shared_ptr<arrow::Table> getDataWithLocations();
        ~ GribMessage();



    private:

        string getStringParameter(string parameterName);
        long getNumericParameter(string parameterName);
        double getDoubleParameter(string parameterName);
        std::unique_ptr<GridArea> getGridArea();
        std::vector<double> colToVector(std::shared_ptr<arrow::ChunkedArray> columnArray);
        GribLocationData* getLocationData(std::unique_ptr<GridArea> gridArea);
        GribReader* _reader;
        codes_handle* h;
        long _message_id;
   
};

#endif /*GRIB_MESSAGE_H_INCLUDED*/