#ifndef GRIB_MESSAGE_H_INCLUDED
#define GRIB_MESSAGE_H_INCLUDED

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

        codes_handle* h;

        GribMessage(GribReader* reader, 
                    codes_handle* handle, 
                    long message_id);
        long getGribMessageId();
        double getLatitudeOfFirstPoint();
        double getLongitudeOfFirstPoint();
        double getLatitudeOfLastPoint();
        double getLongitudeOfLastPoint();
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
        long getStepRange();
        long getHourOffset();
        long getEditionNumber();
        long getNumberOfPoints();
        bool iScansNegatively();
        bool jScansPositively();
        std::shared_ptr<arrow::Table> getData();
        std::shared_ptr<arrow::Table> getDataWithStations();
        ~ GribMessage();



    private:

        string getStringParameter(string parameterName);
        long getNumericParameter(string parameterName);
        double getDoubleParameter(string parameterName);
        std::unique_ptr<GridArea> getGridArea();
        std::vector<double> colToVector(std::shared_ptr<arrow::ChunkedArray> columnArray);
        GribLocationData* getLocationData(std::unique_ptr<GridArea> gridArea);
        GribReader* reader;
        long message_id;
   
};

#endif /*GRIB_MESSAGE_H_INCLUDED*/