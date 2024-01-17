#include "../src/gribreader.hpp"
#include "../src/gribmessage.hpp"

// #define USE_CMAKE

#ifdef USE_CMAKE
    #include "../pybind11/include/pybind11/pybind11.h"
    #include "../pybind11/include/pybind11/stl.h" 
    #include "../pybind11/include/pybind11/chrono.h" 

#else
    #include <pybind11/pybind11.h>
    #include <pybind11/stl.h>
    #include "pybind11/chrono.h" 
#endif


#include <arrow/api.h>
#include <arrow/python/pyarrow.h>
#include "../src/caster.hpp"


namespace py = pybind11;
using namespace std;
PYBIND11_MODULE(gribtoarrow, m)
{
    arrow::py::import_pyarrow();
    py::class_<GribReader>(m, "GribReader")
        .def(py::init<string>(), pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            Creates a new Grib reader. 

            Parameters
            ----------
            filepath (str): A string containing the full path of the grib file                  
        )EOL") // constructor
        .def("withLocations", py::overload_cast<std::shared_ptr<arrow::Table>>(&GribReader::withLocations), pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            Adds locations which will be filtered in each message. 

            Parameters
            ----------
            stations (pyArrow.Table): A PyArrow table which contains a minimum of 2 columns called lat and lon

            The grib will be filtered by any of the coordinated given by lat and lon which are within the grid of 
            the underlying message.
            Any additional columns passed in the table will be passed through in the results when  getDataWithLocations
            is called on the message. e.g. if you passed a table with the columns "LocationName, Country, lat, lon" then 
            the fields of LocationName and Country would also be present in the results of the message.                 
        )EOL")
        .def("withLocations", py::overload_cast<std::string>(&GribReader::withLocations), pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            Adds locations which will be filtered in each message. 

            Parameters
            ----------
            path (string): Path to a csv containing minimum two columns called lat and lon

            The grib will be filtered by any of the coordinated given by lat and lon which are within the grid of 
            the underlying message.
            Any additional columns passed in the table will be passed through in the results when  getDataWithLocations
            is called on the message. e.g. if you passed a table with the columns "LocationName, Country, lat, lon" then 
            the fields of LocationName and Country would also be present in the results of the message.                 
        )EOL") 
        .def("withConversions", &GribReader::withConversions, pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            Adds conversion which be filtered in each message matching message. 

            Parameters
            ----------
            conversions (pyArrow.Table): A PyArrow table which contains the following columns:
            parameterId: arrow::int64()
            addition_value: arrow::float64()
            subtraction_value: arrow::float64()
            multiplication_value: arrow::float64()
            division_value: arrow::float64()
            ceiling_value:  arrow::float64()

            When the paramterId in the message matches a paramterId in this table the appropriate operation will occur.

            e.g. If you wanted to convert from Kelvin to Celcius you would pass a table which contained the parameterId and
            contained 273.15 in the column subtraction_value                
        )EOL") 
        .def(
            "__iter__",
            [](GribReader &s) { return py::make_iterator(s.begin(), s.end()); },
            py::keep_alive<0, 1>() )
        .doc() = R"EOL(
            Enables the easy conversion of data in the grib format to Apache Arrow. 

            The main entry point is a class called GribReader                    
        )EOL";

    py::class_<GribMessage>(m, "GribMessage")
        .def("getCodesHandleAddress", &GribMessage::getCodesHandleAddress)
        .def("getObjectAddress", &GribMessage::getObjectAddress)
        .def("getParameterId", &GribMessage::getParameterId, pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            The paramId (parameterId of the message)                 
        )EOL") 
        .def("getGribMessageId", &GribMessage::getGribMessageId, pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            The index of the message in the grib e.g. is this the first message, the 2nd message, the xth message etc..                 
        )EOL") 
        .def("getModelNumber", &GribMessage::getModelNumber, pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            The model / perturbation Number.                 
        )EOL") 
        .def("getPerturbationNumber", &GribMessage::getModelNumber, pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            The perturbation Number (same thing as model)               
        )EOL") 
        .def("getStep", &GribMessage::getStep, pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            The step interval e.g. 3 (often useful with getStepUnits()) e.g units might be h                
        )EOL") 
        .def("getStepUnits", &GribMessage::getStepUnits, pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            The step units e.g. h,d,m etc..                
        )EOL") 
        .def("getShortName", &GribMessage::getShortName, pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            The shortname e.g. tcc (total cloud cover), tp (total precipitation )                
        )EOL") 
        .def("getDate", &GribMessage::getDate, pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            Gets the date of the message as a string 
            If you don't want to apply any conversion then use  getChronoDate which will return a datetime object              
        )EOL") 
        .def("getDataType", &GribMessage::getDataType, pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            Gets the dataType of the message as a string             
        )EOL") 
        .def("getTime", &GribMessage::getTime, pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            Gets the time of the message as a string   
            If you don't want to apply any conversion then use  getChronoDate which will return a datetime object                 
        )EOL") 
        .def("getDateNumeric", &GribMessage::getDateNumeric, pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            Gets the date as a number / decimal  
            If you don't want to apply any conversion then use  getChronoDate which will return a datetime object                 
        )EOL") 
        .def("getTimeNumeric", &GribMessage::getTimeNumeric, pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            Gets the time as a number / decimal  
            If you don't want to apply any conversion then use  getChronoDate which will return a datetime object                 
        )EOL") 
        .def("getChronoDate", &GribMessage::getChronoDate, pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            Gets the date / time as a python Datetime object                
        )EOL") 
        .def("getObsDate", &GribMessage::getObsDate, pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            Gets the Observation date / time as a python Datetime object                
        )EOL") 
        .def("getLatitudeOfFirstPoint", &GribMessage::getLatitudeOfFirstPoint, pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            Gets the Latitude of the first point in the message                
        )EOL") 
        .def("getLongitudeOfFirstPoint", &GribMessage::getLongitudeOfFirstPoint, pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            Gets the Longitude of the first point in the message                
        )EOL") 
        .def("getLatitudeOfLastPoint", &GribMessage::getLatitudeOfLastPoint, pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            Gets the Latitude of the last point in the message                
        )EOL") 
        .def("getLongitudeOfLastPoint", &GribMessage::getLongitudeOfLastPoint, pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            Gets the Longitude of the last point in the message                
        )EOL") 
        .def("getGridDefinitionTemplateNumber", &GribMessage::getGridDefinitionTemplateNumber, pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            Gets the grid defintion template number               
        )EOL") 
        .def("getData", &GribMessage::getData, pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            Gets the Data from the message

            Return 3 fields the value and the latitude and longitude or the value               
        )EOL") 
        .def("getDataWithLocations", &GribMessage::getDataWithLocations, pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            Return the values constrained by the locations specified in table to restrict by when passed in the reader              
        )EOL") 
        .def("iScansNegatively", &GribMessage::iScansNegatively, pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            Return if the i(s) scan negatively in the grid              
        )EOL") 
        .def("jScansPositively", &GribMessage::jScansPositively, pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            Return if the j(s) scan positively in the grid              
        )EOL") 
            .def("getEditionNumber", &GribMessage::getEditionNumber, pybind11::call_guard<pybind11::gil_scoped_release>(), R"EOL(
            Return if the grib version e.g. 1/2              
        )EOL") 
        .doc() = R"EOL(
            This class provides the ability to access attributes such as the parameterId  

            There are two methods available to access the data:

            getData() - Will return all the data present in the values array for the message even if station were defined in the reader
            getDataWithLocations() - Restricts the results which are within the bounds of the coordinates of the message and the stations
                                    defined in the reader class
        )EOL";
        ;

}

