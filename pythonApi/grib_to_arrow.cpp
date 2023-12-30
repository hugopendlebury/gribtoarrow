#include "../src/gribreader.hpp"
#include "../src/gribmessage.hpp"
#include "../pybind11/include/pybind11/pybind11.h"
// to convert C++ STL containers to python list
#include "../pybind11/include/pybind11/stl.h" 
#include "../pybind11/include/pybind11/chrono.h" 
#include <arrow/api.h>
#include <arrow/python/pyarrow.h>
#include "../src/caster.hpp"


namespace py = pybind11;
using namespace std;
PYBIND11_MODULE(gribtoarrow, m)
{
    arrow::py::import_pyarrow();
    py::class_<GribReader>(m, "GribReader")
        .def(py::init<string>(), pybind11::call_guard<pybind11::gil_scoped_release>()) // constructor
        .def("withStations", &GribReader::withStations, pybind11::call_guard<pybind11::gil_scoped_release>())
        .def("withConversions", &GribReader::withConversions, pybind11::call_guard<pybind11::gil_scoped_release>())
        .def(
            "__iter__",
            [](GribReader &s) { return py::make_iterator(s.begin(), s.end()); },
            py::keep_alive<0, 1>() )
        .doc() = R"EOL(Enables the easy conversion of data in the grib format to Apache Arrow. 

                The main entry point is a class called GribReader                    
                )EOL";

    py::class_<GribMessage>(m, "GribMessage")
        .def("getCodesHandleAddress", &GribMessage::getCodesHandleAddress)
        .def("getObjectAddress", &GribMessage::getObjectAddress)
        .def("getParameterId", &GribMessage::getParameterId, pybind11::call_guard<pybind11::gil_scoped_release>())
        .def("getGribMessageId", &GribMessage::getGribMessageId, pybind11::call_guard<pybind11::gil_scoped_release>())
        .def("getModelNumber", &GribMessage::getModelNumber, pybind11::call_guard<pybind11::gil_scoped_release>())
        .def("getStep", &GribMessage::getStep, pybind11::call_guard<pybind11::gil_scoped_release>())
        .def("getStepUnits", &GribMessage::getStepUnits, pybind11::call_guard<pybind11::gil_scoped_release>())
        .def("getShortName", &GribMessage::getShortName, pybind11::call_guard<pybind11::gil_scoped_release>())
        .def("getDate", &GribMessage::getDate, pybind11::call_guard<pybind11::gil_scoped_release>())
        .def("getTime", &GribMessage::getTime, pybind11::call_guard<pybind11::gil_scoped_release>())
        .def("getDateNumeric", &GribMessage::getDateNumeric, pybind11::call_guard<pybind11::gil_scoped_release>())
        .def("getTimeNumeric", &GribMessage::getTimeNumeric, pybind11::call_guard<pybind11::gil_scoped_release>())
        .def("getChronoDate", &GribMessage::getChronoDate, pybind11::call_guard<pybind11::gil_scoped_release>())
        .def("getLatitudeOfFirstPoint", &GribMessage::getLatitudeOfFirstPoint, pybind11::call_guard<pybind11::gil_scoped_release>())
        .def("getLongitudeOfFirstPoint", &GribMessage::getLongitudeOfFirstPoint, pybind11::call_guard<pybind11::gil_scoped_release>())
        .def("getLatitudeOfLastPoint", &GribMessage::getLatitudeOfLastPoint, pybind11::call_guard<pybind11::gil_scoped_release>())
        .def("getLongitudeOfLastPoint", &GribMessage::getLongitudeOfLastPoint, pybind11::call_guard<pybind11::gil_scoped_release>())
        .def("getData", &GribMessage::getData, pybind11::call_guard<pybind11::gil_scoped_release>())
        .def("getDataWithStations", &GribMessage::getDataWithStations, pybind11::call_guard<pybind11::gil_scoped_release>())
        ;

}

