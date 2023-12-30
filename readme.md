## Goal

GribToArrow is a C / C++ project which creates a python module to simplify working with 
files in the Grib Format.

Under the hood it uses the ECMWF eccodes library. This is wrapped in C++ and exposed
to python via the library pybind11 and apache arrow.

The python module is comprised of the following:

gribtoarrow (Class)
    This class is equivalent to a reader object it implements an iterator to simplify data access
    A fluent API is provided to allow for functionality such as adding weather stations

gribmessage
    This is an object which will be returned by each iterator of the iterator
    This exposes methods such as getData(), getDataWithStations()


A sample usage of the Library is given below

    import polars as pl
    from gribtoarrow import GribToArrow

    stations = (
        pl.read_csv("/Users/hugo/Development/cpp/grib_to_arrow/stations.csv", has_header=False)
        .with_columns([pl.col('column_7').alias('lat'),pl.col('column_8').alias('lon')]
    ).to_arrow()

    reader = ( 
        GribToArrow("/Users/hugo/Development/cpp/grib_to_arrow/big.grib")
            .withStations(arrow_stations)
    )
    data = [pl.from_arrow(message.getDataWithStations()) for message in reader]
    df = pl.concat(data)
    print(f"done all data extracted {len(df)} rows from grib")
    df.write_parquet("/Users/hugo/Development/cpp/grib_to_arrow/output.parquet")

##Performance
The module is fast since it operates entirely in memory. In addition it releases the GIL to allow python threading. Currently it doesn't use threading in the C++ layer, this is because the default compile on OSX doesn't include OMP and since the main usage is from python it is anticpated the just relasing the GIL will be sufficient.
In addition since everything is extracted in memory and made available to arrow and hence the vast ecosystem of tools such as polars and pandas multiprocessing and partitioning can be utilised.
A test on a 2023 MacBook Pro extracted 230 million rows from a concatenated grib and wrote this to a CSV in 6 seconds.

##Core functionality

The main entry point is the GribReader class, which takes a string path to a grib file in the constructor.

In addition GribReader has a fluent API which includes the following methods

- withStations -> Pass an arrow table to this function which includes the columns "lat" and "lon" and the results will be filtered to nearest location based on the provided co-ordinated

- withConversions -> Pass an arrow table with columns "parameterId", "addition_value", "subtraction_value", "multiplication_value", "division_value".
The values will be used to perform computations on the data. See the tests folder for examples.

Grib reader is iterable so can be used in any for loop / generator / list comprehension etc..
Each iteratation of the reader will return a GribMessage. 

GribMessage provides methods to get attribute based fields and the data.

##Create Python module

###Install Dependencies

- Install ECCODES
- Install arrow
- pip install pyarrow (or use venv but remember to activate it when testing)

###Clone pybind11

In the project folder, clone pybind11

git clone https://github.com/pybind/pybind11.git
Look at .gitignore file, pybind11 repo is ignored as we just read it.

##Compile

Open a terminal being in the project folder, run

mkdir build
cd build
cmake ..
make 
In the build directory, you should have a compiled module with the name similar to:

gribtoarrow.cpython-39-x86_64-linux-gnu.so

or on OSX

gribtoarrow.cpython-312-darwin.so

##Run

Currently there is no installation script, in the future it is anticipated that a wheel file will be built. 

For now either

set the environment variable PYTHON_PATH to the build directory which contains the so file

e.g.

export PYTHON_PATH=/Users/hugo/Development/cpp/grib_to_arrow/build

Or just cd into the build directory remembering to activate your venv contains pyarrow if you are using venv.

Now type

python
in Python, import the library

import gribtoarrow 

You are now ready to work with library. There are many examples of how to use it from python in the tests folder.






