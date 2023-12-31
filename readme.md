## Goal

GribToArrow is a C / C++ project which using C++20 and creates a python module to simplify working with files in the Grib Format.

Under the hood it uses the ECMWF eccodes library. This is wrapped in C++ and exposed to python via the library pybind11 and apache arrow.

Having worked with Meterologic data using the ECMWF tooling for a while I became familar with the structure of Grib files and many of the 
command line tools. However extracting the data was a pain due to having to rely on code which was lacking in tests and where the author had 
left the company a while ago. In addition the existing codebase was inflexible and required preprocessing and lots of glue at the shell level, 
meaning the main logic couldn't be tested either due to missing tools in CI/CD servers. 

GribToArrow was created to overcome these problems. Although it is currently is built with a CMake file it is anticipated that this can be 
changed to use a more modern build backend with a pyproject.toml file to create a wheel which includes the dependencies and allows thorough testing.

GribToArrow aims to abstract away the low level detail and create a python binding which exposed the data in arrow format. The Apache Arrow 
format is rapidly becoming a key component of the data eco-system. Exposing the Grib data in a modern column based tabular format allows for rapid high level development. Enabling operations such as filtering, calculation, renaming, projecting, transposing and saving to files / databases 
a breeze via the use of high level libraries such as polars / pandas.


The python module is comprised of the following:

gribtoarrow (Class)
    This class is equivalent to a reader object it implements an iterator to simplify data access.
    A fluent API is provided to allow for functionality such as adding locations of interest based on latitude / longitude. 

gribmessage (Class)
    This is an object which will be returned by each iteration of the iterator.
    This exposes methods such as getData(), getDataWithStations() and many method to get attribute values such as paramId, shortName etc..


A sample usage of the Library in python is given below. In this code a referance / config CSV is read with polars.
The config CSV contains a list of latitude / longitudes where we want to know the equivalent values in the Grib file
e.g. this might be a list of all the major world cities.
The polars table is converted to arrow and passed to the GribToArrowMethod which returns our reader / iterator object.
Next a simple list comprehension is used to extract all the details from every message and the results are saved to a parquet file.
As can been seen a lot of work was accomplished in just 14 lines of python. In addition to the low amount of code required
we also quick performance.

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

## Performance
The module is fast since it operates entirely in memory. In addition it releases the GIL to allow python threading. Currently it doesn't 
use threading in the C++ layer, this is because the author created the project using OSX and the default compiler on OSX doesn't include OMP. 
Since the main usage is from python it is anticpated the just relasing the GIL will be sufficient.
In addition since everything is extracted in memory and made available to arrow and hence the vast ecosystem of tools such as polars and 
pandas multiprocessing and partitioning of files can be utilised to also achieve parallism.
A test on a 2023 MacBook Pro extracted 230 million rows from a concatenated grib and wrote this to a parquet file in 6 seconds.

## Core functionality

The main entry point is the GribReader class, which takes a string path to a grib file in the constructor.

In addition GribReader has a fluent API which includes the following methods

- withStations -> Pass an arrow table to this function which includes the columns "lat" and "lon" and the results will be filtered to the nearest location based on the provided co-ordinates.

- withConversions -> Pass an arrow table with columns "parameterId", "addition_value", "subtraction_value", "multiplication_value", "division_value".
The values will be used to perform computations on the data. e.g. The underlying grib might contain a parameter where the data is in Kelvin but you want the values in Celcius, passing a config table with these values will enable the conversions to be performed early in the data pipeline using the Apache Arrow Compute Kernel / module. See the tests folder for examples.

Grib reader is iterable so can be used in any for loop / generator / list comprehension etc..
Each iteratation of the reader will return a GribMessage. 

GribMessage provides methods to get attribute based fields and the data.

## Creating Python module

At the time of writing the project use CMAKE. The paths are currently hardcoded into the CMAKE file so you will need to amend the paths
to match the location of the libraries on your system.
It is anticipated that this will modified in future to created a smarter CMAKE file or simply remove this and replace it with a more modern
build backend and a pyproject.toml file to create a python wheel which can be installed / distributed on PyPi etc...

### Install Dependencies

- Install ECCODES -> build from source
- Install arrow -> use homebrew on osx
- pip install pyarrow (or use venv but remember to activate it when testing)
- pip install polars (if you want to run the samples / tests). At the lowest level you can intereact with the result using pyArrow or any tools which can interact with the Apache Arrow ecosystem e.g. Pandas / Polars etc..

### Clone pybind11

The module is created using pybind11. Rather than adding this as source to this repository you should instead clone the latest version into this 
repo. This can be done with a command similar to the one below, note it might not be exactly this command git might suggest to use a sub-project.

In the project folder, clone pybind11

git clone https://github.com/pybind/pybind11.git
Look at .gitignore file, pybind11 repo is ignored as we just read it.

### Compile

Use an IDE / Plugin such as visual studio code 

Or...

Open a terminal being in the project folder, run

mkdir build
cd build
cmake ..
make 
In the build directory, you should have a compiled module with the name similar to:

gribtoarrow.cpython-39-x86_64-linux-gnu.so

or on OSX

gribtoarrow.cpython-312-darwin.so

### Run

Currently there is no installation script, in the future it is anticipated that a wheel file will be built. 

For now either :-

set the environment variable PYTHON_PATH to the build directory which contains the .so file

e.g.

export PYTHON_PATH=/Users/hugo/Development/cpp/grib_to_arrow/build

Or just cd into the build directory remembering to activate your venv which ontains pyarrow if you are using venv.

Now type

python

To access the python REPL and then import the library using the command below.

import gribtoarrow 

You are now ready to work with library. There are many examples of how to use it from python in the tests folder
and the .py files in the pythonApi. A list of the exposed methods can be seen in grib_to_arrow.cpp in the pythonApi
folder.






