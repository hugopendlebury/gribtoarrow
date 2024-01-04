## Goal

GribToArrow is a C / C++ project which uses C++20 and creates a python module to simplify working with files in the GRIB Format 
(GRIdded Binary or General Regularly-distributed Information in Binary form).

Under the hood it uses the ECMWF eccodes library. This is wrapped in C++ and exposed to python via the library pybind11 and Apache Arrow. If
you want a simple pythonic way to interact with GRIB data then give this module a try.

Having worked with Meteorologic data using the ECMWF tooling for a while I became familar with the structure of Grib files and many of the 
command line tools provided by ECMWF. However extracting the data was a pain due to having to rely on legacy c code written by someone who
had left the company a while ago and was poorly structured and lacking in tests. In addition the existing codebase was inflexible and 
required preprocessing and lots of glue at the shell level, meaning the main logic couldn't be tested particulary well due to
missing tools on the CI/CD servers and a black box executable program.

GribToArrow was created to overcome these problems. It can be installed using CMAKE or in a more pythonic way using poetry.

GribToArrow aims to abstract away the low level detail and create a python binding which exposes the data in arrow format. The Apache Arrow 
format is rapidly becoming a key component of most modern data eco-systems. Exposing the Grib data in a modern column based format allows for 
rapid high level development. Operations such as filtering, calculations, joining, aggregating, renaming, projecting, transposing and 
saving to files / databases becomes a breeze via the ease of integrating with high level libraries such as polars, pandas and duckdb. 
Additionally due to the way Apache Arrow is created the integration is typically known as zero copy meaning data can be passed between 
any tool which can read Apache Arrow at zero cost.

What does this mean in reality ? It means you can mix and match tools. If you start using this library with polars but find some functionality
is missing such as geospatial functions you can keep the existing logic in polars and pass the dataframe to a tool such as duckdb to perform
the geospatial elements of your processing and then pass this back to polars if required (At the time of writing geoparquet is a work in 
progress and once this is completed work on geopolars will commence. However duckdb has integrated many of the geospatial functions from postGIS).


The python module is comprised of the following:

gribtoarrow (Class)
    This class is equivalent to a reader object it implements an iterator to simplify data access.
    A fluent API is provided to allow for functionality such as adding locations of interest based on latitude / longitude. 

gribmessage (Class)
    This is an object which will be returned by each iteration of the iterator.
    This exposes methods such as getData(), getDataWithStations() and many methods to get attribute values such as paramId, shortName etc..


A sample usage of the Library in python is given below. In this code a config CSV is read with polars.
The CSV contains a list of latitude / longitudes where we want to know the nearest equivalent values for those locations in the Grib file.
e.g. This might be a list of all the major world cities.
The polars table is converted to arrow and passed to the GribToArrow method which returns our reader / iterator object.
Next a simple list comprehension is used to extract all the details from every message and the results are saved to a parquet file.
As can been seen a lot of work was accomplished in just 14 lines of python. In addition to the low amount of code required
we also benefit from quick performance. 

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
use threading in the C++ layer, this is because the author created the project using OSX and the default compiler on OSX doesn't include OMP, 
this can be done if required although for the reasons detailed below may not be needed. 
Since the main usage is from python it is anticipated that just releasing the GIL will be sufficient.
In addition since everything is extracted in memory and made available to arrow and hence the vast ecosystem of tools such as polars,
pandas and duckdb then multiprocessing and partitioning of files parquet can be utilised to also achieve a high degree of parallism.
A test on a 2023 MacBook Pro extracted 230 million rows from a concatenated grib and wrote this to a parquet file in 6 seconds.

## Core functionality

The main entry point is the GribReader class, which takes a string path to a grib file in the constructor.

In addition GribReader has a fluent API which includes the following methods :-

- withStations -> Pass an arrow table to this function which includes the columns "lat" and "lon" and the results will be filtered to the 
nearest location based on the provided co-ordinates. e.g. You might have a grib file at 0.5 resolution for every location of earth. Logically a lot
of those locations will be at sea, so you could use this facility and specify a list of latitutdes and longitudes to restricte the amount of results
returned.

- withConversions -> Pass an arrow table with columns "parameterId", "addition_value", "subtraction_value", "multiplication_value", "division_value".
The values will be used to perform computations on the data. e.g. The underlying grib might contain a parameter where the data is in Kelvin but 
you want the values to be in Celcius. Passing a config table with these values will enable the conversions to be performed early in the data 
pipeline using the Apache Arrow Compute Kernel / module. See the tests folder for examples.

Grib reader is iterable so can be used in any for loop / generator / list comprehension etc..
Each iteratation of the reader will return a GribMessage. 

GribMessage also provides methods to get attribute based fields and the data.

## Creating the Python module

At the time of writing the project can be built two ways.

-  CMAKE. The paths are currently hardcoded into the CMAKE file so you will need to amend the paths to match the location of the libraries on 
your system. It should noted that after you have installed arrow and pyarrow you can find the include and library paths using these commands.
pyarrow.get_library_dirs() and pyarrow.get_include()

- Poetry. A pyproject.toml file is present in the repo which simplies the build.
    Simply execute the commands below


    poetry build
    poetry install

### Install Dependencies - This will depend on if you are building the project with CMAKE or poetry.

- Install ECCODES -> build from source
- Install arrow -> use homebrew on osx (we need both arrow and pyarrow)

If using cmake you will also need to install the follow

- pip install pyarrow (or use venv but remember to activate it when testing)
- pip install polars (if you want to run the samples / tests). At the lowest level you can interact with the results using pyArrow or any 
tools which can work with the Apache Arrow ecosystem e.g. Pandas, Polars, Duckdb, Vaex etc..

If you are using poetry this will be taken care of for you.

### Clone This project

Clone this project using git

Then cd into the folder and clone pybind11 at the root level of the folder.

### Clone pybind11

The module is created using pybind11. Rather than adding this as source to this repository you should instead clone the latest version into this 
repo. This can be done with a command similar to the one below, note it might not be exactly this command git might suggest to use a sub-project 
or similar, follow the git recommendation.

In the project folder, clone pybind11

git clone https://github.com/pybind/pybind11.git
Look at .gitignore file, pybind11 repo is ignored as we just read it.

### Compile

Use an IDE / Plugin such as visual studio code 

Or...

Open a terminal and cd into the project folder then run

mkdir build
cd build
cmake ..
make 
In the build directory, you should have a compiled module with the name similar to:

gribtoarrow.cpython-312-x86_64-linux-gnu.so

or on OSX

gribtoarrow.cpython-312-darwin.so

Where 312 is your python version (In the case above the author is running python 3.12)

### Run

Currently there is no installation script, in the future it is anticipated that a wheel file will be built. 

For now either :-

set the environment variable PYTHON_PATH to the build directory which contains the .so file

e.g.

export PYTHON_PATH=/Users/hugo/Development/cpp/grib_to_arrow/build

or 

export DYLD_LIBRARY_PATH=/usr/local/lib
python
import pyarrow
import gribtoarrow

Or just cd into the build directory. Remember to activate your venv which contains pyarrow if you are using venv.

Now type

python

Which will load the python REPL and then import the library using the command below.

import gribtoarrow 

You are now ready to work with library. There are many examples of how to use it from python in the tests folder
and the .py files in the pythonApi folder. A list of the exposed methods can be seen in grib_to_arrow.cpp in the pythonApi
folder.






