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
    This exposes methods such as getData(), getDataWithLocations() and many methods to get attribute values such as paramId, shortName etc..


A sample usage of the Library in python is given below. In this code a config CSV is read with polars.
The CSV contains a list of latitude / longitudes where we want to know the nearest equivalent values for those locations in the Grib file.
e.g. This might be a list of all the major world cities.
The polars table is converted to arrow and passed to the GribToArrow method which returns our reader / iterator object.
Next a simple list comprehension is used to extract all the details from every message and the results are saved to a parquet file.
As can been seen a lot of work was accomplished in just 14 lines of python. In addition to the low amount of code required
we also benefit from quick performance. 

```python
import polars as pl
from gribtoarrow import GribToArrow

locations = (
    pl.read_csv("/Users/hugo/Development/cpp/grib_to_arrow/locations.csv", has_header=False)
    .with_columns([pl.col('column_7').alias('lat'),pl.col('column_8').alias('lon')]
).to_arrow()

reader = ( 
    GribToArrow("/Users/hugo/Development/cpp/grib_to_arrow/big.grib")
        .withLocations(locations)
)
data = [pl.from_arrow(message.getDataWithLocations()) for message in reader]
df = pl.concat(data)
print(f"done all data extracted {len(df)} rows from grib")
df.write_parquet("/Users/hugo/Development/cpp/grib_to_arrow/output.parquet")
```

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

- withLocations -> Pass an arrow table to this function which includes the columns "lat" and "lon" and the results will be filtered to the 
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

The recommended way to create the project is to use poetry.

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

If you have used poetry then a wheel will be built which contains gribtoarrow and eccodes.
The resulting ELF files on linux should be linked correctly and have the rpath set so there is no need to set LD_LIBRARY_PATH 

OSX should also work the same way.

If you have any issues shout out.

Note it might be necessary to import pyarrow prior to gribtoarrow

e.g. You might need to

import pyarrow
import gribtoarrow

This will be necessary if Poetry build the project with a different version of arrow / pyarrow than is normally installed

If this is the case your will see something like the below

>>> import gribtoarrow
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
ImportError: dlopen(/Users/hugo/Development/cpp/grib_to_arrow/dist/temp/gribtoarrow.cpython-312-darwin.so, 0x0002): Library not loaded: @rpath/libarrow_python.dylib

This is because arrow and pyarrow are needed in the build and are linked against. However poetry uses venv so gribtoarrow is build
against a version in a poetry venv which can't later be found.

I might be able to play with @rpaths / @rpath-link but for now just import the system installed pyarrow first.

## Poetry Building

The poetry build performs the following steps:

- Defines cmake as a dependency (this is required to build eccodes and may not be installed on the system in question)

Downloads eccodes from ECMWF
Compiles ECCODES (into a folder called temp_eccodes)
Compiles this project using PyBind11
    Note the following (The build of this project is done in build.py)
    In build.py we set rpath. This is the runtime path which is baked into the shared object which will be used to look 
    for any dependencies. The rpath is set to a folder called "eccodes" which should be in the same location the the gribtopython.so
    The eccodes libraries are copied into a folder in "dist"
    A folder called "lib" is also created in dist into which eccodes_memfs is copied
    This is done so that the wheel is bundled with all of it's dependencies and everything can be found without the need 
    to specify environment variables such as LD_LIBRARY_PATH

A visual representation of the files inside the wheel is given below

    .
    ├── eccodes
    │   ├── libeccodes.dylib
    │   └── libeccodes_memfs.dylib
    ├── gribtoarrow.cpython-312-darwin.so
    └── lib
        └── libeccodes_memfs.dylib

As can be seen the main dynamic library gribtoarrow.cpython-312-darwin.so (will have a different name on linux) is at the root level.
In order for gribtoarrow to be able to use libeccodes -Wl,-rpath,$ORIGIN/eccodes in set in the linker.
$ORIGIN in a linux specific option an basically means the current location (which when installed will be part of site-packages)
So in effect gribtoarrow looks in a child folder called eccodes for libeccodes. 
Note how libeccodes_memfs is present twice and also in a location called "lib".
This isn't documented on ECMWFs page and it appread that libeccoes either has it's own rpath or look in some locations one of which
is ../lib (this was found via the use of strace)

In the case of OSX the logic and the wheel is basically the same except rather than using $ORIGIN in the rpath @loader_path is
used instead.


## Documention

    Doc string have been added to grib_to_arrow and it should be possible to generate documents using Sphinx.
