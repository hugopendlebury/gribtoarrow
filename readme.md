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
import gribtoarrow

stations = (
    pl.read_csv("/Users/hugo/Development/cpp/grib_to_arrow/master.csv", has_header=False)
    .with_columns([pl.col('column_7').alias('lat'),pl.col('column_8').alias('lon')]
).to_arrow()

reader = ( 
	robot.GribToArrow("/Users/hugo/Development/cpp/grib_to_arrow/big.grib")
	     .withStations(arrow_stations)
)
data = [pl.from_arrow(message.getDataWithStations2()) for message in reader]
df = pl.concat(data)
print(f"done all data extracted {len(df)} rows read from grib")
df.write_parquet("/Users/hugo/Development/cpp/grib_to_arrow/output.parquet")









