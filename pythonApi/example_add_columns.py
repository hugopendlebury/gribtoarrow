import polars as pl
from gribtoarrow import GribReader


reader = GribReader("/Users/hugo/Development/cpp/grib_to_arrow/biggest.grib")

# The message contains points (data)
# it also contains header type information such as step, parameterID etc..
# we can access these and set them as custom columns on a dataframe
# e.g. using polars this would be done using pl.lit - since the granularity of these
# properties is one row to the many rows of the points / data

# To see all of the properties use dir(message) in python or look at grib_to_arrow.cpp in
# the pythonApi folder

data = []
for message in reader:
    df = pl.from_arrow(message.getData()).with_columns(
        [
            pl.lit(message.getStep()).alias("step"),
            pl.lit(message.getParameterId()).alias("parameterID"),
            pl.lit(message.getModelNumber()).alias("perturbationNumber"),
        ]
    )
    data.append(df)

df = pl.concat(data)

print(f"done {len(df)} rows read")
df.write_parquet("/Users/hugo/Development/cpp/grib_to_arrow/output.parquet")
