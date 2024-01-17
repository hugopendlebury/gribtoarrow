import polars as pl
from gribtoarrow import GribReader

stations = (
    pl.read_csv(
        "/Users/hugo/Development/cpp/grib_to_arrow/master.csv", has_header=False
    ).with_columns([pl.col("column_7").alias("lat"), pl.col("column_8").alias("lon")])
).to_arrow()

reader = GribReader(
    "/Users/hugo/Development/cpp/grib_to_arrow/biggest.grib"
).withLocations(stations)

data = [pl.from_arrow(b.getDataWithLocations()) for b in reader]
df = pl.concat(data)

print(f"done {len(df)} rows read")
df.write_parquet("/Users/hugo/Development/cpp/grib_to_arrow/output.parquet")
