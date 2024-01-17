import polars as pl
from gribtoarrow import GribReader

locations = (
    pl.read_csv(
        "/Users/hugo/Development/cpp/grib_to_arrow/master.csv", has_header=False
    ).with_columns([pl.col("column_7").alias("lat"), pl.col("column_8").alias("lon")])
).to_arrow()

reader = GribReader(
    "/Users/hugo/Development/cpp/grib_to_arrow/biggest.grib"
).withLocations(locations)

required_shortNames = set(["tcc"])
payloads = []
for message in reader:
    if message.getShortName() in required_shortNames:
        payloads.append(message.getDataWithLocations())


df = pl.concat(payloads)

print(f"done {len(df)} rows read")
df.write_parquet("/Users/hugo/Development/cpp/grib_to_arrow/output.parquet")
