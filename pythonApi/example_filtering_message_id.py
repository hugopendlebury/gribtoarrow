import polars as pl
from gribtoarrow import GribReader

stations = (
	 pl.read_csv("/Users/hugo/Development/cpp/grib_to_arrow/master.csv", has_header=False)
         .with_columns([
				pl.col('column_7').alias('lat')
				,pl.col('column_8').alias('lon')
			])
).to_arrow()

reader = ( 
	GribReader("/Users/hugo/Development/cpp/grib_to_arrow/gespr.t00z.pgrb2a.0p50.f840")
	     .withStations(stations)
)

payloads = []
for message in reader:
    if message.getGribMessageId() == 0:
    	payloads.append(pl.from_arrow(message.getDataWithStations()))

#payloads = [pl.from_arrow(message.getDataWithStations()) for message in reader if message.getGribMessageId() == 0]

df = pl.concat(payloads)

print(f"done {len(df)} rows read")
df.write_parquet("/Users/hugo/Development/cpp/grib_to_arrow/output.parquet")
