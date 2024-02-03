import polars as pl
from gribtoarrow import GribReader

#This is the latitude / longitude of Canary wharf
locations = pl.DataFrame({'lat' : [51.5054], 'lon': [-0.027176]}).to_arrow()

conversions = (
	pl.DataFrame({
        	'parameterId' : [167],
        	'addition_value' : [20.0],
        	'subtraction_value' : [None],
        	'multiplication_value' : [None],
        	'division_value' : [None],
        	'ceiling_value' : [None]
	}).select(
     		pl.col("parameterId")
    		,pl.col("addition_value").cast(pl.Float64)
    		,pl.col("subtraction_value").cast(pl.Float64)
    		,pl.col("multiplication_value").cast(pl.Float64)
    		,pl.col("division_value").cast(pl.Float64)
    		,pl.col("ceiling_value").cast(pl.Float64)
	)
).to_arrow()

reader = ( 
	GribReader("/Users/hugo/Development/cpp/grib_to_arrow/gespr.t00z.pgrb2a.0p50.f840")
	     .withLocations(locations)
	     .withConversions(conversions)
)

#paramter 167 is 2t (temperature at 2 metres above ground level) its units are in K we cant to convert to DegreesCelcius
payloads = []
for message in reader:
    df = (
		pl.from_arrow(message.getDataWithLocations())
			.with_columns([
                     		pl.lit(message.getParameterId()).alias("paramId")
			])
	)
    payloads.append(df)

df = pl.concat(payloads).filter(pl.col("paramId").is_in([167]))

print(df)
