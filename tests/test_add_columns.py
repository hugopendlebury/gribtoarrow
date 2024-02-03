import polars as pl
import pytest
import os
import sys
import pyarrow


class TestColumns():
    def test_column_addition_to_df(self, resource):
        from gribtoarrow import GribReader

        reader = GribReader(str(resource) + "/gep01.t00z.pgrb2a.0p50.f003")

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
                    pl.lit(message.getGribMessageId()).alias("messageId"),
                ]
            )
            data.append(df)

        df = pl.concat(data)
        # Verify that the data frame is as expected
        cols = set(df.columns)
        assert "step" in cols
        assert "parameterID" in cols
        assert "perturbationNumber" in cols

        df_first_row = df.filter(pl.col("messageId") == 0)
        assert all(x == 3 for x in df_first_row["step"].to_list())
        assert all(x == 156 for x in df_first_row["parameterID"].to_list())
        assert all(x == 1 for x in df_first_row["perturbationNumber"].to_list())

    def test_auto_columns(self, resource):
        """The columns parameterId, modelNo, forecast_date, datetime are added to the results 
        when getDataWithLocations is called. Ensure that these automatically added fields are the same
        as the values obtained when calling the function directly on the message
        """

        from gribtoarrow import GribReader

        locations = pl.DataFrame(
            {"lat": [51.5054, 53.4808], "lon": [-0.027176, 2.2426]}
        ).to_arrow()

        reader = GribReader(str(resource) + "/gep01.t00z.pgrb2a.0p50.f003").withLocations(locations)
        data = []
        for message in reader:
            df = pl.from_arrow(message.getDataWithLocations()).with_columns(
                    parameterIdMsg = pl.lit(message.getParameterId()),
                    modelNoMsg = pl.lit(message.getModelNumber()),
                    forecast_dateMsg = pl.lit(message.getChronoDate()),
                    datetimeMsg = pl.lit(message.getObsDate()),    
            ).filter(
                        (pl.col("parameterId") != pl.col("parameterIdMsg")) |
                        (pl.col("modelNo") != pl.col("modelNoMsg")) |
                        (pl.col("forecast_date") != pl.col("forecast_dateMsg")) |
                        (pl.col("datetime") != pl.col("datetimeMsg")) 
                    )
            data.append(df)
        df = pl.concat(data)
        assert len(df) == 0