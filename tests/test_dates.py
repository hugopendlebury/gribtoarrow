import polars as pl

class TestLocations:

    def __getLocations(self):
        # Locations are Canary Wharf and Manchester
        return pl.DataFrame(
            {"lat": [51.5054, 53.4808], "lon": [-0.027176, 2.2426]}
        ).to_arrow()

    def test_with_zero_hour_run_and_hour_3(self, resource):
        from gribtoarrow import GribReader
    
        locations = self.__getLocations()

        reader = GribReader(
            str(resource) + "/gep01.t00z.pgrb2a.0p50.f003"
        ).withLocations(locations)

        df = pl.concat(
            pl.from_arrow(message.getDataWithLocations()) for message in reader
        )

        date_hours = df.select(pl.col("datetime").dt.hour().unique())['datetime'].to_list()
        forecast_hours = df.select(pl.col("forecast_date").dt.hour().unique())['forecast_date'].to_list()

        assert all(x == 3 for x in date_hours)
        assert all(x == 0 for x in forecast_hours)

    def test_with_18_hour_run_and_hour_6(self, resource):
        from gribtoarrow import GribReader
    
        locations = self.__getLocations()

        reader = GribReader(
            str(resource) + "/ecmwfaifs0h.grib"
        ).withLocations(locations)

        df = pl.concat(
            pl.from_arrow(message.getDataWithLocations()) for message in reader
        )

        date_hours = df.select(pl.col("datetime").dt.hour().unique())['datetime'].to_list()
        forecast_hours = df.select(pl.col("forecast_date").dt.hour().unique())['forecast_date'].to_list()

        assert all(x == 0 for x in date_hours)
        assert all(x == 18 for x in forecast_hours)

