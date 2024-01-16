import polars as pl
import pytest
import os
import sys
import pyarrow

class TestStations:
    def test_find_nearest(self, resource):
        from gribtoarrow import GribReader

        # Locations are Canary Wharf,Manchester & kristiansand
        stations = pl.DataFrame(
            {"lat": [51.5054, 53.4808, 58.1599], "lon": [-0.027176, 2.2426, 8.0182]}
        ).to_arrow()

        reader = GribReader(str(resource) + "/meps_weatherapi_sorlandet.grb").withStations(stations)

        df = pl.concat(
            pl.from_arrow(message.getDataWithStations()) for message in reader
        )

        # The underlying Grib has 268  messages so we expect 268 rows for  kristiansand

        assert len(df) == 268

    def test_areas_are_restricted_to_coordinates(self, resource):
        from gribtoarrow import GribReader
        # This file was created by combining 2 gribs
        # Locations kristiansand (sorlandet) (index 1) Bergen (West Norway) (index 2)
        # There are 268 rows in sorlandet grib 57.8 / 7 / 58.8 / 9.4 
        # There are 242 rows in west Norway Grib 57.9 / 4.45 / 63 / 7
        stations = pl.DataFrame(
            {"location_id": [1,2], "lat": [58.1599, 60.3913], "lon": [8.0182, 5.3221]}
        ).to_arrow()

        reader = GribReader(str(resource) + "/norway.grb").withStations(stations)

        df = pl.concat(
            pl.from_arrow(message.getDataWithStations()) for message in reader
        )

        # The underlying Grib has 510 messages so we expect 268 rows for  kristiansand

        assert len(df) == 510
        assert len(df.filter(pl.col("location_id") == 1)) == 268
        assert len(df.filter(pl.col("location_id") == 2)) == 242

    def test_areas_are_restricted_to_coordinates_str(self, resource):
        from gribtoarrow import GribReader
        # This file was created by combining 2 gribs
        # Locations kristiansand (sorlandet) (index 1) Bergen (West Norway) (index 2)
        # There are 268 rows in sorlandet grib 57.8 / 7 / 58.8 / 9.4 
        # There are 242 rows in west Norway Grib 57.9 / 4.45 / 63 / 7
        stations = pl.DataFrame(
            {"location_id": [1,2], "lat": [58.1599, 60.3913], "lon": [8.0182, 5.3221], "location_name": ["kristiansand", "Bergen"]}
        )

        stations.write_csv(str(resource) + '/test_stations.csv')

        reader = GribReader(str(resource) + "/norway.grb").withStations(str(resource) + '/test_stations.csv')

        df = pl.concat(
            pl.from_arrow(message.getDataWithStations()) for message in reader
        )

        # The underlying Grib has 510 messages so we expect 268 rows for  kristiansand

        assert len(df) == 510
        assert len(df.filter(pl.col("location_id") == 1)) == 268
        assert len(df.filter(pl.col("location_id") == 2)) == 242
        assert "location_name" in df.columns