import polars as pl
import pytest
import os
import sys
import pyarrow

@pytest.fixture()
def resource():
    test_path = os.path.dirname(os.path.realpath(__file__))
    dir_path = test_path.split(os.sep)[:-1]
    dir_path.append("build")
    module_path = os.sep.join(dir_path)
    sys.path.append(module_path)
    yield test_path

class TestStations:
    def test_find_nearest(self, resource):
        from gribtoarrow import GribReader

        # Locations are Canary Wharf,Manchester & kristiansand
        stations = pl.DataFrame(
            {"lat": [51.5054, 53.4808, 58.1599], "lon": [-0.027176, 2.2426, 8.0182]}
        ).to_arrow()

        reader = GribReader(
            f"{resource}{os.sep}meps_weatherapi_sorlandet.grb"
        ).withStations(stations)

        df = pl.concat(
            pl.from_arrow(message.getDataWithStations()) for message in reader
        )

        # The underlying Grib has 268  messages so we expect 268 rows for  kristiansand

        assert len(df) == 268
