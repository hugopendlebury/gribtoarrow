import polars as pl
import pytest
import os
import sys


@pytest.fixture()
def resource():
    # At the moment the extension is built with cmake and we have no wheel / install
    # as a result we need to help python find our module
    # TODO - create a wheel
    test_path = os.path.dirname(os.path.realpath(__file__))
    dir_path = test_path.split(os.sep)[:-1]
    dir_path.append("build")
    module_path = os.sep.join(dir_path)
    sys.path.append(module_path)
    yield test_path


class TestStations:
    def test_find_nearest(self, resource):
        from gribtoarrow import GribReader

        raw_reader = GribReader(f"{resource}{os.sep}gep01.t00z.pgrb2a.0p50.f003")
        # Call getData on reader class without setting any stations
        df = pl.concat(pl.from_arrow(message.getData()) for message in raw_reader)

        # With 85 messages and 259920 points per message size should be 22093200
        assert len(df) == 22093200

        # Locations are Canary Wharf and Manchester
        stations = pl.DataFrame(
            {"lat": [51.5054, 53.4808], "lon": [-0.027176, 2.2426]}
        ).to_arrow()

        reader = GribReader(
            f"{resource}{os.sep}gep01.t00z.pgrb2a.0p50.f003"
        ).withStations(stations)

        df = pl.concat(
            pl.from_arrow(message.getDataWithStations()) for message in reader
        )

        # The underlying Grib has 85 messages so we expect 170 rows

        assert len(df) == 170
