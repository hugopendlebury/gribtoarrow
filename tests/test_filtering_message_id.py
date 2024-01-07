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

class TestFilterMessageId:
    def test_filterByMessageId(self, resource):
        from gribtoarrow import GribReader

        reader = GribReader(f"{resource}{os.sep}gep01.t00z.pgrb2a.0p50.f003")

        dfs = [
            pl.from_arrow(message.getData()).with_columns(
                [pl.lit(message.getShortName()).alias("shortName")]
            )
            for message in reader
            if message.getGribMessageId() == 0
        ]

        df = pl.concat(dfs)

        assert len(df) == 259920
        shortName = set(df["shortName"].to_list())
        assert len(shortName) == 1
        assert "gh" in shortName
