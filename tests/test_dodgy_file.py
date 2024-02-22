import polars as pl
import pytest


class TestDodgyFile:
    def test_compressed_file(self, resource):
        import gribtoarrow

        #Since the file is compressed externally we can't read it
        reader = gribtoarrow.GribReader(str(resource) + "/meps_weatherapi_sorlandet.grb.bz2")

        with pytest.raises(gribtoarrow.GribException):
            iter(reader)
