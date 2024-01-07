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

class TestColumns:
    def test_column_addition_to_df(self, resource):
        from gribtoarrow import GribReader

        reader = GribReader(f"{resource}{os.sep}gep01.t00z.pgrb2a.0p50.f003")

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
