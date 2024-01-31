import polars as pl
import pytest
import os
import sys
import pyarrow

class TestExceptions:
    def test_incorrect_grib_filePath(self, resource):
        import gribtoarrow

        with pytest.raises(gribtoarrow.NoSuchGribFileException):
            gribtoarrow.GribReader(str(resource) + "/NOFILEPRESENT.grb")

    def test_incorrect_locations_filePath(self, resource):
        import gribtoarrow

        with pytest.raises(gribtoarrow.NoSuchLocationsFileException):
            gribtoarrow.GribReader(str(resource) + "/norway.grb").withLocations(str(resource) + "/NOLOCATIONSFILE.csv")

    def test_invalid_csv_filePath(self, resource):
        import gribtoarrow

        with pytest.raises(gribtoarrow.InvalidCSVException):
            gribtoarrow.GribReader(str(resource) + "/norway.grb").withLocations(str(resource) + "/badfile.csv")

    def test_incorrect_conversion_cols(self, resource):
        import gribtoarrow

        conversions = (
            pl.DataFrame(
                {
                    "parameterId": [167],
                    "addition_value": [None],
                    "subtraction_value": [None],
                }
            )
        ).to_arrow()

        with pytest.raises(gribtoarrow.InvalidSchemaException):
            (
                gribtoarrow.GribReader(f"{resource}{os.sep}gep01.t00z.pgrb2a.0p50.f003")
                    .withConversions(conversions)
            )

    def test_arrow_table_wrong_ctypes(self, resource):
        import gribtoarrow

        conversions = (
            pl.DataFrame(
               {
                    "parameterId": [167],
                    "addition_value": ["hugo pendlebury"],
                    "subtraction_value": [1],
                    "multiplication_value": [1.1],
                    "division_value": ["fred"],
                }
            )
        ).to_arrow()

        with pytest.raises(gribtoarrow.InvalidSchemaException):
            (
                gribtoarrow.GribReader(f"{resource}{os.sep}gep01.t00z.pgrb2a.0p50.f003")
                    .withConversions(conversions)
            )


    def test_incorrect_missing_csv_conversion_cols(self, resource):
        import gribtoarrow
        csv_file = "__TEST__MISSING_CONVERSION_COL.csv"
        pl.DataFrame(
            {
                "parameterId": [167],
                "addition_value": [None],
                "subtraction_value": [None],
            }
        ).write_csv(str(resource) + csv_file)


        with pytest.raises(gribtoarrow.InvalidSchemaException):
            (
                gribtoarrow.GribReader(f"{resource}{os.sep}gep01.t00z.pgrb2a.0p50.f003")
                    .withConversions(str(resource) + csv_file)
            )

 