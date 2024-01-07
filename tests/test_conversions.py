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

class TestConversions:
    def get_2t_df(self, reader):
        # parameter 167 is 2t (temperature at 2 metres above ground level) its units are in K we cant to convert to DegreesCelcius
        payloads = []
        for message in reader:
            if message.getParameterId() == 167:
                payloads.append(pl.from_arrow(message.getDataWithStations()))

        df = pl.concat(payloads)
        return df

    def get_tcc_df(self, reader):
        # parameter 228164 is tcc (total cloud cover) this is given as a percentage
        payloads = []
        for message in reader:
            if message.getParameterId() == 228164:
                payloads.append(pl.from_arrow(message.getDataWithStations()))

        df = pl.concat(payloads)
        return df

    @pytest.mark.skip(reason="Not sure how to test this yet")
    def test_validation(self, resource):
        from gribtoarrow import GribReader

        stations = pl.DataFrame({"lat": [51.5054], "lon": [-0.027176]}).to_arrow()

        # The table passed to the reader should contain mandatory columns
        # In this instance it is missing a field called "ceiling_value"

        conversions = (
            pl.DataFrame(
                {
                    "parameterId": [167],
                    "addition_value": [None],
                    "subtraction_value": [None],
                    "multiplication_value": [None],
                    "division_value": [None],
                }
            ).select(
                pl.col("parameterId"),
                pl.col("addition_value").cast(pl.Float64),
                pl.col("subtraction_value").cast(pl.Float64),
                pl.col("multiplication_value").cast(pl.Float64),
                pl.col("division_value").cast(pl.Float64),
            )
        ).to_arrow()

        (
            GribReader(f"{resource}{os.sep}gep01.t00z.pgrb2a.0p50.f003")
            .withStations(stations)
            .withConversions(conversions)
        )

    def test_addition(self, resource):
        # In this test we will read a Grib file which was downloaded from
        # NOAA.
        # We will filter the results to the nearest grid based on the Latiture / Longitude of Canary Wharf
        # We will also filter the messages to shortCode 2t (parameter 167)
        # This parameter is Supplied by NOAA in Kelvin so we will convert this to Celcius.
        # The underlying Grib file was downloaded in December 2023 which was a mild winter with temperatures
        # around 6-12 Celcius. The grib file is 00 (Midnight) so the converted value is within expectations

        from gribtoarrow import GribReader

        # This is the latitude / longitude of Canary wharf
        stations = pl.DataFrame({"lat": [51.5054], "lon": [-0.027176]}).to_arrow()

        raw_results_reader = GribReader(
            f"{resource}{os.sep}gep01.t00z.pgrb2a.0p50.f003"
        ).withStations(stations)

        raw_df = self.get_2t_df(raw_results_reader)

        assert round(raw_df["value"].to_list()[0], 2) == round(280.128, 2)

        # In reality the use would probably store these in a config file / CSV

        conversions = (
            pl.DataFrame(
                {
                    "parameterId": [167],
                    "addition_value": [-273.15],
                    "subtraction_value": [None],
                    "multiplication_value": [None],
                    "division_value": [None],
                    "ceiling_value": [None],
                }
            ).select(
                pl.col("parameterId"),
                pl.col("addition_value").cast(pl.Float64),
                pl.col("subtraction_value").cast(pl.Float64),
                pl.col("multiplication_value").cast(pl.Float64),
                pl.col("division_value").cast(pl.Float64),
                pl.col("ceiling_value").cast(pl.Float64),
            )
        ).to_arrow()

        reader = (
            GribReader(f"{resource}{os.sep}gep01.t00z.pgrb2a.0p50.f003")
            .withStations(stations)
            .withConversions(conversions)
        )

        converted_df = self.get_2t_df(reader)

        assert round(converted_df["value"].to_list()[0], 2) == round(6.978, 2)

    def test_subtraction(self, resource):
        # In this test we will read a Grib file which was downloaded from
        # NOAA.
        # We will filter the results to the nearest grid based on the Latiture / Longitude of Canary Wharf
        # We will also filter the messages to shortCode 2t (parameter 167)
        # This parameter is Supplied by NOAA in Kelvin so we will convert this to Celcius.
        # The underlying Grib file was downloaded in December 2023 which was a mild winter with temperatures
        # around 6-12 Celcius. The grib file is 00 (Midnight) so the converted value is within expectations

        from gribtoarrow import GribReader

        # This is the latitude / longitude of Canary wharf
        stations = pl.DataFrame({"lat": [51.5054], "lon": [-0.027176]}).to_arrow()

        raw_results_reader = GribReader(
            f"{resource}{os.sep}gep01.t00z.pgrb2a.0p50.f003"
        ).withStations(stations)

        raw_df = self.get_2t_df(raw_results_reader)

        assert round(raw_df["value"].to_list()[0], 2) == round(280.128, 2)

        # In reality the use would probably store these in a config file / CSV

        conversions = (
            pl.DataFrame(
                {
                    "parameterId": [167],
                    "addition_value": [None],
                    "subtraction_value": [273.15],
                    "multiplication_value": [None],
                    "division_value": [None],
                    "ceiling_value": [None],
                }
            ).select(
                pl.col("parameterId"),
                pl.col("addition_value").cast(pl.Float64),
                pl.col("subtraction_value").cast(pl.Float64),
                pl.col("multiplication_value").cast(pl.Float64),
                pl.col("division_value").cast(pl.Float64),
                pl.col("ceiling_value").cast(pl.Float64),
            )
        ).to_arrow()

        reader = (
            GribReader(f"{resource}{os.sep}gep01.t00z.pgrb2a.0p50.f003")
            .withStations(stations)
            .withConversions(conversions)
        )

        converted_df = self.get_2t_df(reader)

        assert round(converted_df["value"].to_list()[0], 2) == round(6.978, 2)

    def test_division(self, resource):
        # In this test we will read a Grib file which was downloaded from
        # NOAA.
        # We will filter the results to the nearest grid based on the Latiture / Longitude of Canary Wharf
        # We will also filter the messages to shortCode tcc (parameter 228164)
        # This parameter is Supplied by NOAA as a percentage we will divide by 100
        # to convert this as a range between 0-1 (the same as parameter 164)

        from gribtoarrow import GribReader

        # This is the latitude / longitude of Canary wharf
        stations = pl.DataFrame({"lat": [51.5054], "lon": [-0.027176]}).to_arrow()

        raw_results_reader = GribReader(
            f"{resource}{os.sep}gep01.t00z.pgrb2a.0p50.f003"
        ).withStations(stations)

        raw_df = self.get_tcc_df(raw_results_reader)

        print(f"VALUE IS {raw_df['value'].to_list()[0]}")

        assert raw_df["value"].to_list()[0] == 100.0

        # In reality the use would probably store these in a config file / CSV

        conversions = (
            pl.DataFrame(
                {
                    "parameterId": [228164],
                    "addition_value": [None],
                    "subtraction_value": [None],
                    "multiplication_value": [None],
                    "division_value": [100],
                    "ceiling_value": [None],
                }
            ).select(
                pl.col("parameterId"),
                pl.col("addition_value").cast(pl.Float64),
                pl.col("subtraction_value").cast(pl.Float64),
                pl.col("multiplication_value").cast(pl.Float64),
                pl.col("division_value").cast(pl.Float64),
                pl.col("ceiling_value").cast(pl.Float64),
            )
        ).to_arrow()

        reader = (
            GribReader(f"{resource}{os.sep}gep01.t00z.pgrb2a.0p50.f003")
            .withStations(stations)
            .withConversions(conversions)
        )

        converted_df = self.get_tcc_df(reader)

        assert converted_df["value"].to_list()[0] == 1

    def test_multiplication(self, resource):
        # In this test we will read a Grib file which was downloaded from
        # NOAA.
        # We will filter the results to the nearest grid based on the Latiture / Longitude of Canary Wharf
        # We will also filter the messages to shortCode tcc (parameter 228164)
        # This parameter is Supplied by NOAA as a percentage we will multiple this by 0.01
        # to convert this as a range between 0-1 (the same as parameter 164)

        from gribtoarrow import GribReader

        # This is the latitude / longitude of Canary wharf
        stations = pl.DataFrame({"lat": [51.5054], "lon": [-0.027176]}).to_arrow()

        raw_results_reader = GribReader(
            f"{resource}{os.sep}gep01.t00z.pgrb2a.0p50.f003"
        ).withStations(stations)

        raw_df = self.get_tcc_df(raw_results_reader)

        print(f"VALUE IS {raw_df['value'].to_list()[0]}")

        assert raw_df["value"].to_list()[0] == 100.0

        # In reality the use would probably store these in a config file / CSV

        conversions = (
            pl.DataFrame(
                {
                    "parameterId": [228164],
                    "addition_value": [None],
                    "subtraction_value": [None],
                    "multiplication_value": [0.01],
                    "division_value": [None],
                    "ceiling_value": [None],
                }
            ).select(
                pl.col("parameterId"),
                pl.col("addition_value").cast(pl.Float64),
                pl.col("subtraction_value").cast(pl.Float64),
                pl.col("multiplication_value").cast(pl.Float64),
                pl.col("division_value").cast(pl.Float64),
                pl.col("ceiling_value").cast(pl.Float64),
            )
        ).to_arrow()

        reader = (
            GribReader(f"{resource}{os.sep}gep01.t00z.pgrb2a.0p50.f003")
            .withStations(stations)
            .withConversions(conversions)
        )

        converted_df = self.get_tcc_df(reader)

        assert converted_df["value"].to_list()[0] == 1
