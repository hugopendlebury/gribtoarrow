import polars as pl
import pytest
import os
import sys
import pyarrow

class TestLocations:

    def test_default_fields_present(self, resource):
        from gribtoarrow import GribReader

        stations = pl.DataFrame(
            {"lat": [51.5054, 53.4808], "lon": [-0.027176, 2.2426]}
        ).to_arrow()
    
        reader = GribReader(
            str(resource) + "/gep01.t00z.pgrb2a.0p50.f003"
        ).withLocations(stations)

        df = pl.concat(
            pl.from_arrow(message.getDataWithLocations()) for message in reader
        )

        assert 'parameterId' in df.columns
        assert 'modelNo' in df.columns
        assert 'lat' in df.columns
        assert 'lon' in df.columns
        assert 'distance' in df.columns
        assert 'nearestlatitude' in df.columns
        assert 'nearestlongitude' in df.columns
        assert 'value' in df.columns
        assert 'datetime' in df.columns
        assert 'forecast_date' in df.columns



    def test_find_nearest(self, resource):
        from gribtoarrow import GribReader

        raw_reader = GribReader(str(resource) + "/gep01.t00z.pgrb2a.0p50.f003")
        # Call getData on reader class without setting any location based data
        df = pl.concat(pl.from_arrow(message.getData()) for message in raw_reader)

        # With 85 messages and 259920 points per message size should be 22093200
        assert len(df) == 22093200

        # Locations are Canary Wharf and Manchester
        stations = pl.DataFrame(
            {"lat": [51.5054, 53.4808], "lon": [-0.027176, 2.2426]}
        ).to_arrow()

        reader = GribReader(
            str(resource) + "/gep01.t00z.pgrb2a.0p50.f003"
        ).withLocations(stations)

        df = pl.concat(
            pl.from_arrow(message.getDataWithLocations()) for message in reader
        )

        # The underlying Grib has 85 messages so we expect 170 rows

        assert len(df) == 170

    def test_passthrough_columns(self, resource):
        #Any fields added to the location metadata lookup should be passed through after find nearest is performed
        from gribtoarrow import GribReader

        stations = pl.DataFrame(
            {"lat": [51.5054, 53.4808], 
             "lon": [-0.027176, 2.2426],
             "name": ["Canary Wharf", "Manchester"],
             "awesome_factor": ["Dull", "Buzzing"],
             "beer" : ["London Pride", "Boddingtons"],
             "band" : ["blur", "oasis"]}
        ).to_arrow()

        reader = GribReader(
            str(resource) + "/gep01.t00z.pgrb2a.0p50.f003"
        ).withLocations(stations)

        df : pl.DataFrame = pl.concat(
            pl.from_arrow(message.getDataWithLocations()) for message in reader
        )

        # The underlying Grib has 85 messages so we expect 170 rows
        assert len(df) == 170
        columns =  set(df.columns)
        print(f"YO I got the following columns {columns}")
        assert 'lat' in columns        
        assert 'lon' in columns
        assert 'name' in columns
        assert 'awesome_factor' in columns
        assert 'beer' in columns
        manc = df.filter(pl.col('name')=='Manchester').select(['lat','lon','name','awesome_factor','beer','band']).unique()
        assert all('Buzzing' == field for field in manc['awesome_factor'])
        assert all('Boddingtons' == field for field in manc['beer'])
        assert all('oasis' == field for field in manc['band'])

