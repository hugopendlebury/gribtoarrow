import datetime
import math
import polars as pl

class TestFilterMessageId:
    def test_attributes(self, resource):
        from gribtoarrow import GribReader

        reader = GribReader(str(resource) + "/gep01.t00z.pgrb2a.0p50.f003")

        message = next(iter(reader))

        assert 156 == message.getParameterId()
        assert 0 == message.getGribMessageId()
        assert 1 == message.getModelNumber()
        assert 3 == message.getStep()
        assert "h" == message.getStepUnits()
        assert "gh" == message.getShortName()
        assert "20231208" == message.getDate()
        assert "0000" == message.getTime()
        assert 20231208 == message.getDateNumeric()
        assert 0 == message.getTimeNumeric()
        assert datetime.datetime(2023, 12, 8) == message.getChronoDate()
        assert datetime.datetime(2023, 12, 8, 3, 0) == message.getObsDate()
        assert 90.0 == message.getLatitudeOfFirstPoint()
        assert 0.0 == message.getLongitudeOfFirstPoint()
        assert -90.0 == message.getLatitudeOfLastPoint()
        assert 359.5 == message.getLongitudeOfLastPoint()
        assert 2 == message.getEditionNumber()
        assert message.iScansNegatively() is False
        assert message.jScansPositively() is False
        assert message.getDataType() == "pf"

    def test_default_missingKeys(self, resource):
        from gribtoarrow import GribReader

        reader = GribReader(str(resource) + "/gep01.t00z.pgrb2a.0p50.f003")

        message = next(iter(reader))
        #key exists so should get value
        assert 156 == message.getNumericParameterOrDefault("paramId")
        #key doesn't exist (deliberate typo and no default specified)
        assert -9999 == message.getNumericParameterOrDefault("paramid")
        #key doesn't exist and default specified
        assert -123 == message.getNumericParameterOrDefault("paramid", -123)
        #key doesn't exist and default specified as namedArg
        assert -123 == message.getNumericParameterOrDefault("paramid", defaultValue=-123)
        #key exists should return value
        assert 90.0 == message.getDoubleParameterOrDefault("latitudeOfFirstGridPointInDegrees")
        #key doesn't exist should return value (NaN)
        result = message.getDoubleParameterOrDefault("latitudeOfFirstGridPointInDegreesPleaseBoss")
        assert(math.isnan(result) == True)
        #key doesn't exist default specified should return requested default
        result = message.getDoubleParameterOrDefault("latitudeOfFirstGridPointInDegreesPleaseBoss", 99.99)
        assert round(result,2) == round(99.99,2)
        assert "gh" == message.getStringParameterOrDefault("shortName")
        #String key does exist should return default (empty string)
        assert "" == message.getStringParameterOrDefault("elChapo")
        #String key does exist should return user specified key
        assert "shorty" == message.getStringParameterOrDefault("elChapo", "shorty")

    def test_tryGetKey(self, resource):
        from gribtoarrow import GribReader

        # Locations are Canary Wharf,Manchester & kristiansand
        locations = pl.DataFrame(
            {"lat": [51.5054, 53.4808, 58.1599], "lon": [-0.027176, 2.2426, 8.0182]}
        ).to_arrow()

        reader = GribReader(str(resource) + "/gep01.t00z.pgrb2a.0p50.f003").withLocations(locations)

        message = next(iter(reader))
        #key exists so should get value
        assert 156 == message.tryGetKey("paramId")
        #key doesn't exist (deliberate typo and no default specified)
        assert None == message.tryGetKey("paramid")
        #key exists should return value
        assert 90.0 == message.tryGetKey("latitudeOfFirstGridPointInDegrees")
        #key doesn't exist should return value (NaN)
        assert None == message.tryGetKey("latitudeOfFirstGridPointInDegreesPleaseBoss")
        assert "gh" == message.tryGetKey("shortName")
        #String key does exist should return None
        assert None == message.tryGetKey("elChapo")

        #The next example it to show how we can use tryGetKey (in c# you can return a bool and use ref)
        #from python 3.8 we can do this with walrus
        messages = []
        for message in reader:
            msg = pl.from_arrow(message.getDataWithLocations()).with_columns([
                #We know this columns doesn't exist but want a different default
                pl.lit(key).alias("Shorty") if(key := message.tryGetKey("elChapo")) else pl.lit("sinaloa").alias("Shorty")
            ])
            messages.append(msg)
    
        df : pl.DataFrame = pl.concat(messages)
        assert all(x == "sinaloa" for x in df["Shorty"].to_list())
        

        

