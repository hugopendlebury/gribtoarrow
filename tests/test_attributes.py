import pytest
import os
import sys
import datetime
import pyarrow

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

