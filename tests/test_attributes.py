import pytest
import os
import sys
import datetime


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


class TestFilterMessageId:
    def test_attributes(self, resource):
        from gribtoarrow import GribReader

        reader = GribReader(f"{resource}{os.sep}gep01.t00z.pgrb2a.0p50.f003")

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
        assert 90.0 == message.getLatitudeOfFirstPoint()
        assert 0.0 == message.getLongitudeOfFirstPoint()
        assert -90.0 == message.getLatitudeOfLastPoint()
        assert 359.5 == message.getLongitudeOfLastPoint()

