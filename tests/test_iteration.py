import polars as pl
import pytest
import os
import sys
import pyarrow
from functools import reduce

class TestIteration:

    def get_iterator_count(self, reader):
        return sum(1 for _ in reader)

    def test_iterate_once(self, resource):
        from gribtoarrow import GribReader

        reader = GribReader(str(resource) + "/meps_weatherapi_sorlandet.grb")
        
        cnt = self.get_iterator_count(reader)
        assert cnt == 268
        cnt = self.get_iterator_count(reader)
        assert cnt == 0

    def test_iterate_repeatable(self, resource):
        from gribtoarrow import GribReader

        reader = GribReader(str(resource) + "/meps_weatherapi_sorlandet.grb").withRepeatableIterator(True)
        
        cnt = self.get_iterator_count(reader)
        assert cnt == 268
        cnt = self.get_iterator_count(reader)
        assert cnt == 268