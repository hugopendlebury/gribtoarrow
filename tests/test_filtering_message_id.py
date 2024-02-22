import polars as pl

class TestFilterMessageId:
    def test_filterByMessageId(self, resource):
        from gribtoarrow import GribReader

        reader = GribReader(str(resource) + "/gep01.t00z.pgrb2a.0p50.f003")

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
