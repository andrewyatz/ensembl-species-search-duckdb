from os import path


class TestDataDir:
    @staticmethod
    def get_file(file=None):
        basedir = path.join(path.dirname(__file__))
        if not file:
            return basedir
        return path.join(basedir, file)


class DatabaseFixture:
    def __init__(self, duckdb, dir=None, compression="brotli"):
        self.duckdb = duckdb
        self.compression = compression
        if not dir:
            self.dir = TestDataDir.get_file("data")
        else:
            self.dir = dir

    def load_tables(self) -> int:
        return self.duckdb.load_parquet_directory(dir=self.dir, compression="brotli")
