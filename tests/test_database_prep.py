import unittest

from src.db import DuckDb
from tests.util import DatabaseFixture
from os import path


class TestDatabaseLoad(unittest.TestCase):
    def test_load(self):
        """
        Test we can load the database tables from parquet dumps
        and the counts are as expected
        """
        duckdb = DuckDb.create()
        self.assertTrue(duckdb)
        report = self.load_tables(duckdb)
        for t in report:
            name = t["name"]
            counts = t["counts"]
            self.assertEqual(
                first=counts["expected"],
                second=counts["actual"],
                msg=f"Assert count of table {name}",
            )

    def load_tables(self, duckdb):
        tables = [
            {"name": "ncbi_taxa_node", "count": 2652282},
            {"name": "ncbi_taxa_name", "count": 4442575},
            {"name": "assembly", "count": 3647},
            {"name": "ensembl_release", "count": 11},
            {"name": "genome_release", "count": 6546},
            {"name": "genome", "count": 3692},
            {"name": "organism", "count": 2892},
        ]
        loaded_count = DatabaseFixture(duckdb=duckdb).load_tables()
        self.assertEqual(
            first=len(tables),
            second=loaded_count,
            msg="Checking we loaded the right number of tables",
        )
        con = duckdb.con
        reported_counts = []
        for t in tables:
            name = t["name"]
            con.execute(f"select count(*) from {name}")
            reported_counts.append(
                {
                    "name": name,
                    "counts": {"expected": t["count"], "actual": con.fetchone()[0]},
                }
            )
        return reported_counts


if __name__ == "__main__":
    unittest.main()
