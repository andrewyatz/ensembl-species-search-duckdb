import unittest
import os
import tempfile
from src.db import DuckDb, SQLiteDb
from src.species import Species, SpeciesFts
from tests.util import DatabaseFixture

class TestCreateSpeciesLookup(unittest.TestCase):
  def test_species_creation(self):
      """
      Runs the basic creation script but writes persistent databases 
      to a tmpdir and does very basic prodding.
      """
      tmpdir = tempfile.mkdtemp()
      sqlite_db_name = "sqlitedb"
      sqlite_path = os.path.join(tmpdir, 'species_fts.sqlite')

      duckdb = DuckDb.create()
      sqlite = SQLiteDb.create(sqlite_path, sqlite_db_name)
      
      dbfixture = DatabaseFixture(duckdb=duckdb)
      dbfixture.load_tables()
      
      species = Species(duckdb=duckdb, source_schema="memory")
      species.run()

      duckdb.con.execute("select count(*) from species")
      self.assertEqual(first=6423, second=duckdb.con.fetchone()[0])
      
      sqlite.remove_sqlite()
      duckdb.connect_to_sqlite(sqlite_db_name, sqlite.path)
      species_fts = SpeciesFts(duckdb=duckdb, sqlite=sqlite, indexed_table="species")
      species_fts.run()

      cursor = sqlite.con.cursor()
      cursor.execute("SELECT count(*) FROM species_fts s WHERE s.species_fts MATCH ?", ("homo sap*",))
      self.assertEqual(first=416, second=cursor.fetchone()[0])
      cursor.close()
