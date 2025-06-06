#!/usr/bin/env python3

import logging

logging.basicConfig(level=logging.INFO)

from src.db import DuckDb, SQLiteDb
from src.species import Species, SpeciesFts
from src.taxonomy import Taxonomy, TaxonomySQLiteFts
from src.util import get_config


target_sqlite_db = "search_fts.sqlite"
target_duckdb_db = "search.duckdb"
config = get_config()
db_config = config["source_database"]

duckdb = DuckDb.create()

duckdb.load_extension("mysql")
duckdb.load_extension("sqlite")
duckdb.connect_to_mysql(
    "mysqldb",
    host=db_config["host"],
    port=db_config["port"],
    user=db_config["user"],
    database=db_config["database"],
)

duckdb.connect_to_duckdb("taxonomy", "local_taxonomy.duckdb")

# Create the SQLite DB, switch to it and populate
sqlite_db_name = "sqlitedb"
sqlite = SQLiteDb.create(target_sqlite_db, sqlite_db_name)
sqlite.remove_sqlite()
duckdb.connect_to_sqlite(sqlite_db_name, sqlite.path)

Species(duckdb=duckdb).run()
SpeciesFts(duckdb=duckdb, sqlite=sqlite, indexed_table="species").run()

Taxonomy(duckdb=duckdb, taxonomy_source="taxonomy").run()
TaxonomySQLiteFts(duckdb=duckdb, sqlite=sqlite, indexed_table="taxonomy_names").run()

duckdb.persist_database(target_duckdb_db)
