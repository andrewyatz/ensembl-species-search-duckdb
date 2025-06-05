#!/usr/bin/env python3

import logging

logging.basicConfig(level=logging.INFO)

from src.db import DuckDb, SQLiteDb
from src.species import Species, SpeciesFts
from src.taxonomy import Taxonomy, TaxonomySQLiteFts

DEBUG = False

target_sqlite_db = "search_fts.sqlite"
target_duckdb_db = "search.duckdb"
duckdb = DuckDb.create()

duckdb.load_extension("mysql")
duckdb.load_extension("sqlite")
duckdb.connect_to_mysql(
    "mysqldb",
    "mysql-ens-production-1.ebi.ac.uk",
    "ensro",
    "ensembl_genome_metadata",
    port=4721,
)

# Create the SQLite DB, switch to it and populate
sqlite_db_name = "sqlitedb"
sqlite = SQLiteDb.create(target_sqlite_db, sqlite_db_name)
sqlite.remove_sqlite()
duckdb.connect_to_sqlite(sqlite_db_name, sqlite.path)

Species(duckdb=duckdb).run()
SpeciesFts(duckdb=duckdb, sqlite=sqlite, indexed_table="species").run()

Taxonomy(duckdb=duckdb).run()
TaxonomySQLiteFts(duckdb=duckdb, sqlite=sqlite, indexed_table="taxonomy_names").run()

duckdb.persist_database(target_duckdb_db)