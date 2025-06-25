#!/usr/bin/env python3
import logging

from src.db import DuckDb, SQLiteDb
from src.species import Species, SpeciesFts
from src.taxonomy import Taxonomy, TaxonomySQLiteFts
from src.config import get_config


config = get_config()
config.enable_logging()

db = config.source_database

duckdb = DuckDb.create()

duckdb.load_extension("mysql")
duckdb.load_extension("sqlite")
duckdb.connect_to_mysql(
    "mysqldb",
    host=db.host,
    port=db.port,
    user=db.user,
    database=db.database,
)

duckdb.connect_to_duckdb("taxonomy", config.lookups.local_taxonomy)

# Create the SQLite DB, switch to it and populate
sqlite_db_name = "sqlitedb"
sqlite = SQLiteDb.create(config.lookups.sqlite_fts, sqlite_db_name)
sqlite.remove_sqlite()
duckdb.connect_to_sqlite(sqlite_db_name, sqlite.path)

Species(duckdb=duckdb).run()
SpeciesFts(duckdb=duckdb, sqlite=sqlite, indexed_table="species").run()

Taxonomy(
    duckdb=duckdb,
    taxonomy_source="taxonomy",
    build_taxonomy_fts=config.lookups.build_taxonomy_fts,
).run()
if config.lookups.build_taxonomy_fts:
    TaxonomySQLiteFts(
        duckdb=duckdb, sqlite=sqlite, indexed_table="taxonomy_names"
    ).run()
else:
    logging.getLogger().info("Skipping building the Taxonomy FTS")

duckdb.persist_database(config.lookups.duckdb_search)
