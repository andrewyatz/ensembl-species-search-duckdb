#!/usr/bin/env python3

from src.db import DuckDb, CopyTable
from src.config import get_config

config = get_config()
config.enable_logging()

target_duckdb_db = config.lookups.local_taxonomy
db_config = config.source_database

duckdb = DuckDb.create()

duckdb.load_extension("mysql")
duckdb.connect_to_mysql(
    "mysqldb",
    host=db_config.host,
    port=db_config.port,
    user=db_config.user,
    database=db_config.database,
)

CopyTable(
    duckdb.con,
    "mysqldb",
    "memory",
    "ncbi_taxa_node",
    "ncbi_taxa_node",
    ["taxon_id", "parent_id"],
).run()
CopyTable(
    duckdb.con,
    "mysqldb",
    "memory",
    "ncbi_taxa_name",
    "ncbi_taxa_name",
    [["taxon_id", "name_class"]],
).run()

duckdb.persist_database(target_duckdb_db)
