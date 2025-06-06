#!/usr/bin/env python3

import logging

logging.basicConfig(level=logging.INFO)

from src.db import DuckDb, CopyTable
from src.util import get_config

DEBUG = False

target_duckdb_db = "local_taxa.duckdb"
config = get_config()
db_config = config["source_database"]

duckdb = DuckDb.create()

duckdb.load_extension("mysql")
duckdb.connect_to_mysql(
    "mysqldb",
    host=db_config["host"],
    port=db_config["port"],
    user=db_config["user"],
    database=db_config["database"],
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
