#!/usr/bin/env python3

from src.config import get_config
from src.db import DuckDb
import os
import logging
import datetime
import json

config = get_config()
config.enable_logging()
log = logging.getLogger()
duckdb = DuckDb.create()
duckdb.load_extension("mysql")

db = config.source_database
duckdb.connect_to_mysql(
    "mysqldb",
    host=db.host,
    port=db.port,
    user=db.user,
    database=db.database,
)

tables = [
    "ncbi_taxa_node",
    "ncbi_taxa_name",
    "assembly",
    "genome",
    "organism",
    "genome_release",
    "ensembl_release",
]
data_format = "parquet"
compression = "brotli"

con = duckdb.con
current_dir = os.path.dirname(__file__)
dir = os.path.join(current_dir, "tests", "data")
for table in tables:
    source_table = f"mysqldb.{table}"
    log.info(f"Copying {source_table} to {dir}")
    duckdb.write_table_to_disk(
        table=table,
        dir=dir,
        data_format=data_format,
        compression=compression,
        schema="mysqldb",
    )
    log.info(f"Finished copying {table}")

log.info("Printing summary")
now = datetime.datetime.now()
summary = {
    "tables": tables,
    "data_format": data_format,
    "compression": compression,
    "datetime": now.isoformat(),
}

with open(os.path.join(dir, "dump_report.json"), "w") as f:
    json.dump(summary, fp=f, sort_keys=True, indent=4)
