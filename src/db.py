from typing import Optional, Sequence, Any
from abc import ABC, abstractmethod
import collections.abc
import duckdb
import sqlite3
import logging
import os


class SQLiteDb:
    @staticmethod
    def create(name, path: str = ":memory:"):
        return SQLiteDb(path, name)

    def __init__(self, name: str, path: str):
        self.name = name
        self.path = path
        self._con = None
        self._con_loaded = False

    @property
    def con(self):
        if not self._con_loaded:
            self._con = self.create_sqlite_connection()
            self._con_loaded = True
        return self._con

    def create_sqlite_connection(self):
        logging.info(f"Connecting to SQLite at {self.path}")
        sqlite3_con = sqlite3.connect(self.path)
        logging.info("Connected to SQLite")
        return sqlite3_con

    def remove_sqlite(self):
        if os.path.exists(self.path):
            logging.info(f"Removing {self.path} SQLite database")
            os.remove(self.path)


class DuckDb:
    @staticmethod
    def create(db: str = ":memory:"):
        con = duckdb.connect(db)
        return DuckDb(con, db)

    def __init__(self, con, name: str):
        self.con = con
        self.name = name

    def current_catalog(self) -> str:
        results = self.con.sql("select current_catalog()").fetchall()
        return results[0][0]

    def switch(self, catalog: str) -> str:
        previous_catalog = self.current_catalog()
        logging.info(f"Switching to {catalog}")
        self.con.execute(f"USE {catalog}")
        return previous_catalog

    def detach(self, catalog: str) -> None:
        logging.info(f"Detaching from {catalog}")
        self.con.execute(f"DETACH {catalog}")

    def load_extension(self, extension: str, install=True) -> None:
        if install:
            logging.info(f"Installing extension {extension}")
            self.con.execute(f"INSTALL {extension}")
        logging.info(f"Loading extension {extension}")
        self.con.execute(f"LOAD {extension}")

    def connect_to_mysql(
        self,
        name: str,
        host: str,
        user: str,
        database: str,
        password: str = None,
        port: int = 3306,
    ) -> None:
        logging.info(f"Connecting to MySQL as {name}")
        connection_string = f"host={host} user={user} port={port} database={database}"
        if password:
            connection_string = connection_string + "password={password}"
        sql = f"ATTACH '{connection_string}' AS mysqldb (TYPE mysql)"
        logging.debug(f"Connection string: {sql}")
        self.con.execute(sql)
        logging.info(f"MySQL attached as {name}")

    def connect_to_sqlite(self, name: str, path: str) -> None:
        logging.info(f"Connecting to SQLite as {name}")
        connection_string = f"ATTACH '{path}' as {name} (TYPE sqlite)"
        self.con.execute(connection_string)
        logging.info(f"SQLite attached as {name}")

    def connect_to_duckdb(self, name: str, path: str) -> None:
        logging.info(f"Connecting to DuckDb as {name}")
        connection_string = f"ATTACH '{path}' as {name}"
        self.con.execute(connection_string)
        logging.info(f"DuckDb attached as {name}")

    def drop_tables(self, tables: Sequence[str]) -> None:
        for table in tables:
            logging.info(f"Dropping table {table}")
            self.con.execute(f"DROP TABLE {table}")

    def persist_database(self, path: str, overwrite: bool = True) -> None:
        if overwrite and os.path.exists(path):
            os.unlink(path)
        self.connect_to_duckdb(name="persist", path=path)
        current_catalog = self.current_catalog()
        self.con.execute(f"copy from database {current_catalog} to persist")
        self.detach("persist")

    def write_table_to_disk(
        self,
        table: str,
        dir: str,
        data_format: str = "parquet",
        compression: str = "brotli",
        schema: str = None,
    ) -> None:
        """
        Write to disk at the given directory
        """
        target_file = f"{table}.{data_format}.{compression}"
        path = os.path.join(dir, target_file)
        if schema:
            source_table = f"{schema}.{table}"
        else:
            source_table = table
        sql = f"""
            COPY (SELECT * FROM {source_table})
            TO '{path}'
            (FORMAT {data_format}, COMPRESSION {compression})
        """
        logging.info(f"Copying table {source_table} into {path}")
        logging.debug(f"Copy SQL: {sql}")
        self.con.execute(sql)

    def load_parquet_to_table(self, table: str, path: str, schema: str = None) -> None:
        """
        Take a table name and a path to a parquet file and load into said table
        """
        if schema:
            target_table = f"{schema}.{table}"
        else:
            target_table = table
        sql = f"""
            create table {target_table} AS from read_parquet('{path}')
        """
        logging.info(f"Loading table {target_table} from file {path}")
        logging.debug(f"Load SQL: {sql}")
        self.con.execute(sql)

    def load_parquet_directory(self, dir, compression="brotli", schema=None) -> int:
        """
        Looks for anything in the given directory of the format NNN.parquet.compression
        and loads. Assumes that NNN is the target table name. Creates a table per
        parquet file
        """
        data_format = "parquet"
        files = os.listdir(dir)
        target_ext = f".{data_format}.{compression}"
        logging.info(f"Loading files in directory {dir}")
        loaded_count = 0
        for file in files:
            if target_ext in file:
                table = file.replace(target_ext, "")
                path = os.path.join(dir, file)
                logging.debug(f"Processing {file} to be loaded into {table}")
                self.load_parquet_to_table(table, path, schema)
                loaded_count += 1
        return loaded_count


class CreateIndex:
    def __init__(
        self, con: Any, table: str, columns: Sequence[Any], db: Optional[str] = None
    ):
        self.con = con
        self.table = table
        self.columns = columns
        self.db = db

    def run(self):
        for col in self.columns:
            target_table_name = self.table
            if self.db is not None:
                target_table_name = f"{self.db}.{self.table}"
            # Check if we were given an array/list/tuple and if so we want to index multiple columns
            indexed_columns = col
            if isinstance(col, collections.abc.Sequence) and not isinstance(col, str):
                indexed_columns = "".join([x[0] for x in col])
                col = ",".join(col)
            index_name = f"{self.table}_{indexed_columns}_idx"
            sql = f"create index {index_name} on {target_table_name}({col})"
            logging.debug(f"Index SQL: {sql}")
            self.con.execute(sql)
            logging.info(f"Created index {index_name} on {target_table_name}")


class CopyTable:
    def __init__(
        self,
        con: duckdb.DuckDBPyConnection,
        sourcedb: str,
        targetdb: str,
        source_table: str,
        target_table: Optional[str],
        index_columns: Optional[Sequence[Any]],
    ):
        self.con = con
        self.sourcedb = sourcedb
        self.targetdb = targetdb
        self.source_table = source_table
        if target_table is None:
            self.target_table = source_table
        else:
            self.target_table = target_table
        self.index = CreateIndex(
            con=con, table=target_table, columns=index_columns, db=targetdb
        )

    def run(self):
        logging.info(
            f"Copying table {self.sourcedb}.{self.source_table} to {self.targetdb}.{self.target_table}"
        )
        sql = f"create table {self.targetdb}.{self.target_table} as select * from {self.sourcedb}.{self.source_table}"
        logging.debug(f"Copy table SQL: {sql}")
        self.con.execute(sql)
        logging.info(f"Created table {self.targetdb}.{self.target_table}")
        if not self.index.columns:
            self.index.run()


class CreateSQLiteFTS(ABC):
    def __init__(self, duckdb: DuckDb, sqlite: SQLiteDb, indexed_table):
        self.duckdb = duckdb
        self.sqlite = sqlite
        self.indexed_table = indexed_table

    def run(self):
        logging.info(f"Building SQLite full-text search for {self.__class__.__name__}")
        current_catalog = self.duckdb.current_catalog()
        logging.info("Creating FTS schema in SQLite")
        indexed_table = self.indexed_table
        CopyTable(
            self.duckdb.con,
            current_catalog,
            self.sqlite.name,
            indexed_table,
            indexed_table,
            index_columns=[],
        ).run()
        sqlite_con = self.sqlite.con
        ddl = self.fts_ddl()
        logging.debug(f"{self.__class__.__name__} FTS DDL {ddl}")
        sqlite_con.execute(ddl)
        logging.info("Populating FTS schema from DuckDB")
        sql = self.fts_sql()
        logging.debug(f"{self.__class__.__name__} FTS population SQL: {sql}")
        sqlite_con.execute(sql)
        logging.info("Dropping the source table from SQLite")
        sqlite_con.execute(f"DROP TABLE {indexed_table}")
        logging.info("Committing")
        sqlite_con.commit()
        logging.info("Finished")

    @abstractmethod
    def fts_ddl(self):
        pass

    @abstractmethod
    def fts_sql(self):
        pass
