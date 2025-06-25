import logging
from .db import CreateIndex, DuckDb, SQLiteDb, CreateSQLiteFTS


class Species:

    _default_boost = {"GRCh38.p14": 1000, "GRCh37.p13": 900, "%T2T%": 500}

    def __init__(self, duckdb: DuckDb, boost=_default_boost, source_schema="mysqldb"):
        self.duckdb = duckdb
        self.boost = boost
        self.source_schema = source_schema

    def run(self):
        con = self.duckdb.con
        logging.info("Building sequence for IDs")
        con.execute(self.species_sequence_ddl())
        logging.info("Building species table")
        con.execute(self.species_ddl())
        logging.info("Populating species table")
        con.execute(self.species_sql(db=self.source_schema))
        self.apply_boost()
        logging.info("Building indexes")
        self.build_indexes()

    def species_ddl(self):
        ddl = """
        CREATE TABLE species (
        species_id INTEGER,
        accession VARCHAR,
        name VARCHAR,
        assembly_default VARCHAR,
        tol_id VARCHAR,
        ensembl_name VARCHAR,
        assembly_uuid VARCHAR,
        url_name VARCHAR,
        genome_uuid VARCHAR,
        production_name VARCHAR,
        common_name VARCHAR,
        scientific_name VARCHAR,
        biosample_id VARCHAR,
        strain VARCHAR,
        is_current BIGINT,
        release_label VARCHAR,
        release_type VARCHAR,
        taxonomy_id BIGINT,
        species_taxonomy_id BIGINT,
        search_boost BIGINT
    )
    """
        logging.debug(f"Generated DDL: {ddl}")
        return ddl

    def species_sequence_ddl(self):
        ddl = """
            CREATE SEQUENCE species_sequence START WITH 1 INCREMENT by 1
        """
        logging.debug(f"Generated DDL: {ddl}")
        return ddl

    def species_sql(self, db="mysqldb"):
        sql = f"""
    insert into species
    select
        nextval('species_sequence'),
        a.accession, a.name, a.assembly_default, a.tol_id, a.ensembl_name, a.assembly_uuid, 
        a.url_name, g.genome_uuid, g.production_name, o.common_name, o.scientific_name, 
        o.biosample_id, o.strain, er.is_current, er.label, er.release_type, o.taxonomy_id, o.species_taxonomy_id, 0 as search_boost
    from {db}.assembly a
    join {db}.genome g on a.assembly_id = g.assembly_id
    join {db}.organism o on g.organism_id = o.organism_id
    join {db}.genome_release gr on g.genome_id = gr.genome_id
    join {db}.ensembl_release er on gr.release_id = er.release_id
    where (er.release_type = 'integrated' and er.is_current = 1) 
    or (gr.is_current = 1 and er.release_type= 'partial')
"""
        logging.debug(f"Generated SQL: {sql}")
        return sql

    def apply_boost(self):
        logging.info("Applying boosted values")
        for ensembl_name in self.boost:
            sql = "UPDATE species set search_boost =? where ensembl_name like ?"
            params = (self.boost[ensembl_name], ensembl_name)
            logging.debug(f"Executing SQL '{sql}' with params '{params}'")
            self.duckdb.con.execute(sql, params)
        logging.info("Boosted")

    def build_indexes(self):
        species_indexes = ("taxonomy_id", "genome_uuid", "species_id")
        indexer = CreateIndex(self.duckdb.con, "species", species_indexes)
        indexer.run()


class SpeciesFts(CreateSQLiteFTS):
    def __init__(self, duckdb: DuckDb, sqlite: SQLiteDb, indexed_table="species"):
        super().__init__(duckdb=duckdb, sqlite=sqlite, indexed_table=indexed_table)

    def fts_ddl(self):
        return """
    CREATE VIRTUAL TABLE species_fts USING fts5(
    species_id,
    genome_uuid,
    accession,
    name,
    assembly_default,
    tol_id,
    ensembl_name,
    production_name,
    common_name,
    scientific_name,
    biosample_id,
    strain,
    release_label,
    release_type,
    taxonomy_id,
    search_boost,
    tokenize='unicode61'
)
"""

    def fts_sql(self):
        return """
    INSERT INTO species_fts (
        species_id,
        genome_uuid, 
        accession, 
        name, 
        assembly_default, 
        tol_id, 
        ensembl_name, 
        production_name, 
        common_name, 
        scientific_name, 
        biosample_id, 
        strain,
        release_label,
        release_type,
        taxonomy_id,
        search_boost)
    SELECT 
        species_id,
        genome_uuid, 
        accession, 
        name, 
        assembly_default, 
        tol_id, 
        ensembl_name, 
        production_name, 
        common_name, 
        scientific_name, 
        biosample_id, 
        strain,
        release_label,
        release_type,
        taxonomy_id,
        search_boost
    FROM species
"""
