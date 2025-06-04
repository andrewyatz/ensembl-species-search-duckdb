from .db import DuckDb, SQLiteDb, CopyTable, CreateIndex, CreateSQLiteFTS
import logging


class Taxonomy:
    def __init__(self, duckdb: DuckDb):
        self.duckdb = duckdb

    def run(self):
        logging.info("Copying taxonomy and organism tables from MySQL")
        self._copy_tables()
        logging.info("Computing Taxonomy hierarchy")
        self._create_ncbi_hierarchy_lookup()
        logging.info("Creating taxonomy names lookup")
        self._create_taxonomy_names()
        logging.info("Cleaning up imported unused tables")
        self._cleanup()

    def _copy_tables(self):
        current_catalog = self.duckdb.current_catalog()
        CopyTable(
            self.duckdb.con,
            "mysqldb",
            current_catalog,
            "organism",
            "organism",
            ["taxonomy_id"],
        ).run()

        CopyTable(
            self.duckdb.con,
            "mysqldb",
            current_catalog,
            "ncbi_taxa_node",
            "ncbi_taxa_node",
            ["taxon_id", "parent_id"],
        ).run()

        CopyTable(
            self.duckdb.con,
            "mysqldb",
            current_catalog,
            "ncbi_taxa_name",
            "ncbi_taxa_name",
            [["taxon_id", "name_class"]],
        ).run()

    def _cleanup(self):
        self.duckdb.drop_tables(["ncbi_taxa_node", "ncbi_taxa_name"])

    def _create_ncbi_hierarchy_lookup(self):
        logging.info("Creating the NCBI hierarchy lookup")
        sql = """
    create table computed_hierarchy AS
    WITH RECURSIVE TaxonHierarchy AS (
    SELECT
        n.taxon_id,
        n.parent_id,
        ARRAY[o.taxonomy_id] AS path_array,
        CASE
            WHEN n.taxon_id = o.taxonomy_id AND n.parent_id IS NOT NULL AND n.parent_id <> o.taxonomy_id
            THEN ARRAY[n.parent_id]
            ELSE ARRAY[]::INTEGER[]
        END AS ancestor_array
    FROM
        ncbi_taxa_node n
    INNER JOIN
        (SELECT DISTINCT taxonomy_id FROM organism) o ON n.taxon_id = o.taxonomy_id

    UNION ALL

    SELECT
        n.taxon_id,
        n.parent_id,
        array_append(th.path_array, n.taxon_id) AS path_array,
        CASE
            WHEN n.parent_id IS NOT NULL AND NOT array_contains(th.ancestor_array, n.parent_id) AND n.parent_id <> th.taxon_id
            THEN array_append(th.ancestor_array, n.parent_id)
            ELSE th.ancestor_array
        END AS ancestor_array
    FROM
        ncbi_taxa_node n
    INNER JOIN
        TaxonHierarchy th ON n.taxon_id = th.parent_id
    WHERE
        n.parent_id IS NOT NULL AND n.parent_id <> th.taxon_id
    ),
    RankedHierarchy AS (
        SELECT
            taxon_id,
            path_array[1] AS start_taxon_id,
            ancestor_array,
            array_length(path_array) AS depth,
            ROW_NUMBER() OVER (PARTITION BY path_array[1] ORDER BY array_length(path_array) DESC) AS rn
        FROM
            TaxonHierarchy
    )
    SELECT
        o.taxonomy_id AS organism_taxonomy_id,
        COALESCE((SELECT rh.ancestor_array FROM RankedHierarchy rh WHERE rh.start_taxon_id = o.taxonomy_id AND rh.rn = 1), ARRAY[]::INTEGER[]) AS ancestor_taxon_ids
    FROM
        (SELECT DISTINCT taxonomy_id FROM organism) o;
"""
        self.duckdb.con.execute(sql)
        CreateIndex(
            con=self.duckdb.con,
            table="computed_hierarchy",
            columns=["organism_taxonomy_id"],
        ).run()
        logging.info("Finished building names")

    def _create_taxonomy_names(self):
        logging.info("Creating the taxonomy names")
        sql = """
CREATE TABLE taxonomy_names AS
SELECT n1.taxon_id as taxonomy_id, n1.name as scientific_name, n2.name as genbank_common_name, n3.name as common_name, n4.name as equivalent_name
FROM ncbi_taxa_node ntn
JOIN ncbi_taxa_name n1 on ntn.taxon_id = n1.taxon_id and n1.name_class = 'scientific name'
LEFT JOIN ncbi_taxa_name n2 on n1.taxon_id = n2.taxon_id and n2.name_class = 'genbank common name'
LEFT JOIN ncbi_taxa_name n3 on n1.taxon_id = n3.taxon_id and n3.name_class = 'common name'
LEFT JOIN ncbi_taxa_name n4 on n1.taxon_id = n4.taxon_id and n4.name_class = 'equivalent name'
WHERE ntn.genbank_hidden_flag = 0
"""
        self.duckdb.con.execute(sql)
        logging.info("Finished building names")


class TaxonomySQLiteFts(CreateSQLiteFTS):
    def __init__(
        self, duckdb: DuckDb, sqlite: SQLiteDb, indexed_table="taxonomy_names"
    ):
        super().__init__(duckdb=duckdb, sqlite=sqlite, indexed_table=indexed_table)

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
        sqlite_con.execute(self.fts_ddl())
        logging.info("Populating FTS schema from DuckDB")
        sqlite_con.execute(self.fts_sql())
        sqlite_con.execute(f"DROP TABLE {indexed_table}")
        logging.info("Committing")
        sqlite_con.commit()
        logging.info("Finished")

    def fts_ddl(self):
        return """
    CREATE VIRTUAL TABLE taxonomy_fts USING fts5(
    taxonomy_id,
    scientific_name,
    genbank_common_name,
    common_name,
    equivalent_name,
    search_boost,
    tokenize='unicode61'
)
"""

    def fts_sql(self):
        return """INSERT INTO taxonomy_fts (
            taxonomy_id,
            scientific_name,
            genbank_common_name,
            common_name,
            equivalent_name,
            search_boost)
        SELECT 
            taxonomy_id,
            scientific_name,
            genbank_common_name,
            common_name,
            equivalent_name,
            0
        FROM taxonomy_names
"""
