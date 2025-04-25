import os
import logging

logging.basicConfig(level=logging.INFO)

from src.db import DuckDb, SQLiteDb
from src.species import Species, SpeciesFts
from src.taxonomy import Taxonomy, TaxonomySQLiteFts

DEBUG = False

target_sqlite_db = "test.sqlite"
target_duckdb_db = "test.duckdb"
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

DuckDb.persist_database(target_duckdb_db)

# # Going back to memory and detaching from our SQLite DB
# duckdb.switch("memory")
# duckdb.detach(sqlite_db_name)

# # Build FTS
# SpeciesFts(sqlite).run()

# Building Taxa Names

# cursor = duckdb.con.execute('select distinct name_class from mysqldb.ncbi_taxa_name')
# results = cursor.fetchall()
# print(results)

# Types of names
# "scientific name"
# "genbank common name"
# "common name"
# "equivalent name"

# CopyTable(
#     duckdb.con,
#     "mysqldb",
#     "memory",
#     "organism",
#     "organism",
#     ["taxonomy_id"],
# ).run()

# CopyTable(
#     duckdb.con,
#     "mysqldb",
#     "memory",
#     "ncbi_taxa_node",
#     "ncbi_taxa_node",
#     ["taxon_id", "parent_id"],
# ).run()

# CopyTable(
#     duckdb.con,
#     "mysqldb",
#     "memory",
#     "ncbi_taxa_name",
#     "ncbi_taxa_name",
#     [["taxon_id", "name_class"]],
# ).run()

# duckdb.con.execute(
#     """
#     create table computed_hierarchy AS
#     WITH RECURSIVE TaxonHierarchy AS (
#     SELECT
#         n.taxon_id,
#         n.parent_id,
#         ARRAY[o.taxonomy_id] AS path_array,
#         CASE
#             WHEN n.taxon_id = o.taxonomy_id AND n.parent_id IS NOT NULL AND n.parent_id <> o.taxonomy_id
#             THEN ARRAY[n.parent_id]
#             ELSE ARRAY[]::INTEGER[]
#         END AS ancestor_array
#     FROM
#         ncbi_taxa_node n
#     INNER JOIN
#         (SELECT DISTINCT taxonomy_id FROM organism) o ON n.taxon_id = o.taxonomy_id

#     UNION ALL

#     SELECT
#         n.taxon_id,
#         n.parent_id,
#         array_append(th.path_array, n.taxon_id) AS path_array,
#         CASE
#             WHEN n.parent_id IS NOT NULL AND NOT array_contains(th.ancestor_array, n.parent_id) AND n.parent_id <> th.taxon_id
#             THEN array_append(th.ancestor_array, n.parent_id)
#             ELSE th.ancestor_array
#         END AS ancestor_array
#     FROM
#         ncbi_taxa_node n
#     INNER JOIN
#         TaxonHierarchy th ON n.taxon_id = th.parent_id
#     WHERE
#         n.parent_id IS NOT NULL AND n.parent_id <> th.taxon_id
#     ),
#     RankedHierarchy AS (
#         SELECT
#             taxon_id,
#             path_array[1] AS start_taxon_id,
#             ancestor_array,
#             array_length(path_array) AS depth,
#             ROW_NUMBER() OVER (PARTITION BY path_array[1] ORDER BY array_length(path_array) DESC) AS rn
#         FROM
#             TaxonHierarchy
#     )
#     SELECT
#         o.taxonomy_id AS organism_taxonomy_id,
#         COALESCE((SELECT rh.ancestor_array FROM RankedHierarchy rh WHERE rh.start_taxon_id = o.taxonomy_id AND rh.rn = 1), ARRAY[]::INTEGER[]) AS ancestor_taxon_ids
#     FROM
#         (SELECT DISTINCT taxonomy_id FROM organism) o;
# """
# )

# sql = """
# CREATE TABLE taxonomy_names AS
# SELECT n1.taxon_id as taxonomy_id, n1.name as scientific_name, n2.name as genbank_common_name, n3.name as common_name, n4.name as equivalent_name
# FROM ncbi_taxa_node ntn
# JOIN ncbi_taxa_name n1 on ntn.taxon_id = n1.taxon_id and n1.name_class = 'scientific name'
# LEFT JOIN ncbi_taxa_name n2 on n1.taxon_id = n2.taxon_id and n2.name_class = 'genbank common name'
# LEFT JOIN ncbi_taxa_name n3 on n1.taxon_id = n3.taxon_id and n3.name_class = 'common name'
# LEFT JOIN ncbi_taxa_name n4 on n1.taxon_id = n4.taxon_id and n4.name_class = 'equivalent name'
# WHERE ntn.genbank_hidden_flag = 0
# """
# duckdb.con.execute(sql)

# TaxonomySQLiteFts(duckdb=duckdb, sqlite=sqlite).run()

# # duckdb.con.execute("drop table ncbi_taxa_name")
# for t in ("ncbi_taxa_name", "ncbi_taxa_node"):
#     duckdb.con.execute(f"DROP TABLE {t}")

# if os.path.exists(target_duckdb_db):
#     os.unlink(target_duckdb_db)

# duckdb.connect_to_duckdb(name="persist", path=target_duckdb_db)
# duckdb.con.execute("copy from database memory to persist")

# con = duckdb.connect(":memory:")

# con.execute("INSTALL mysql;")
# con.execute("LOAD mysql;")
# con.execute("INSTALL sqlite;")
# con.execute("LOAD sqlite;")

# con.execute(
#     """
#     ATTACH 'host=mysql-ens-production-1.ebi.ac.uk user=ensro port=4721 database=ensembl_genome_metadata'
#     AS mysqldb (TYPE mysql)
# """
# )
# if os.path.exists(target_sqlite_db):
#     os.remove(target_sqlite_db)
# con.execute("ATTACH 'test.sqlite' as sqlitedb (TYPE sqlite)")

# con.execute("create table ncbi_taxa_node as select * from mysqldb.ncbi_taxa_node")
# con.execute("create index ncbitn_idx on ncbi_taxa_node(taxon_id)")
# con.execute("create index ncbitn_parent_idx on ncbi_taxa_node(parent_id)")

# con.execute("use sqlitedb")

# print("Creating species table")
# con.execute(
#     """
#     CREATE TABLE species (
#     accession VARCHAR,
#     name VARCHAR,
#     assembly_default VARCHAR,
#     tol_id VARCHAR,
#     ensembl_name VARCHAR,
#     assembly_uuid VARCHAR,
#     url_name VARCHAR,
#     genome_uuid VARCHAR,
#     production_name VARCHAR,
#     common_name VARCHAR,
#     scientific_name VARCHAR,
#     biosample_id VARCHAR,
#     strain VARCHAR,
#     is_current BIGINT,
#     label VARCHAR,
#     taxonomy_id BIGINT,
#     species_taxonomy_id BIGINT,
#     search_boost BIGINT
# )
# """
# )
# con.execute(
#     """
#     insert into species
#     select a.accession, a.name, a.assembly_default, a.tol_id, a.ensembl_name, a.assembly_uuid, a.url_name, g.genome_uuid, g.production_name, o.common_name, o.scientific_name, o.biosample_id, o.strain, er.is_current, er.label, o.taxonomy_id, o.species_taxonomy_id, 0 as search_boost
#     from mysqldb.assembly a
#     join mysqldb.genome g on a.assembly_id = g.assembly_id
#     join mysqldb.organism o on g.organism_id = o.organism_id
#     join mysqldb.genome_release gr on g.genome_id = gr.genome_id
#     join mysqldb.ensembl_release er on gr.release_id = er.release_id;
# """
# )

# boost = {"GRCh38.p14": 1000, "GRCh37.p13": 900, "%T2T%": 500}
# for ensembl_name in boost:
#     con.execute(
#         "UPDATE species set search_boost =? where ensembl_name like ?",
#         (boost[ensembl_name], ensembl_name),
#     )

# print("Building indexes including fts")
# species_indexes = ("taxonomy_id", "genome_uuid")
# for col in species_indexes:
#     con.execute(f"create index species_{col}_idx on species({col});")

# con.execute("use memory")
# con.execute("detach sqlitedb")

# sqlite3_con = sqlite.con
# # sqlite3_con = sqlite3.connect(target_sqlite_db)
# sqlite3_con.execute(
#     """
#     CREATE VIRTUAL TABLE species_fts USING fts5(
#     genome_uuid,
#     accession,
#     name,
#     assembly_default,
#     tol_id,
#     ensembl_name,
#     production_name,
#     common_name,
#     scientific_name,
#     biosample_id,
#     strain,
#     label,
#     tokenize='unicode61'
# );
# """
# )
# sqlite3_con.execute(
#     """
#   INSERT INTO species_fts (genome_uuid, accession, name, assembly_default, tol_id, ensembl_name, production_name, common_name, scientific_name, biosample_id, strain, label)
# SELECT genome_uuid, accession, name, assembly_default, tol_id, ensembl_name, production_name, common_name, scientific_name, biosample_id, strain, label
# FROM species
# """
# )
# sqlite3_con.commit()

# if DEBUG:
#     cursor = sqlite3_con.cursor()
#     cursor.execute(
#         """
#   SELECT f.genome_uuid, f.common_name, f.scientific_name, s.accession, s.ensembl_name, s.assembly_default, s.strain, bm25(species_fts) AS score, s.search_boost
#   FROM species_fts f
#   JOIN species s on f.genome_uuid = s.genome_uuid
#   WHERE species_fts MATCH 'hom* barb*'
#   order by s.search_boost desc, score desc
#   limit 10
#   """
#     )
#     results = cursor.fetchall()
#     print(results)

# sqlite3_con.close()
# con.close()
