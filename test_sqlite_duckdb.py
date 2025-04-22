import duckdb
import sqlite3
import os

DEBUG = False

target_sqlite_db = "test.sqlite"
con = duckdb.connect(":memory:")

con.execute("INSTALL mysql;")
con.execute("LOAD mysql;")
con.execute("INSTALL sqlite;")
con.execute("LOAD sqlite;")

con.execute(
    """
    ATTACH 'host=mysql-ens-production-1.ebi.ac.uk user=ensro port=4721 database=ensembl_genome_metadata' 
    AS mysqldb (TYPE mysql)
"""
)
if os.path.exists(target_sqlite_db):
  os.remove(target_sqlite_db)
con.execute("ATTACH 'test.sqlite' as sqlitedb (TYPE sqlite)")

# con.execute("create table ncbi_taxa_node as select * from mysqldb.ncbi_taxa_node")
# con.execute("create index ncbitn_idx on ncbi_taxa_node(taxon_id)")
# con.execute("create index ncbitn_parent_idx on ncbi_taxa_node(parent_id)")

con.execute("use sqlitedb")

print("Creating species table")
con.execute("""
    CREATE TABLE species (
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
    label VARCHAR,
    taxonomy_id BIGINT,
    species_taxonomy_id BIGINT,
    search_boost BIGINT
)
""")
con.execute(
    """
    insert into species
    select a.accession, a.name, a.assembly_default, a.tol_id, a.ensembl_name, a.assembly_uuid, a.url_name, g.genome_uuid, g.production_name, o.common_name, o.scientific_name, o.biosample_id, o.strain, er.is_current, er.label, o.taxonomy_id, o.species_taxonomy_id, 0 as search_boost
    from mysqldb.assembly a
    join mysqldb.genome g on a.assembly_id = g.assembly_id
    join mysqldb.organism o on g.organism_id = o.organism_id
    join mysqldb.genome_release gr on g.genome_id = gr.genome_id
    join mysqldb.ensembl_release er on gr.release_id = er.release_id;
"""
)

boost = {"GRCh38.p14": 1000, "GRCh37.p13": 900, "%T2T%": 500}
for ensembl_name in boost:
    con.execute(
        "UPDATE species set search_boost =? where ensembl_name like ?",
        (boost[ensembl_name], ensembl_name),
    )

print("Building indexes including fts")
species_indexes = ("taxonomy_id", "genome_uuid")
for col in species_indexes:
    con.execute(f"create index species_{col}_idx on species({col});")

con.execute("use memory")
con.execute("detach sqlitedb");

sqlite3_con = sqlite3.connect(target_sqlite_db)
sqlite3_con.execute(
    """
    CREATE VIRTUAL TABLE species_fts USING fts5(
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
    label,
    tokenize='unicode61'
);
"""
)
sqlite3_con.execute("""
  INSERT INTO species_fts (genome_uuid, accession, name, assembly_default, tol_id, ensembl_name, production_name, common_name, scientific_name, biosample_id, strain, label)
SELECT genome_uuid, accession, name, assembly_default, tol_id, ensembl_name, production_name, common_name, scientific_name, biosample_id, strain, label
FROM species
""")
sqlite3_con.commit()

if DEBUG:
  cursor = sqlite3_con.cursor()
  cursor.execute("""
  SELECT f.genome_uuid, f.common_name, f.scientific_name, s.accession, s.ensembl_name, s.assembly_default, s.strain, bm25(species_fts) AS score, s.search_boost
  FROM species_fts f 
  JOIN species s on f.genome_uuid = s.genome_uuid
  WHERE species_fts MATCH 'hom* barb*'
  order by s.search_boost desc, score desc
  limit 10
  """)
  results = cursor.fetchall()
  print(results)

sqlite3_con.close()
con.close()