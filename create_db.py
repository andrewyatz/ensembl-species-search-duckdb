import duckdb

target_db = "searcher.duckdb"

con = duckdb.connect(":memory:")
con.execute("INSTALL mysql;")
con.execute("LOAD mysql;")
con.execute("INSTALL fts;")
con.execute("LOAD fts;")
con.execute(
    """
    ATTACH 'host=mysql-ens-production-1.ebi.ac.uk user=ensro port=4721 database=ensembl_genome_metadata' 
    AS mysqldb (TYPE mysql);
"""
)

print("Creating species table")
con.execute(
    """
    create table species as
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
con.execute(
    """
    PRAGMA create_fts_index('species', 'genome_uuid', 'accession', 'name', 'assembly_default', 'tol_id', 'ensembl_name', 'assembly_uuid', 'url_name', 'genome_uuid', 'production_name', 'common_name', 'scientific_name', 'biosample_id', 'strain', 'label', overwrite=1);
"""
)
species_indexes = ("taxonomy_id", "genome_uuid")
for col in species_indexes:
    con.execute(f"create index species_{col}_idx on species({col});")

## Now create taxonomy walker
print("Creating taxonomy lookup table")
print("Copying and indexing from mysql")
con.execute("create table organism as select * from mysqldb.organism;")
con.execute("create table ncbi_taxa_node as select * from mysqldb.ncbi_taxa_node;")
con.execute("create index organism_taxonomy_id_idx on organism (taxonomy_id);")
con.execute("create index ncbi_taxa_node_taxon_id_idx on ncbi_taxa_node (taxon_id);")
con.execute("create index ncbi_taxa_node_parent_id_idx on ncbi_taxa_node (parent_id);")

print("Creating taxonomy lookup")
con.execute(
    """
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
)

con.execute("drop table ncbi_taxa_node")
con.execute("drop table organism")

## Write the database out to disk
print("Writing to disk")
con.execute(f"attach '{target_db}' as target_db;")
con.execute("copy from database memory to target_db;")
con.execute("detach target_db;")
