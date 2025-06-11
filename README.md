# Ensembl Species Search REST API with DuckDB and SQLite

This project provides a basic REST API that interacts with a DuckDB and SQLite database to find genomes and species hosted in Ensembl.

## Running the web app

```bash
uvicorn main:app
```

Navigate to <http://127.0.0.1:8000/docs> to find the OpenAPI spec descriptions.

## Resources created

By running the `generate_lookups.py` script we generate two resources

- `search.duckdb`: contains taxonomy hierachy lookups, taxonomy names and species information
- `search_fts.sqlite`: contains full-text searching for the taxonomy and species names

## Generating a local copy of taxonomy

Due to the size of the taxonomy tables you can take a local copy of these using `build_local_taxa_tables.py`. This will generate a duckdb file called `local_taxonomy.duckdb`.

## Config file

You can provide a TOML file to the program to configure the building of the resources. An example is provided in `config.toml.example`. The program will attempt to load this from:

- The environment variable `SEARCH_CONFIG` which is set to a path to the file
- Defaults to `config.toml`

### DuckDB tables

### Computed tables

- `species` : computed table from Ensembl metadata schema bringing together multiple names for species. Input for FTS
- `taxonomy_names` : selection of names (scientific name, genbank common name, common name, equivalent name) for taxonomy. Input for FTS
- `computed_hierachy` : for each Ensembl organism/taxonomy node a precomputed hierachy travelsal of the taxonomy

### Copies

- `organism` : direct copy from the Ensembl metadata schema

### SQLite tables

All SQLite tables are FTS5 tables created from those held in DuckDB

- `species_fts` : full-text version of `species`
- `taxonomy_fts` : full-text version of `taxonomy_names`

## Running FTS queries

### Species

Find all entries which involve the text hom and barb.

```sql
SELECT genome_uuid, common_name, scientific_name, accession, ensembl_name, assembly_default, strain, bm25(species_fts) AS score, search_boost
FROM species_fts f
WHERE species_fts MATCH 'hom* barb*'
order by search_boost desc, score desc
```

This at time of writing will pull back human assemblies from HPRC linked to Barbados.

```text
fc3df422-514f-4b31-a9fb-a1b6ddd762d4|Human|Homo sapiens|GCA_018467005.1|HG02486.alt.pat.f1_v2|HG02486.alt.pat.f1_v2|African Caribbean in Barbados|-7.49702114914051|0
df605024-0fbb-47eb-8769-4ef47d76d04c|Human|Homo sapiens|GCA_018466835.1|HG02257.alt.pat.f1_v2|HG02257.alt.pat.f1_v2|African Caribbean in Barbados|-7.49702114914051|0
3d5ca882-670d-4d17-85dc-15185738c40a|Human|Homo sapiens|GCA_018852585.1|HG02145.pri.mat.f1_v2|HG02145.pri.mat.f1_v2|African ancestry from Barbados|-7.49702114914051|0
f544f1e8-1d21-4667-883b-7a6a43022962|Human|Homo sapiens|GCA_018466985.1|HG02559.pri.mat.f1_v2|HG02559.pri.mat.f1_v2|African Caribbean in Barbados|-7.49702114914051|0
```

### Taxonomy

```sql
SELECT taxonomy_id, scientific_name, genbank_common_name, common_name, equivalent_name, bm25(taxonomy_fts) AS score, search_boost
FROM taxonomy_fts f
WHERE taxonomy_fts MATCH 'homo sapiens'
order by search_boost desc, score desc
```

This at time of writing will pull back human assemblies from HPRC linked to Barbados.

```text
2045059|Homo sapiens x Rattus norvegicus fusion cell line||||-17.8756750404544|0
1131344|Homo sapiens x Mus musculus hybrid cell line||||-17.8756750404544|0
2883641|Homo sapiens x Pan troglodytes tetraploid cell line||||-17.8756750404544|0
1849111|Homo sapiens x Cricetulus griseus hybrid cell line||||-17.8756750404544|0
1035824|Trichuris sp. ex Homo sapiens JP-2011||||-18.9089024355812|0
63221|Homo sapiens neanderthalensis|Neandertal|Neanderthal man||-20.0688998670496|0
63221|Homo sapiens neanderthalensis|Neandertal|Neandertal man||-20.0688998670496|0
63221|Homo sapiens neanderthalensis|Neandertal|Neanderthal||-21.3805228822598|0
1383439|Homo sapiens/Mus musculus xenograft||||-21.3805228822598|0
1573476|Homo sapiens/Rattus norvegicus xenograft||||-21.3805228822598|0
2665953|Homo sapiens environmental sample||||-22.8755792335153|0
741158|Homo sapiens subsp. 'Denisova'|Denisova hominin|Denisovans|Homo sapiens ssp. Denisova|-23.6320701889989|0
741158|Homo sapiens subsp. 'Denisova'|Denisova hominin|Denisovan|Homo sapiens ssp. Denisova|-23.6320701889989|0
741158|Homo sapiens subsp. 'Denisova'|Denisova hominin|Denisovans|Homo sapiens ssp. 'Denisova'|-23.6320701889989|0
741158|Homo sapiens subsp. 'Denisova'|Denisova hominin|Denisovan|Homo sapiens ssp. 'Denisova'|-23.6320701889989|0
9606|Homo sapiens|human|||-24.5954423289268|0
```

## Design decisions

### Using SQLite versus DuckDB for FTS

DuckDB's FTS does not support wildcard querying only full text hits to words in the submitted query. This does make for a very precise FTS but not one that results in a search that could be used in autompletion or one that allows a user to provide known knowledge or patterns around text e.g. using a truncated TOLID to find linked assemblies.

For example we can create the DuckDB equivalent FTS as so (we need to add in a unique key in to make this work).

```sql
ALTER TABLE species ADD COLUMN species_id INTEGER;
CREATE OR REPLACE SEQUENCE species_ids;
UPDATE species SET species_id = nextval('species_ids');
CREATE UNIQUE INDEX species_id_idx ON species(species_id);
PRAGMA create_fts_index('species', 'species_id', 'accession', 'name', 'assembly_default', 'tol_id', 'ensembl_name', 'common_name', 'genome_uuid', 'scientific_name', 'biosample_id', 'strain', 'species_taxonomy_id', overwrite=1, lower = 1);

```

Running this for species:

```sql
SELECT *, fts_main_species.match_bm25(species_id, 'human') as score 
from species 
where score is not null 
order by score desc;
```

Will return hits such as:

```text
┌──────────────────┬──────────────────────┬──────────────────────┬─────────┬──────────────────────┬───┬─────────────┬─────────────────────┬──────────────┬────────────┬────────────────────┐
│    accession     │         name         │   assembly_default   │ tol_id  │     ensembl_name     │ … │ taxonomy_id │ species_taxonomy_id │ search_boost │ species_id │       score        │
│     varchar      │       varchar        │       varchar        │ varchar │       varchar        │   │    int64    │        int64        │    int64     │   int32    │       double       │
├──────────────────┼──────────────────────┼──────────────────────┼─────────┼──────────────────────┼───┼─────────────┼─────────────────────┼──────────────┼────────────┼────────────────────┤
│ GCA_009914755.4  │ T2T-CHM13v2.0        │ T2T-CHM13v2.0        │ NULL    │ T2T-CHM13v2.0        │ … │        9606 │                9606 │          500 │       2136 │  1.348181022078285 │
│ GCA_009914755.4  │ T2T-CHM13v2.0        │ T2T-CHM13v2.0        │ NULL    │ T2T-CHM13v2.0        │ … │        9606 │                9606 │          500 │        115 │  1.348181022078285 │
│ GCA_000001405.29 │ GRCh38.p14           │ GRCh38               │ NULL    │ GRCh38.p14           │ … │        9606 │                9606 │         1000 │       4058 │ 1.3018980588048785 │
│ GCA_000001405.29 │ GRCh38.p14           │ GRCh38               │ NULL    │ GRCh38.p14           │ … │        9606 │                9606 │         1000 │       4696 │ 1.3018980588048785 │
│ GCA_000001405.14 │ GRCh37.p13           │ GRCh37               │ NULL    │ GRCh37.p13           │ … │        9606 │                9606 │          900 │       4573 │ 1.2586874053392114 │
│ GCA_000001405.14 │ GRCh37.p13           │ GRCh37               │ NULL    │ GRCh37.p13           │ … │        9606 │                9606 │          900 │       5201 │ 1.2586874053392114 │
```

However you can substitute this for other terms and find that we do not get the expected hits back.

```sql
select count(*) from ( select fts_main_species.match_bm25(species_id, 'hum') as score from species where score is not null);
-- returns 0

select count(*) from ( select fts_main_species.match_bm25(species_id, 'hum*') as score from species where score is not null);
-- returns 0

select count(*) from ( select fts_main_species.match_bm25(species_id, 'GCA_009914755') as score from species where score is not null);
-- returns 6023

select count(*) from ( select * from species where accession like 'GCA_009914755%');
-- returns 2 as per the above hits
```

## Known bugs

1. We bring back too many rows for species searches because the metadata schema includes data for partial and integrated releases
2. We have a duplicate issue in taxonomy names where a linked name class e.g. `equivalent name` has more than one entry in the target taxa name table
3. We cannot create duckdb FTS indexes as both of these create duplicate rows via their "primary key"
