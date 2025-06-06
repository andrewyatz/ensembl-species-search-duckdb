from fastapi import FastAPI, Path, Query
from typing import Optional
import os
from src.db import DuckDb, SQLiteDb
import logging

logging.basicConfig(level=logging.INFO)

app = FastAPI()

# Global variables to hold the database connection
duckdb = DuckDb.create("search.duckdb")
sqlite = SQLiteDb.create("search_fts.sqlite", "search_fts.sqlite")


@app.get("/species/search")
async def search_species(
    q: str = Query(..., min_length=3), limit: Optional[int] = 1000
):
    query = f"""
    SELECT s.accession, s.scientific_name, s.genome_uuid, s.tol_id, s.common_name, s.biosample_id, s.strain, bm25(species_fts) AS score, search_boost
    FROM species_fts s
    WHERE s.species_fts MATCH ?
    order by s.search_boost desc, score desc
    limit {limit}
"""
    cursor = sqlite.con.cursor()
    results = cursor.execute(query, (q,)).fetchall()
    items = results_to_hash_list(results, cursor)
    json = {"items": items, "meta": {"items": len(items), "limit": limit}}
    return json


@app.get("/species/taxonomy/{taxonomy_id}")
async def search_items(taxonomy_id: int = Path(..., ge=1), limit: Optional[int] = 1000):
    query = f"""
      SELECT s.species_id, s.accession, s.scientific_name, s.genome_uuid, s.tol_id, s.common_name, s.biosample_id, s.strain, s.taxonomy_id, s.species_taxonomy_id, s.is_current, s.release_label, s.release_type, list_position(ancestor_taxon_ids, ?) as taxonomy_position
from species s
join computed_hierarchy ch on s.taxonomy_id = ch.organism_taxonomy_id
where array_contains(ch.ancestor_taxon_ids, ?)
or s.taxonomy_id =?
limit {limit};
"""
    cursor = duckdb.con.cursor()
    results = cursor.execute(query, (taxonomy_id, taxonomy_id, taxonomy_id)).fetchall()
    items = results_to_hash_list(results, cursor)
    json = {"items": items}
    json["meta"] = {"items": len(results), "limit": limit}
    return json


def results_to_hash_list(results, cursor):
    items = []
    for row in results:
        item = {}
        iter_num = 0
        for key in cursor.description:
            item[key[0]] = row[iter_num]
            iter_num += 1
        items.append(item)
    return items


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
