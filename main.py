from fastapi import FastAPI, Path, Query
import duckdb
from typing import Optional
import os

app = FastAPI()

N_GRAM_SIZE = 3

# Global variable to hold the database connection
target_db = "searcher.duckdb"
con = duckdb.connect(target_db)


@app.get("/species/search")
async def search_items(
    q: str = Query(..., min_length=3), limit: Optional[int] = 1000
):
    query = f"""
      SELECT s.accession, s.scientific_name, s.genome_uuid, s.tol_id, s.common_name, s.biosample_id, s.strain, s.taxonomy_id, s.species_taxonomy_id, s.search_boost, fts_main_species.match_bm25(s.genome_uuid, ?) as score
from species s
where score is not null
order by search_boost desc, score desc
limit {limit};
"""
    results = con.execute(query, (q,)).fetchall()
    json = {
        "items": [
            {
                "accession": row[0],
                "scientific_name": row[1],
                "genome_uuid": row[2],
                "tol_id" : row[3],
                "common_name" : row[4],
                "biosample_id" : row[5],
                "strain" : row[6],
                "taxonomy_id" : row[7],
                "species_taxonomy_id" : row[8],
                "search_boost": row[9],
                "score": row[10]
            }
            for row in results
        ]
    }
    json["meta"] = {"items": len(results), "limit": limit}
    return json


@app.get("/species/taxonomy/{taxonomy_id}")
async def search_items(taxonomy_id: int = Path(..., ge=1), limit: Optional[int] = 1000):
    query = f"""
      SELECT s.accession, s.scientific_name, s.genome_uuid, s.tol_id, s.common_name, s.biosample_id, s.strain, s.taxonomy_id, s.species_taxonomy_id
from species s
join computed_hierarchy ch on s.taxonomy_id = ch.organism_taxonomy_id
where array_contains(ch.ancestor_taxon_ids, ?)
or s.taxonomy_id =?
limit {limit};
"""
    results = con.execute(query, (taxonomy_id, taxonomy_id)).fetchall()
    json = {
        "items": [
            {
                "accession": row[0],
                "scientific_name": row[1],
                "genome_uuid": row[2],
                "tol_id" : row[3],
                "common_name" : row[4],
                "biosample_id" : row[5],
                "strain" : row[6],
                "taxonomy_id" : row[7],
                "species_taxonomy_id" : row[8]
            }
            for row in results
        ]
    }
    json["meta"] = {"items": len(results), "limit": limit}
    return json

def generate_query_ngrams(query, n):
    query = query.lower()
    return set(query[i:i+n] for i in range(len(query) - n + 1))

@app.get("/taxonomy/search")
async def taxonomy_search(q: str = Query(..., min_length=1), limit: Optional[int] = 1000):
    """
    Provides autocomplete suggestions based on n-gram matching.
    """
    if len(q) < N_GRAM_SIZE:
        return []  # Not enough characters to form an n-gram

    query_ngrams = generate_query_ngrams(q, N_GRAM_SIZE)
    results = con.execute(f"""
        SELECT t.taxonomy_id, t.name
        FROM taxonomy_ngrams tng
        JOIN taxonomy t ON tng.taxon_id = t.taxon_id
        WHERE tng.ngram IN ({', '.join(['?' for _ in query_ngrams])})
        GROUP BY t.id, t.name
        ORDER BY COUNT(DISTINCT tng.ngram) DESC, t.name
        LIMIT {limit};
    """, list(query_ngrams)).fetchall()
    con.close()
    json = {
        "items" : [
            {
                "taxonomy_id" : row[0],
                "name" : row[1]
            }
            for row in results
        ]
    } 
    json["meta"] = {"items": len(results), "limit": limit}
    return json

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
