from fastapi import FastAPI, Path, Query
from starlette.responses import FileResponse
from typing import Optional
import os
from src.db import DuckDb, SQLiteDb
import logging
import requests
import urllib

logging.basicConfig(level=logging.INFO)

app = FastAPI()

# Global variables to hold the database connection
duckdb = DuckDb.create("search.duckdb")
sqlite = SQLiteDb.create("search_fts.sqlite", "search_fts.sqlite")


@app.get("/", include_in_schema=False)
async def read_index():
    return FileResponse("static/index.html")


@app.get("/species/search", summary="Find a species using Full Text Search (FTS)")
async def search_species(q: str = Query(..., min_length=3), limit: Optional[int] = 100):
    """
    For a given string search for genomes held within Ensembl
    """
    query = f"""
    SELECT s.name, s.accession, s.scientific_name, s.assembly_default, s.tol_id, s.common_name, s.biosample_id, s.strain, s.genome_uuid, s.release_label, s.release_type, s.taxonomy_id, bm25(species_fts) AS score, search_boost
    FROM species_fts s
    WHERE s.species_fts MATCH ?
    order by s.search_boost desc, score desc
    limit {limit}
"""
    cursor = sqlite.con.cursor()
    results = cursor.execute(query, (q,)).fetchall()
    items = results_to_hash_list(results, cursor)
    json = {
        "meta": {"status": "success", "items": len(items), "limit": limit},
        "items": items,
    }
    return json


@app.get(
    "/species/taxonomy/{taxonomy_id}",
    summary="Find species in Ensembl that are a descendent of a taxonomic node",
)
async def intersect_taxonomy_by_taxon_id(
    taxonomy_id: int = Path(..., ge=1), limit: Optional[int] = 100
):
    """
    For a given taxonomy ID, bring back the species in Ensembl that intersect that
    identifier i.e. they are children bound by that taxonomic node
    """
    items = _get_intersecting_items(taxonomy_id, limit)
    json = {
        "meta": {"status": "success", "items": len(items), "limit": limit},
        "items": items,
    }
    return json


def _get_intersecting_items(taxonomy_id: int, limit):
    query = """
      SELECT s.name, s.accession, s.scientific_name, s.assembly_default, s.tol_id, s.common_name, s.biosample_id, s.strain, s.taxonomy_id, s.species_taxonomy_id, s.is_current, s.release_label, s.release_type, s.genome_uuid, coalesce(list_position(ch.ancestor_taxon_ids, ?), 0) as taxonomy_step
from species s
join computed_hierarchy ch on s.taxonomy_id = ch.organism_taxonomy_id
where list_contains(ch.ancestor_taxon_ids, ?)
or s.taxonomy_id =?
"""
    if limit:
        query = f"{query} limit {limit}"
    cursor = duckdb.con.cursor()
    results = cursor.execute(query, (taxonomy_id, taxonomy_id, taxonomy_id)).fetchall()
    return results_to_hash_list(results, cursor)


# Types of taxon to limit to and not ascend higher. Or we keep ascending until we bust out
# genus e.g. Homo
# subfamily e.g. Homininae
# family e.g. Hominidae
# superfamily e.g. Hominoidea
# parvorder e.g. Catarrhini
# infraorder e.g. Simiiformes
# suborder e.g. Haplorrhini
# order e.g. Primates
# superorder e.g. Euarchontoglires
# clade e.g. Boreoeutheria
# superclass e.g. Sarcopterygii
_purl_prefix = "http://purl.obolibrary.org/obo/NCBITaxon_"


@app.get(
    "/species/intersect/{taxonomy_id}",
    summary="Find the nearest relative in Ensembl to a given taxonomic node (max_taxon_level defaults to order)",
)
async def intersect_taxonomy(
    taxonomy_id: int = Path(..., ge=1),
    max_taxon_level="order",
    integrated_only: bool = False,
    limit: Optional[int] = 100,
):
    """
    For a taxonomic identifier, bring back all genomes in Ensembl that intersect it and
    ascend the taxonomic tree for other possible hits. Best used to give a species of interest
    and find the nearest relative to it in Ensembl
    """
    hierarchy = get_hierarchy(taxonomy_id=taxonomy_id)
    break_loop = False
    seen_genome = {}
    genomes = []
    for taxon in hierarchy["items"]:
        if break_loop:
            break
        # Stop processing on the next iteration
        if taxon["rank"] == max_taxon_level:
            break_loop = True

        taxons = _get_intersecting_items(taxonomy_id=taxon["id"], limit=None)
        for candidate in taxons:
            if candidate["genome_uuid"] in seen_genome:
                continue
            if integrated_only and candidate["release_type"] != "integrated":
                continue
            seen_genome[candidate["genome_uuid"]] = True
            candidate["intersecting_taxon"] = taxon
            candidate["total_distance"] = candidate["taxonomy_step"] + taxon["distance"]
            genomes.append(
                dict(
                    {
                        "total_distance": (
                            candidate["taxonomy_step"] + taxon["distance"]
                        )
                    },
                    **candidate,
                )
            )

    genomes.sort(key=lambda x: (x["total_distance"], x["accession"]))
    if len(genomes) > limit:
        genomes = genomes[slice(0, (limit))]

    json = {"meta": {"status": "success", "items": len(genomes)}, "items": genomes}
    return json


@app.get(
    "/taxonomy/hierarchy/{taxonomy_id}",
    summary="Return the ancestors of a taxonomic node",
)
def get_hierarchy(taxonomy_id: int = Path(..., ge=1), include_root: bool = False):
    """
    Get the hierarchy of a taxonomic node. Uses OLSv4 from EMBL-EBI in the background
    """
    iri = f"http://purl.obolibrary.org/obo/NCBITaxon_{taxonomy_id}"
    encoded_iri = urllib.parse.quote_plus(iri)
    encoded_iri = urllib.parse.quote_plus(encoded_iri)
    url = f"https://www.ebi.ac.uk/ols4/api/ontologies/ncbitaxon/terms/{encoded_iri}/hierarchicalAncestors"
    params = {"lang": "en"}
    resp = requests.get(url, params=params)
    if resp:
        json = resp.json()
        output = []
        distance = 1
        for item in json["_embedded"]["terms"]:
            if item["is_root"] and not include_root:
                continue
            iri = item["iri"]
            ancestor_id = int(iri.replace(_purl_prefix, ""))
            rank = None
            if "annotation" in item and "has_rank" in item["annotation"]:
                rank = item["annotation"]["has_rank"][0]
                rank = rank.replace(_purl_prefix, "")
            output.append(
                {
                    "iri": iri,
                    "id": ancestor_id,
                    "rank": rank,
                    "label": item["label"],
                    "distance": distance,
                }
            )
            distance = distance + 1
        return {"meta": {"status": "success", "items": len(output)}, "items": output}
    else:
        return {"meta": {"status": "error", "error": resp.content}}


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
