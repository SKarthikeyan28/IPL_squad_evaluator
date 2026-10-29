"""Fetch player attributes from Wikidata, DBpedia and Wikipedia into the raw layer.

1. **Wikidata (SPARQL):** for each ESPNcricinfo ID (property P2697, the link Cricsheet's
   register also carries as ``key_cricinfo``): the item, date of birth (P569), country for
   sport (P1532), citizenship (P27) and the English Wikipedia article.
2. **DBpedia (SPARQL):** for those articles, the infobox ``batting``, ``bowling`` and ``role``.
3. **Wikipedia (MediaWiki API):** the top section, which holds the infobox, of only the
   articles the first two left with a gap (``wikipedia_gaps``). DBpedia is an occasional
   extraction of Wikipedia and never extracted some pages.

Each source's answers are stored as one JSON snapshot, ``<raw_dir>/<source>/<fetched_at>.json``,
and recorded in the manifest like any other download. Endpoints return rows in no fixed
order, so a snapshot is canonical (rows de-duplicated and sorted) before it is hashed: a
refetch that got the same answers stores nothing, which keeps reruns idempotent.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import httpx

from cricket_value.ingest.download import FetchResult, FetchStatus, utc_now
from cricket_value.ingest.manifest import ArchiveRecord, Manifest

Binding = dict[str, Any]


@dataclass(frozen=True)
class AttributeSource:
    name: str
    url: str


WIKIDATA = AttributeSource("wikidata_players", "https://query.wikidata.org/sparql")
DBPEDIA = AttributeSource("dbpedia_players", "https://dbpedia.org/sparql")
WIKIPEDIA = AttributeSource("wikipedia_infoboxes", "https://en.wikipedia.org/w/api.php")
WIKIPEDIA_BATCH = 50  # the API's limit on titles per request

ENWIKI = "https://en.wikipedia.org/wiki/"
DBPEDIA_RESOURCE = "http://dbpedia.org/resource/"


def wikidata_query(cricinfo_ids: Sequence[str]) -> str:
    values = " ".join(json.dumps(i) for i in cricinfo_ids)
    return f"""SELECT ?cid ?item ?dob ?sport_country ?citizenship ?article WHERE {{
  VALUES ?cid {{ {values} }}
  ?item wdt:P2697 ?cid .
  OPTIONAL {{ ?item wdt:P569 ?dob }}
  OPTIONAL {{ ?item wdt:P1532 ?sc . ?sc rdfs:label ?sport_country
             FILTER(lang(?sport_country) = "en") }}
  OPTIONAL {{ ?item wdt:P27 ?c . ?c rdfs:label ?citizenship
             FILTER(lang(?citizenship) = "en") }}
  OPTIONAL {{ ?article schema:about ?item ; schema:isPartOf <https://en.wikipedia.org/> }}
}}"""


def dbpedia_query(resources: Sequence[str]) -> str:
    values = " ".join(f"<{r}>" for r in resources)
    props = " ".join(f"<http://dbpedia.org/property/{p}>" for p in ("batting", "bowling", "role"))
    return f"SELECT ?s ?p ?o WHERE {{ VALUES ?s {{ {values} }} VALUES ?p {{ {props} }} ?s ?p ?o }}"


def article_title(article_url: str) -> str:
    """English Wikipedia article URL -> title. Wikidata's sitelinks are URL-encoded
    ("Morn%C3%A9_Morkel"); titles everywhere else use the characters themselves."""
    return unquote(article_url.removeprefix(ENWIKI))


def dbpedia_resource(article_url: str) -> str:
    """English Wikipedia article URL -> DBpedia resource URI (same title)."""
    return DBPEDIA_RESOURCE + article_title(article_url)


def wikipedia_gaps(wd_bindings: Iterable[Binding], db_bindings: Iterable[Binding]) -> list[str]:
    """Titles of articles where Wikidata and DBpedia leave a gap Wikipedia's infobox can
    fill: no DBpedia batting or bowling, not exactly one date of birth, or no country."""
    props: dict[str, set[str]] = {}
    for b in db_bindings:
        title = b["s"]["value"].removeprefix(DBPEDIA_RESOURCE)
        props.setdefault(title, set()).add(b["p"]["value"].rsplit("/", 1)[-1])
    players: dict[str, dict[str, set[str]]] = {}
    for b in wd_bindings:
        fields = players.setdefault(b["cid"]["value"], {})
        for key in ("article", "dob", "sport_country", "citizenship"):
            if key in b:
                fields.setdefault(key, set()).add(b[key]["value"])
    gaps = set()
    for f in players.values():
        if len(f.get("article", set())) != 1:
            continue
        title = article_title(next(iter(f["article"])))
        if (
            not {"batting", "bowling"} <= props.get(title, set())
            or len(f.get("dob", set())) != 1
            or not (f.get("sport_country") or f.get("citizenship"))
        ):
            gaps.add(title)
    return sorted(gaps)


def canonical_snapshot(
    endpoint: str, queries: list[str], rows: Iterable[Binding], key: str = "bindings"
) -> bytes:
    unique = sorted({json.dumps(r, sort_keys=True) for r in rows})
    doc = {"endpoint": endpoint, "queries": queries, key: [json.loads(r) for r in unique]}
    return (json.dumps(doc, indent=1, sort_keys=True, ensure_ascii=False) + "\n").encode()


def _batches(items: Sequence[str], size: int) -> list[Sequence[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


@dataclass
class AttributeFetcher:
    client: httpx.Client
    raw_dir: Path
    manifest: Manifest
    clock: Callable[[], datetime] = utc_now
    batch_size: int = 400

    def fetch(self, cricinfo_ids: Iterable[str]) -> list[FetchResult]:
        """Wikidata, then DBpedia for the articles Wikidata found, then Wikipedia for the
        gaps. All three snapshots share one timestamp."""
        fetched_at = self.clock().astimezone(UTC)
        ids = sorted(set(cricinfo_ids))
        wd_queries = [wikidata_query(b) for b in _batches(ids, self.batch_size)]
        wd_bindings = self._run(WIKIDATA, wd_queries)
        articles = sorted({b["article"]["value"] for b in wd_bindings if "article" in b})
        resources = [dbpedia_resource(a) for a in articles]
        db_queries = [dbpedia_query(b) for b in _batches(resources, self.batch_size // 2)]
        db_bindings = self._run(DBPEDIA, db_queries)
        titles = wikipedia_gaps(wd_bindings, db_bindings)
        wp_queries = ["|".join(b) for b in _batches(titles, WIKIPEDIA_BATCH)]
        wp_pages = self._wikipedia(wp_queries)
        return [
            self._store(
                WIKIDATA, canonical_snapshot(WIKIDATA.url, wd_queries, wd_bindings), fetched_at
            ),
            self._store(
                DBPEDIA, canonical_snapshot(DBPEDIA.url, db_queries, db_bindings), fetched_at
            ),
            self._store(
                WIKIPEDIA,
                canonical_snapshot(WIKIPEDIA.url, wp_queries, wp_pages, key="pages"),
                fetched_at,
            ),
        ]

    def _wikipedia(self, queries: list[str]) -> list[Binding]:
        """Each requested title with the top-section wikitext of the page it resolves to.
        Titles are normalised and redirects followed, so answers are mapped back."""
        out: list[Binding] = []
        for titles in queries:
            r = self.client.get(
                WIKIPEDIA.url,
                params={
                    "action": "query",
                    "prop": "revisions",
                    "rvprop": "content",
                    "rvslots": "main",
                    "rvsection": "0",
                    "redirects": "1",
                    "format": "json",
                    "formatversion": "2",
                    "titles": titles,
                },
            )
            r.raise_for_status()
            query = r.json()["query"]
            hops = {
                h["from"]: h["to"] for k in ("normalized", "redirects") for h in query.get(k, [])
            }
            pages = {
                p["title"]: p["revisions"][0]["slots"]["main"]["content"]
                for p in query.get("pages", [])
                if "revisions" in p
            }
            for requested in titles.split("|"):
                final = requested
                for _ in range(5):  # normalised, then redirected; a bound guards a cycle
                    final = hops.get(final, final)
                out.append({"requested": requested, "title": final, "wikitext": pages.get(final)})
        return out

    def _run(self, source: AttributeSource, queries: list[str]) -> list[Binding]:
        out: list[Binding] = []
        for q in queries:
            r = self.client.post(
                source.url,
                data={"query": q},
                headers={"Accept": "application/sparql-results+json"},
            )
            r.raise_for_status()
            out += r.json()["results"]["bindings"]
        return out

    def _store(self, source: AttributeSource, content: bytes, fetched_at: datetime) -> FetchResult:
        sha = hashlib.sha256(content).hexdigest()
        latest = self.manifest.latest_archive(source.name)
        if latest is not None and latest.sha256 == sha:
            return FetchResult(source=source.name, status=FetchStatus.UNCHANGED)
        rel_path = f"{source.name}/{fetched_at:%Y%m%dT%H%M%SZ}.json"
        path = self.raw_dir / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():  # raw files are never overwritten
            raise FileExistsError(path)
        path.write_bytes(content)
        self.manifest.add_archive(
            ArchiveRecord(
                source=source.name,
                url=source.url,
                path=rel_path,
                sha256=sha,
                n_bytes=len(content),
                fetched_at=fetched_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                last_modified=None,
            )
        )
        return FetchResult(source=source.name, status=FetchStatus.NEW, path=path)
