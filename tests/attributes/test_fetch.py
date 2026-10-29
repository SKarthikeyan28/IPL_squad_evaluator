from __future__ import annotations

import json
import random
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs

import httpx

from cricket_value.attributes.fetch import (
    DBPEDIA,
    WIKIDATA,
    WIKIPEDIA,
    AttributeFetcher,
    Binding,
    article_title,
    canonical_snapshot,
    dbpedia_resource,
)
from cricket_value.ingest.download import FetchStatus
from cricket_value.ingest.manifest import Manifest

KOHLI: Binding = {
    "cid": {"type": "literal", "value": "253802"},
    "item": {"type": "uri", "value": "http://www.wikidata.org/entity/Q213854"},
    "article": {"type": "uri", "value": "https://en.wikipedia.org/wiki/Virat_Kohli"},
    "dob": {"type": "literal", "value": "1988-11-05T00:00:00Z"},
}
BUMRAH: Binding = {
    "cid": {"type": "literal", "value": "625383"},
    "item": {"type": "uri", "value": "http://www.wikidata.org/entity/Q16227998"},
    "article": {"type": "uri", "value": "https://en.wikipedia.org/wiki/Jasprit_Bumrah"},
}
KOHLI_BOWLING: Binding = {
    "s": {"type": "uri", "value": "http://dbpedia.org/resource/Virat_Kohli"},
    "p": {"type": "uri", "value": "http://dbpedia.org/property/bowling"},
    "o": {"type": "literal", "value": "Right-arm medium"},
}


class Endpoints:
    """Fake endpoints: SPARQL answers from fixed bindings, Wikipedia from a title ->
    wikitext map (with optional redirects). Every query is recorded."""

    def __init__(
        self,
        wikidata: list[Binding],
        dbpedia: list[Binding],
        wikipedia: dict[str, str] | None = None,
        redirects: dict[str, str] | None = None,
    ):
        self.answers = {WIKIDATA.url: wikidata, DBPEDIA.url: dbpedia}
        self.pages = wikipedia or {}
        self.redirects = redirects or {}
        self.queries: dict[str, list[str]] = {WIKIDATA.url: [], DBPEDIA.url: [], WIKIPEDIA.url: []}

    def __call__(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url.copy_with(query=None))
        if url == WIKIPEDIA.url:
            return self._wikipedia(request)
        query = parse_qs(request.content.decode())["query"][0]
        self.queries[url].append(query)
        bindings = list(self.answers[url])
        random.shuffle(bindings)  # endpoints guarantee no order
        return httpx.Response(200, json={"head": {}, "results": {"bindings": bindings}})

    def _wikipedia(self, request: httpx.Request) -> httpx.Response:
        titles = request.url.params["titles"]
        self.queries[WIKIPEDIA.url].append(titles)
        normalized = [{"from": t, "to": t.replace("_", " ")} for t in titles.split("|") if "_" in t]
        names = [t.replace("_", " ") for t in titles.split("|")]
        redirects = [{"from": n, "to": self.redirects[n]} for n in names if n in self.redirects]
        finals = [self.redirects.get(n, n) for n in names]
        pages = [
            {"title": t, "revisions": [{"slots": {"main": {"content": self.pages[t]}}}]}
            if t in self.pages
            else {"title": t, "missing": True}
            for t in finals
        ]
        query = {"normalized": normalized, "redirects": redirects, "pages": pages}
        return httpx.Response(200, json={"batchcomplete": True, "query": query})


def fetcher(tmp_path: Path, endpoints: Endpoints, when: str, batch: int = 400) -> AttributeFetcher:
    raw = tmp_path / "raw"
    return AttributeFetcher(
        client=httpx.Client(transport=httpx.MockTransport(endpoints)),
        raw_dir=raw,
        manifest=Manifest(raw / "manifest"),
        clock=lambda: datetime.fromisoformat(when).replace(tzinfo=UTC),
        batch_size=batch,
    )


def test_first_fetch_stores_one_snapshot_per_source(tmp_path: Path) -> None:
    endpoints = Endpoints([KOHLI, BUMRAH], [KOHLI_BOWLING])
    f = fetcher(tmp_path, endpoints, "2026-10-29T08:00:00")

    results = f.fetch(["253802", "625383"])

    assert {r.source: r.status for r in results} == {
        WIKIDATA.name: FetchStatus.NEW,
        DBPEDIA.name: FetchStatus.NEW,
        WIKIPEDIA.name: FetchStatus.NEW,
    }
    archives = f.manifest.archives().set_index("source")
    assert archives.loc[WIKIDATA.name, "path"] == "wikidata_players/20261029T080000Z.json"
    assert archives.loc[DBPEDIA.name, "fetched_at"] == "2026-10-29T08:00:00Z"
    stored = json.loads(
        (tmp_path / "raw" / "wikidata_players" / "20261029T080000Z.json").read_text()
    )
    assert stored["endpoint"] == WIKIDATA.url
    assert len(stored["bindings"]) == 2


def test_dbpedia_is_asked_about_the_articles_wikidata_returned(tmp_path: Path) -> None:
    endpoints = Endpoints([KOHLI, BUMRAH], [])
    fetcher(tmp_path, endpoints, "2026-10-29T08:00:00").fetch(["253802", "625383"])

    (query,) = endpoints.queries[DBPEDIA.url]
    assert "<http://dbpedia.org/resource/Virat_Kohli>" in query
    assert "<http://dbpedia.org/resource/Jasprit_Bumrah>" in query


def test_ids_are_queried_in_batches(tmp_path: Path) -> None:
    endpoints = Endpoints([KOHLI], [])
    fetcher(tmp_path, endpoints, "2026-10-29T08:00:00", batch=2).fetch(["1", "2", "3", "4", "5"])

    assert len(endpoints.queries[WIKIDATA.url]) == 3


def test_refetching_the_same_answers_stores_nothing(tmp_path: Path) -> None:
    endpoints = Endpoints([KOHLI, BUMRAH], [KOHLI_BOWLING])
    fetcher(tmp_path, endpoints, "2026-10-29T08:00:00").fetch(["253802", "625383"])

    results = fetcher(tmp_path, endpoints, "2026-11-05T08:00:00").fetch(["625383", "253802"])

    assert {r.status for r in results} == {FetchStatus.UNCHANGED}
    assert len(Manifest(tmp_path / "raw" / "manifest").archives()) == 3


def test_a_changed_answer_is_a_new_snapshot(tmp_path: Path) -> None:
    fetcher(tmp_path, Endpoints([KOHLI], []), "2026-10-29T08:00:00").fetch(["253802"])
    corrected = {**KOHLI, "dob": {"type": "literal", "value": "1988-11-06T00:00:00Z"}}

    (wd, _, _) = fetcher(tmp_path, Endpoints([corrected], []), "2026-11-05T08:00:00").fetch(
        ["253802"]
    )

    assert wd.status is FetchStatus.NEW
    archives = Manifest(tmp_path / "raw" / "manifest").archives()
    assert archives["source"].value_counts()[WIKIDATA.name] == 2
    assert len(archives) == 4  # DBpedia and Wikipedia answers didn't change


def test_canonical_snapshot_ignores_binding_order() -> None:
    a = canonical_snapshot("u", ["q"], [KOHLI, BUMRAH])
    b = canonical_snapshot("u", ["q"], [BUMRAH, KOHLI])

    assert a == b


def test_percent_encoded_titles_are_decoded_for_dbpedia() -> None:
    # Wikidata's sitelinks are URL-encoded; DBpedia names resources with the characters
    assert dbpedia_resource("https://en.wikipedia.org/wiki/Morn%C3%A9_Morkel") == (
        "http://dbpedia.org/resource/Morné_Morkel"
    )
    assert article_title("https://en.wikipedia.org/wiki/D%27Arcy_Short") == "D'Arcy_Short"


def test_wikipedia_is_asked_only_about_gaps_and_follows_redirects(tmp_path: Path) -> None:
    # Kohli has a DBpedia batting and bowling, a date of birth and a country: no gap.
    # Bumrah has none of those, and his page has been renamed.
    kohli: Binding = {**KOHLI, "sport_country": {"type": "literal", "value": "India"}}
    dbpedia = [
        {**KOHLI_BOWLING},
        {**KOHLI_BOWLING, "p": {"type": "uri", "value": "http://dbpedia.org/property/batting"}},
    ]
    endpoints = Endpoints(
        [kohli, BUMRAH],
        dbpedia,
        wikipedia={
            "Jasprit Bumrah (cricketer)": "{{Infobox cricketer\n| batting = Right-handed\n}}"
        },
        redirects={"Jasprit Bumrah": "Jasprit Bumrah (cricketer)"},
    )
    f = fetcher(tmp_path, endpoints, "2026-10-29T08:00:00")

    f.fetch(["253802", "625383"])

    assert endpoints.queries[WIKIPEDIA.url] == ["Jasprit_Bumrah"]
    stored = json.loads((tmp_path / "raw" / WIKIPEDIA.name / "20261029T080000Z.json").read_text())
    (page,) = stored["pages"]
    assert page["requested"] == "Jasprit_Bumrah"
    assert page["title"] == "Jasprit Bumrah (cricketer)"
    assert "Right-handed" in page["wikitext"]
