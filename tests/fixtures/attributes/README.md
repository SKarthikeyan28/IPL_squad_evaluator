# Player attribute test fixtures

Real SPARQL answers for the two players in the fixture people register (V Kohli and MS
Dhoni), fetched on 2026-10-29 with `cricket_value.attributes.fetch.AttributeFetcher` and
stored unchanged. MS Dhoni's DBpedia page has no infobox fields, which is a real gap: the
overrides CSV fills it. There is no Wikipedia snapshot: its parser is tested on real
infobox text in `tests/attributes/test_infobox.py`.

**Attribution:** Wikidata (CC0) and DBpedia (CC BY-SA 3.0), derived from Wikipedia.
