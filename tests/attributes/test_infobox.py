from __future__ import annotations

from datetime import date

import pytest

from cricket_value.attributes.infobox import birth_date, clean, infobox_fields

# Shaped like the real top sections the Wikipedia API returns (fetched 2026-10-29)
HARBHAJAN = """{{Short description|Indian cricketer}}
{{Use dmy dates|date=May 2024}}
{{Infobox cricketer
| name = Harbhajan Singh
| country = India
| birth_date = {{Birth date and age|1980|07|03|df=yes}}
| batting = Right-handed
| bowling = Right-arm [[off-spin]]<ref>{{cite web|url=https://example.org}}</ref>
| role = Bowler
| club1 = [[Punjab cricket team|Punjab]]
}}
'''Harbhajan Singh''' (born 3 July 1980) is a former Indian cricketer. His country was
| batting = not part of the infobox
"""


def test_infobox_fields_reads_only_the_cricketer_infobox() -> None:
    fields = infobox_fields(HARBHAJAN)

    assert fields["country"] == "India"
    assert fields["batting"] == "Right-handed"
    assert fields["bowling"] == "Right-arm off-spin"
    assert fields["role"] == "Bowler"
    assert fields["birth_date"].startswith("{{Birth date and age")


def test_a_page_without_a_cricketer_infobox_has_no_fields() -> None:
    assert infobox_fields("'''Someone''' is a person.\n| batting = Right-handed") == {}


@pytest.mark.parametrize(
    ("raw", "text"),
    [
        ("Right-arm [[fast bowling|fast]]", "Right-arm fast"),
        ("[[Left-arm orthodox spin|Slow left-arm orthodox spin]]", "Slow left-arm orthodox spin"),
        ("[[Wicket-keeper|Wicket-keeper-batsman]]", "Wicket-keeper-batsman"),
        ("Batting [[all-rounder]]", "Batting all-rounder"),
        ("{{nowrap|Right-arm [[leg break]]}}", "Right-arm leg break"),
        ("Right-handed<!-- per Cricinfo --><ref name=a/>", "Right-handed"),
    ],
)
def test_clean_strips_wiki_markup(raw: str, text: str) -> None:
    assert clean(raw) == text


@pytest.mark.parametrize(
    ("raw", "born"),
    [
        ("{{Birth date and age|1980|07|03|df=yes}}", date(1980, 7, 3)),
        ("{{Birth date and age|df=yes|1984|10|6}}", date(1984, 10, 6)),
        ("{{birth date and age|2000|8|10|df=y}}", date(2000, 8, 10)),
        ("{{birth date|1971|11|14}}", date(1971, 11, 14)),
        ("1980", None),
        ("{{Birth year and age|1990}}", None),  # year only: not a date of birth
    ],
)
def test_birth_date(raw: str, born: date | None) -> None:
    assert birth_date(raw) == born


def test_an_empty_field_does_not_swallow_the_next_line() -> None:
    wikitext = (
        "{{Infobox cricketer\n| batting =\n| bowling = Right-arm fast\n| role =\n| club1 = X\n}}"
    )

    fields = infobox_fields(wikitext)

    assert "batting" not in fields  # empty, so absent, not "| bowling = Right-arm fast"
    assert fields["bowling"] == "Right-arm fast"
    assert "role" not in fields
