"""Read a few fields from a Wikipedia page's ``{{Infobox cricketer}}``.

The third attribute source, used only where Wikidata and DBpedia left a gap: DBpedia is
an occasional extraction of Wikipedia, and some pages (Harbhajan Singh, Morné Morkel)
were never extracted. Only the infobox is read, never the article text, and values keep
their source wording, so the same style mapper as DBpedia's applies.
"""

from __future__ import annotations

import re
from datetime import date

FIELDS = ("country", "birth_date", "batting", "bowling", "role")

_INFOBOX = re.compile(r"\{\{\s*Infobox cricketer", re.IGNORECASE)
_FIELD = re.compile(r"^[ \t]*\|[ \t]*(\w+)[ \t]*=[ \t]*(.*?)[ \t]*$", re.MULTILINE)
_BIRTH = re.compile(r"\{\{\s*birth date(?: and age)?\s*\|([^{}]*)\}\}", re.IGNORECASE)


def _infobox_text(wikitext: str) -> str | None:
    """The infobox template, by matching braces (its values contain templates too)."""
    start = _INFOBOX.search(wikitext)
    if start is None:
        return None
    depth, i = 0, start.start()
    while i < len(wikitext) - 1:
        pair = wikitext[i : i + 2]
        if pair == "{{":
            depth, i = depth + 1, i + 2
        elif pair == "}}":
            depth, i = depth - 1, i + 2
            if depth == 0:
                return wikitext[start.start() : i]
        else:
            i += 1
    return None


def infobox_fields(wikitext: str) -> dict[str, str]:
    """The FIELDS present in the cricketer infobox, cleaned of markup (see ``clean``)."""
    box = _infobox_text(wikitext)
    if box is None:
        return {}
    out: dict[str, str] = {}
    for key, value in _FIELD.findall(box):
        if key in FIELDS and key not in out and clean(value):
            out[key] = clean(value)
    return out


def clean(value: str) -> str:
    """Wikitext -> plain text: links to their label, refs, comments and wrappers removed."""
    text = re.sub(r"<!--.*?-->", "", value, flags=re.DOTALL)
    text = re.sub(r"<ref[^>/]*/>|<ref[^>]*>.*?</ref>", "", text, flags=re.DOTALL)
    text = re.sub(r"\{\{\s*(?:nowrap|nobr)\s*\|(.*?)\}\}", r"\1", text, flags=re.IGNORECASE)
    text = re.sub(r"\[\[(?:[^\]|]*\|)?([^\]]*)\]\]", r"\1", text)
    return re.sub(r"\s+", " ", text).strip()


def birth_date(value: str) -> date | None:
    """Year, month and day from a ``{{Birth date (and age)|...}}`` template."""
    m = _BIRTH.search(value)
    if m is None:
        return None
    parts = [p.strip() for p in m.group(1).split("|") if "=" not in p]
    try:
        y, mo, d = (int(p) for p in parts[:3])
        return date(y, mo, d)
    except ValueError:
        return None
