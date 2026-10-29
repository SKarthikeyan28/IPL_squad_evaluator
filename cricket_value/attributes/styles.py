"""Map DBpedia's free-text batting and bowling styles to a small, fixed vocabulary.

DBpedia copies the Wikipedia infobox, so one style has many spellings ("Right-arm off
break", "Right arm offbreak", ".../resource/Off_spin"). Each value is normalised, then
read by keywords: the bowling kind ({pace, spin}) and the arm ({left, right}). Where no
arm is given, cricket's naming implies one: leg spin and off spin are right-arm (the
left-arm versions are called wrist spin and orthodox), and orthodox is left-arm.
"Slow" is not a spin word: "slow left-arm orthodox" is caught by "orthodox", while
"medium-slow" and "slow seam" are pace.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import NamedTuple

SPIN_WORDS = (
    "off break",
    "off spin",
    "leg break",
    "leg spin",
    "googly",
    "orthodox",
    "wrist spin",
    "chinaman",
    "spin",
)
PACE_WORDS = ("fast", "medium", "seam", "swing")
RIGHT_ARM_WORDS = ("leg break", "leg spin", "googly", "off break", "off spin")


class BowlingStyle(NamedTuple):
    kind: str | None  # "pace" or "spin"
    arm: str | None  # "left" or "right"


def normalise(raw: str) -> str:
    text = raw.rsplit("/resource/", 1)[-1].lower()
    text = re.sub(r"[_\-]+", " ", text)
    text = re.sub(r"\b(off|leg)(break|spin)\b", r"\1 \2", text)
    return re.sub(r"\s+", " ", text).strip()


def bowling_style(raw: str) -> BowlingStyle:
    text = normalise(raw)
    spin = any(w in text for w in SPIN_WORDS)
    pace = any(w in text for w in PACE_WORDS)
    if not spin and not pace:  # not a bowling style at all, e.g. "Right-handed"
        return BowlingStyle(None, None)
    kind = "spin" if spin and not pace else "pace" if pace and not spin else None
    if "left" in text:
        arm: str | None = "left"
    elif "right" in text or any(w in text for w in RIGHT_ARM_WORDS):
        arm = "right"
    elif "orthodox" in text:
        arm = "left"
    else:
        arm = None
    return BowlingStyle(kind, arm)


def _agree(values: Iterable[str | None]) -> str | None:
    """The single value all non-missing values share; None if none, or if they clash."""
    known = {v for v in values if v is not None}
    return known.pop() if len(known) == 1 else None


def combine_bowling(raws: Iterable[str]) -> BowlingStyle:
    """One style from several DBpedia values for the same player."""
    styles = [bowling_style(r) for r in raws]
    return BowlingStyle(_agree(s.kind for s in styles), _agree(s.arm for s in styles))


def batting_hand(raws: Iterable[str]) -> str | None:
    hands = []
    for raw in raws:
        text = normalise(raw)
        hands.append("left" if "left" in text else "right" if "right" in text else None)
    return _agree(hands)
