from __future__ import annotations

import pytest

from cricket_value.attributes.styles import (
    BowlingStyle,
    batting_hand,
    bowling_style,
    combine_bowling,
)

# Spellings as DBpedia returns them for in-scope players (probed 2026-10-29)
CASES = [
    ("Right-arm fast", "pace", "right"),
    ("Right-arm medium", "pace", "right"),
    ("Right-arm fast-medium", "pace", "right"),
    ("Right arm medium fast", "pace", "right"),
    ("Left-arm fast medium", "pace", "left"),
    ("Right-arm Medium", "pace", "right"),
    ("Right-arm medium-slow", "pace", "right"),
    ("Right-arm slow seam", "pace", "right"),
    ("Right-arm off break", "spin", "right"),
    ("Right-arm offbreak", "spin", "right"),
    ("Right arm off spin", "spin", "right"),
    ("Right-arm leg-break", "spin", "right"),
    ("Right-arm legbreak", "spin", "right"),
    ("Slow left-arm orthodox", "spin", "left"),
    ("Left-arm wrist spin", "spin", "left"),
    ("http://dbpedia.org/resource/Left-arm_orthodox_spin", "spin", "left"),
    ("http://dbpedia.org/resource/Left-arm_unorthodox_spin", "spin", "left"),
    # no arm given: leg spin and off spin are right-arm by definition (the left-arm
    # versions have their own names), and left-arm orthodox is left by definition
    ("http://dbpedia.org/resource/Leg_break", "spin", "right"),
    ("Legbreak googly", "spin", "right"),
    ("http://dbpedia.org/resource/Off_spin", "spin", "right"),
    ("Left-arm orthodox", "spin", "left"),
    # no arm and nothing implies one
    ("http://dbpedia.org/resource/Fast_bowling", "pace", None),
]


@pytest.mark.parametrize(("raw", "kind", "arm"), CASES)
def test_bowling_style_maps_real_spellings(raw: str, kind: str, arm: str | None) -> None:
    assert bowling_style(raw) == BowlingStyle(kind, arm)


@pytest.mark.parametrize("raw", ["", "\u2013", "Batting_order_(cricket)", "Right-handed"])
def test_text_that_is_not_a_bowling_style_maps_to_nothing(raw: str) -> None:
    assert bowling_style(raw) == BowlingStyle(None, None)


def test_a_style_naming_both_pace_and_spin_is_ambiguous() -> None:
    assert bowling_style("Right-arm medium, off break") == BowlingStyle(None, "right")


def test_combining_values_keeps_what_they_agree_on() -> None:
    # DBpedia often gives the same style twice: as text and as a link
    same = combine_bowling(["Right-arm off break", "http://dbpedia.org/resource/Off_spin"])
    clash = combine_bowling(["Right-arm fast", "Right-arm off break"])
    partial = combine_bowling(["http://dbpedia.org/resource/Fast_bowling", "Left-arm fast"])

    assert same == BowlingStyle("spin", "right")
    assert clash == BowlingStyle(None, "right")
    assert partial == BowlingStyle("pace", "left")  # a missing arm doesn't clash
    assert combine_bowling([]) == BowlingStyle(None, None)


@pytest.mark.parametrize(
    ("raw", "hand"),
    [
        (["Right-handed"], "right"),
        (["Left-handed", "Left-hand bat"], "left"),
        (["Right-handed", "Left-handed"], None),
        ([], None),
    ],
)
def test_batting_hand(raw: list[str], hand: str | None) -> None:
    assert batting_hand(raw) == hand
