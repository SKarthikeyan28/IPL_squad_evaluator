"""Scoring rules for probability forecasts, and a paired bootstrap to compare two models.

Log loss and Brier score are *proper scoring rules*: they are minimised by forecasting
the true probability, so they reward calibrated probabilities, not just correct picks.
Accuracy only checks which side of 0.5 a forecast falls, so it is reported second.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np
import numpy.typing as npt

Array = npt.NDArray[np.float64]
Metric = Literal["log_loss", "brier", "accuracy"]

EPS = 1e-15


def log_loss_each(y: Array, p: Array) -> Array:
    p = np.clip(p, EPS, 1 - EPS)
    out: Array = -(y * np.log(p) + (1 - y) * np.log(1 - p))
    return out


def brier_each(y: Array, p: Array) -> Array:
    out: Array = (p - y) ** 2
    return out


def correct_each(y: Array, p: Array) -> Array:
    """1 when the forecast favours the side that won, 0 when it favours the loser.
    A 0.5 forecast picks no one and scores 0.5, the expected score of a random pick."""
    picked_winner = ((p > 0.5) & (y == 1)) | ((p < 0.5) & (y == 0))
    out: Array = np.where(p == 0.5, 0.5, picked_winner.astype(np.float64))
    return out


EACH = {"log_loss": log_loss_each, "brier": brier_each, "accuracy": correct_each}


def log_loss(y: Array, p: Array) -> float:
    return float(log_loss_each(y, p).mean())


def brier(y: Array, p: Array) -> float:
    return float(brier_each(y, p).mean())


def accuracy(y: Array, p: Array) -> float:
    return float(correct_each(y, p).mean())


@dataclass(frozen=True)
class BootstrapResult:
    """Mean of (model 1 - model 2) per match, with a 95% percentile interval."""

    diff: float
    low: float
    high: float


def paired_bootstrap(
    y: Array, p1: Array, p2: Array, *, metric: Metric, n_resamples: int = 2000, seed: int = 0
) -> BootstrapResult:
    """Resample *matches* (the same ones for both models) and recompute the difference.

    Pairing matters: both models face the same easy and hard matches, so most of the
    match-to-match noise cancels and the interval is far narrower than comparing two
    separate intervals.
    """
    each = EACH[metric]
    d = each(y, p1) - each(y, p2)
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(d), size=(n_resamples, len(d)))
    means = d[idx].mean(axis=1)
    low, high = np.percentile(means, [2.5, 97.5])
    return BootstrapResult(diff=float(d.mean()), low=float(low), high=float(high))
