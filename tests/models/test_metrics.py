from __future__ import annotations

import math

import numpy as np
import pytest

from cricket_value.models.metrics import accuracy, brier, log_loss, paired_bootstrap


def test_log_loss_of_a_coin_flip_is_ln2() -> None:
    assert log_loss(np.array([1.0, 0.0]), np.array([0.5, 0.5])) == pytest.approx(math.log(2))


def test_log_loss_clips_certain_wrong_predictions() -> None:
    assert math.isfinite(log_loss(np.array([1.0]), np.array([0.0])))


def test_brier_and_accuracy() -> None:
    y = np.array([1.0, 0.0, 1.0, 0.0])
    p = np.array([0.8, 0.3, 0.4, 0.5])

    assert brier(y, p) == pytest.approx((0.04 + 0.09 + 0.36 + 0.25) / 4)
    assert accuracy(y, p) == pytest.approx(2.5 / 4)  # a 0.5 forecast earns half credit


def test_paired_bootstrap_of_identical_models_is_exactly_zero() -> None:
    y = np.array([1.0, 0.0, 1.0, 1.0, 0.0])
    p = np.array([0.6, 0.4, 0.7, 0.5, 0.2])

    result = paired_bootstrap(y, p, p, metric="log_loss", n_resamples=200, seed=0)

    assert (result.diff, result.low, result.high) == (0.0, 0.0, 0.0)


def test_paired_bootstrap_detects_a_clearly_better_model() -> None:
    rng = np.random.default_rng(1)
    y = rng.integers(0, 2, 400).astype(float)
    good = np.where(y == 1, 0.8, 0.2)
    coin = np.full_like(y, 0.5)

    result = paired_bootstrap(y, good, coin, metric="log_loss", n_resamples=500, seed=0)

    assert result.high < 0  # better = lower loss, and the whole interval is below zero


def test_paired_bootstrap_is_reproducible() -> None:
    rng = np.random.default_rng(2)
    y = rng.integers(0, 2, 100).astype(float)
    p1, p2 = rng.uniform(0.2, 0.8, 100), rng.uniform(0.2, 0.8, 100)

    a = paired_bootstrap(y, p1, p2, metric="brier", n_resamples=300, seed=7)
    b = paired_bootstrap(y, p1, p2, metric="brier", n_resamples=300, seed=7)

    assert a == b
