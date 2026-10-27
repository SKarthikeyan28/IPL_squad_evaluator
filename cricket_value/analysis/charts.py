"""Chart style and helpers for the exploration notebook.

Colours are the dataviz reference palette (light mode): one blue for every single-series
chart, recessive hairline grids, ink colours for text. Every rate is drawn as a point
with its 95% interval against a 50% reference line, because a rate without its
interval invites over-reading one noisy season.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib as mpl
import pandas as pd
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.ticker import PercentFormatter

SURFACE = "#fcfcfb"
SERIES = "#2a78d6"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"


def apply_style() -> None:
    mpl.rcParams.update(
        {
            "figure.facecolor": SURFACE,
            "axes.facecolor": SURFACE,
            "savefig.facecolor": SURFACE,
            "figure.dpi": 110,
            "savefig.dpi": 150,
            "savefig.bbox": "tight",
            "font.family": "sans-serif",
            "font.sans-serif": ["Helvetica Neue", "Arial", "DejaVu Sans"],
            "font.size": 10,
            "text.color": INK,
            "axes.titlesize": 12,
            "axes.titleweight": "bold",
            "axes.titlelocation": "left",
            "axes.titlepad": 12,
            "axes.labelcolor": INK_SECONDARY,
            "axes.edgecolor": AXIS,
            "axes.linewidth": 0.8,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.grid": True,
            "axes.grid.axis": "y",
            "grid.color": GRID,
            "grid.linewidth": 0.6,
            "xtick.color": INK_MUTED,
            "ytick.color": INK_MUTED,
            "xtick.labelcolor": INK_SECONDARY,
            "ytick.labelcolor": INK_SECONDARY,
        }
    )


def rate_over_seasons(
    ax: Axes, df: pd.DataFrame, *, title: str, ylabel: str, overall: dict[str, float]
) -> None:
    """Per-season rate with 95% intervals, a 50% reference and the all-seasons rate."""
    seasons = df["season"].astype(int)
    ax.axhline(0.5, color=INK_MUTED, linewidth=1, zorder=1)
    ax.errorbar(
        seasons,
        df["rate"],
        yerr=[df["rate"] - df["low"], df["high"] - df["rate"]],
        fmt="o",
        color=SERIES,
        markersize=6,
        elinewidth=1.5,
        capsize=0,
        zorder=3,
    )
    ax.axhspan(overall["low"], overall["high"], color=SERIES, alpha=0.08, linewidth=0, zorder=0)
    ax.annotate(
        f"All seasons {overall['rate']:.1%} ({overall['low']:.1%}-{overall['high']:.1%})",
        xy=(1, overall["rate"]),
        xycoords=("axes fraction", "data"),
        xytext=(4, 0),
        textcoords="offset points",
        va="center",
        color=INK_SECONDARY,
        fontsize=9,
    )
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.set_ylim(0.2, 0.9)
    ax.yaxis.set_major_formatter(PercentFormatter(1.0, decimals=0))
    ax.set_xticks(seasons)
    ax.tick_params(axis="x", labelrotation=90)


def rate_by_group(ax: Axes, df: pd.DataFrame, label: str, *, title: str, xlabel: str) -> None:
    """Horizontal points with 95% intervals, one row per group, sorted by rate."""
    df = df.sort_values("rate").reset_index(drop=True)
    ax.axvline(0.5, color=INK_MUTED, linewidth=1, zorder=1)
    ax.errorbar(
        df["rate"],
        df.index,
        xerr=[df["rate"] - df["low"], df["high"] - df["rate"]],
        fmt="o",
        color=SERIES,
        markersize=6,
        elinewidth=1.5,
        capsize=0,
        zorder=3,
    )
    ax.set_yticks(df.index, [f"{name} (n={n})" for name, n in zip(df[label], df["n"], strict=True)])
    ax.grid(axis="y", visible=False)
    ax.grid(axis="x", visible=True)
    ax.set_xlim(0.1, 0.9)
    ax.xaxis.set_major_formatter(PercentFormatter(1.0, decimals=0))
    ax.set_title(title)
    ax.set_xlabel(xlabel)


def bars_over_seasons(
    ax: Axes, seasons: pd.Series, values: pd.Series, *, title: str, ylabel: str
) -> None:
    ax.bar(seasons.astype(int), values, width=0.7, color=SERIES, zorder=2)
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.set_xticks(seasons.astype(int))
    ax.tick_params(axis="x", labelrotation=90)


def line_over_seasons(
    ax: Axes, seasons: pd.Series, values: pd.Series, *, title: str, ylabel: str
) -> None:
    ax.plot(
        seasons.astype(int), values, color=SERIES, linewidth=2, marker="o", markersize=6, zorder=3
    )
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.set_xticks(seasons.astype(int))
    ax.tick_params(axis="x", labelrotation=90)


def save(fig: Figure, name: str, out_dir: Path) -> None:
    """Write the figure to reports/figures. The notebook still displays it."""
    out_dir.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_dir / f"{name}.png")
