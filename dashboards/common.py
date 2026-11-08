"""Shared data loading and chart styling for the Streamlit dashboard.

Reads only the compact, aggregated, PII-free Parquet extracts in ``dashboards/data/`` (written by
``make export``), so the app deploys to Streamlit Community Cloud with no backend.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

DATA_DIR = Path(__file__).resolve().parent / "data"

# Validated categorical palette (fixed order, never cycled) - light-mode steps.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
STATUS = {"good": "#0ca30c", "warning": "#fab219", "serious": "#ec835a", "critical": "#d03b3b"}
INK_MUTED = "#8a8984"
SPLIT_COLOR = {"train": SERIES[0], "valid": SERIES[1], "test": SERIES[2]}


@st.cache_data(show_spinner=False)
def load(name: str) -> pd.DataFrame:
    path = DATA_DIR / f"{name}.parquet"
    if not path.exists():
        st.error(f"Missing `{path.name}`. Run `make export` to generate dashboard extracts.")
        st.stop()
    return pd.read_parquet(path)


def style(fig: go.Figure, height: int = 340, y_title: str | None = None) -> go.Figure:
    fig.update_layout(
        height=height,
        margin={"l": 8, "r": 8, "t": 36, "b": 8},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        hovermode="x unified",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0, "title": None},
        font={"size": 13},
    )
    fig.update_xaxes(showgrid=False, linecolor=INK_MUTED, title=None)
    fig.update_yaxes(gridcolor="rgba(138,137,132,0.18)", zeroline=False, title=y_title)
    fig.update_traces(selector={"type": "scatter"}, line={"width": 2})
    fig.update_traces(selector={"type": "bar"}, marker_line_width=0)
    return fig


def fmt_int(x: float) -> str:
    return f"{int(x):,}"


def fmt_pct(x: float, digits: int = 1) -> str:
    return "–" if pd.isna(x) else f"{x * 100:.{digits}f}%"


def split_filter(key: str, default: tuple[str, ...] = ("train", "valid", "test")) -> list[str]:
    return st.multiselect(
        "Data split",
        ["train", "valid", "test"],
        default=list(default),
        key=key,
        help="train = Jan–Apr, valid = May (threshold tuning), test = June (held out)",
    )
