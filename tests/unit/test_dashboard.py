"""Every dashboard page renders without exceptions against the committed extracts."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
DASH = ROOT / "dashboards"
VIEWS = sorted((DASH / "views").glob("*.py"))

pytest.importorskip("streamlit")
pytestmark = pytest.mark.skipif(
    not (DASH / "data" / "daily_fraud_kpis.parquet").exists(), reason="run `make export` first"
)


@pytest.mark.parametrize("view", VIEWS, ids=[v.stem for v in VIEWS])
def test_view_renders(view: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from streamlit.testing.v1 import AppTest

    monkeypatch.syspath_prepend(str(DASH))
    sys.modules.pop("common", None)
    at = AppTest.from_file(str(view), default_timeout=60)
    at.run()
    assert not at.exception, [e.value for e in at.exception]
    assert at.title, "page should render a title"


def test_extracts_are_small_and_pii_free() -> None:
    import pandas as pd

    total = 0
    for f in (DASH / "data").glob("*.parquet"):
        total += f.stat().st_size
        cols = {c.lower() for c in pd.read_parquet(f).columns}
        assert not cols & {"email", "phone", "first_name", "last_name", "card_number", "card_token"}
    assert total < 2_000_000
