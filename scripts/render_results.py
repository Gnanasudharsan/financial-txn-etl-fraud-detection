"""Render the measured-results tables in docs/RESULTS.md from docs/results/*.json.

Numbers are never typed by hand: the pipeline writes JSON, this script renders it between
``<!-- BEGIN:<name> -->`` / ``<!-- END:<name> -->`` markers.

Usage: uv run python scripts/render_results.py
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "docs" / "RESULTS.md"
README = ROOT / "README.md"
DATA = ROOT / "docs" / "results"


def _load(name: str) -> dict[str, Any] | None:
    p = DATA / f"{name}.json"
    return json.loads(p.read_text()) if p.exists() else None


def _pct(x: float) -> str:
    return f"{x * 100:.2f}%"


def model_table(m: dict[str, Any]) -> str:
    v, t = m["valid"], m["test"]
    rows = [
        ("PR-AUC (average precision)", f"{v['pr_auc']:.4f}", f"{t['pr_auc']:.4f}"),
        ("ROC-AUC", f"{v['roc_auc']:.6f}", f"{t['roc_auc']:.6f}"),
        ("Recall @ threshold", _pct(v["recall"]), _pct(t["recall"])),
        ("Precision @ threshold", _pct(v["precision"]), _pct(t["precision"])),
        ("F1 @ threshold", f"{v['f1']:.4f}", f"{t['f1']:.4f}"),
        ("Alert rate", _pct(v["alert_rate"]), _pct(t["alert_rate"])),
        (
            "TP / FP / FN / TN",
            f"{v['tp']:,} / {v['fp']:,} / {v['fn']:,} / {v['tn']:,}",
            f"{t['tp']:,} / {t['fp']:,} / {t['fn']:,} / {t['tn']:,}",
        ),
        (
            "Rows (fraud)",
            f"{v['rows']:,} ({v['positives']:,})",
            f"{t['rows']:,} ({t['positives']:,})",
        ),
    ]
    out = ["| Metric | Validation (May) | Test (June) |", "|---|---|---|"]
    out += [f"| {a} | {b} | {c} |" for a, b, c in rows]
    out.append("")
    out.append(
        f"Threshold **{m['threshold']:.6f}** (chosen on validation for recall ≥ "
        f"{m['target_recall']:.0%}); best iteration {m['best_iteration']}; "
        f"scale_pos_weight {m['scale_pos_weight']}; training rows {m['train']['rows']:,} "
        f"({m['train']['positives']:,} fraud); "
        f"MLflow model `{m['model_name']}` v{m['model_version']}; "
        f"features table Delta version {m['features_table_version']}."
    )
    out.append("")
    out.append("Top features by share of total gain:")
    out.append("")
    out += [f"- `{k}` — {v:.1%}" for k, v in m["top_features_gain_share"].items()]
    return "\n".join(out)


def recon_table(r: dict[str, Any]) -> str:
    out = ["| Category | Rows |", "|---|---|"]
    out += [f"| {k} | {v:,} |" for k, v in sorted(r["by_category"].items(), key=lambda kv: -kv[1])]
    if r.get("accuracy_vs_truth") is not None:
        out += [
            "",
            f"Classification accuracy vs injected ground truth: **{r['accuracy_vs_truth']:.5%}** "
            f"({r['truth_compared']:,} records compared).",
        ]
    return "\n".join(out)


def pipeline_table(p: dict[str, Any]) -> str:
    out = ["| Step | Wall clock | Rows in | Rows out | Quarantined |", "|---|---|---|---|---|"]
    for step, v in p["steps"].items():
        out.append(
            f"| {step} | {v['seconds']:,.1f} s | {v['rows_in']:,} | {v['rows_out']:,} | "
            f"{v['rows_quarantined']:,} |"
        )
    out.append(f"| **total** | **{p['total_seconds'] / 60:,.1f} min** | | | |")
    return "\n".join(out)


def readme_table(
    m: dict[str, Any] | None, r: dict[str, Any] | None, p: dict[str, Any] | None
) -> str:
    rows: list[tuple[str, str]] = []
    if p:
        gen = p["generation_counts"]
        silver = p["steps"]["silver"]
        rows += [
            (
                "Transactions generated (clean / raw incl. injected defects)",
                f"{gen['clean_transactions']:,} / {gen['raw_transaction_rows']:,}",
            ),
            (
                "Silver rows published / quarantined",
                f"{silver['rows_out']:,} / {silver['rows_quarantined']:,} "
                f"({silver['rows_quarantined'] / silver['rows_in']:.2%})",
            ),
            (
                "End-to-end pipeline runtime (laptop, `local[*]`)",
                f"{p['total_seconds'] / 60:.1f} min",
            ),
        ]
    if m:
        t = m["test"]
        rows += [
            (
                "Fraud model PR-AUC / ROC-AUC (held-out June)",
                f"{t['pr_auc']:.4f} / {t['roc_auc']:.6f}",
            ),
            (
                "Recall / precision at validation-tuned threshold (June)",
                f"{t['recall']:.2%} / {t['precision']:.2%}",
            ),
        ]
    if r:
        rows += [
            (
                "Reconciliation: records classified / breaks found",
                f"{sum(r['by_category'].values()):,} / {r['breaks']:,}",
            ),
            (
                "Recon accuracy vs injected ground truth",
                f"{r['accuracy_vs_truth']:.5%} ({r['truth_compared'] - r['ledger_side_correct']:,} "
                f"of {r['truth_compared']:,} misclassified)",
            ),
            (
                "Recon match + publish + break report runtime",
                f"{r['total_seconds_incl_report']:,.1f} s",
            ),
        ]
    out = ["| Metric | Measured |", "|---|---|"] + [f"| {a} | {b} |" for a, b in rows]
    return "\n".join(out)


def _sub(text: str, name: str, body: str, path: Path) -> str:
    pattern = re.compile(rf"(<!-- BEGIN:{name} -->\n).*?(\n<!-- END:{name} -->)", re.S)
    text, n = pattern.subn(lambda mm: mm.group(1) + body + mm.group(2), text)
    if n == 0:
        raise SystemExit(f"marker for {name!r} not found in {path}")
    return text


def render() -> None:
    text = RESULTS.read_text()
    blocks = {}
    if (m := _load("model_metrics")) is not None:
        blocks["model"] = model_table(m)
    if (r := _load("recon_metrics")) is not None:
        blocks["recon"] = recon_table(r)
    if (p := _load("pipeline_run")) is not None:
        blocks["pipeline"] = pipeline_table(p)
    for name, body in blocks.items():
        text = _sub(text, name, body, RESULTS)
    RESULTS.write_text(text)
    readme = _sub(README.read_text(), "readme-results", readme_table(m, r, p), README)
    README.write_text(readme)
    print(f"rendered {sorted(blocks)} into {RESULTS.relative_to(ROOT)} and the README table")


if __name__ == "__main__":
    render()
