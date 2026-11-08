from __future__ import annotations

import streamlit as st
from common import fmt_int, fmt_pct, load

st.title("Financial transaction lakehouse")
st.caption(
    "Synthetic banking data processed by a PySpark + Delta Lake medallion pipeline. "
    "All figures are aggregates; no customer PII is shipped with this app."
)

kpis = load("daily_fraud_kpis")
dq = load("dq_health")
recon = load("reconciliation_summary")
by_split = load("model_by_split").set_index("split")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Transactions scored", fmt_int(kpis["txn_count"].sum()))
c2.metric(
    "Confirmed fraud",
    fmt_int(kpis["fraud_count"].sum()),
    help="Ground-truth labels from fraud reports / chargebacks",
)
c3.metric("Rows passing DQ", fmt_pct(dq["rows_valid"].sum() / dq["rows_total"].sum(), 2))
breaks = recon.loc[recon["is_break"], "records"].sum()
c4.metric("Reconciliation breaks", fmt_int(breaks))

if "test" in by_split.index:
    t = by_split.loc["test"]
    st.subheader("Held-out test month (June)")
    a, b, c = st.columns(3)
    a.metric("Fraud recall", fmt_pct(t["true_positives"] / t["fraud"]))
    b.metric("Alert precision", fmt_pct(t["true_positives"] / t["alerts"]))
    c.metric("Alerts raised", fmt_int(t["alerts"]))

st.markdown(
    """
**Pages**
- **Fraud KPIs**: daily volume, fraud rate and fraud amount
- **Alerts & precision**: model alerts, captured fraud, and precision by day and fraud pattern
- **Merchant risk**: leaderboard of the riskiest merchants
- **Reconciliation**: ledger vs settlement breaks by type
- **Data quality**: pass rate and quarantine reasons over time
- **Pipeline audit**: every job run with rows in/out and Delta versions written
"""
)
