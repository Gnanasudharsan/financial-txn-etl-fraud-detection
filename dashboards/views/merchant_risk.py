from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st
from common import SERIES, load, style

st.title("Merchant risk leaderboard")
st.caption("Top 500 merchants by confirmed fraud count (merchant names are synthetic).")
mr = load("merchant_risk_top")

c1, c2, c3 = st.columns(3)
tiers = c1.multiselect("Risk tier", ["high", "medium", "low"], default=["high", "medium"])
cats = c2.multiselect("Category", sorted(mr["category"].dropna().unique()))
min_txn = c3.number_input("Min transactions", min_value=0, value=50, step=50)
df = mr[mr["risk_tier"].isin(tiers) & (mr["txn_count"] >= min_txn)]
if cats:
    df = df[df["category"].isin(cats)]

top = df.sort_values("fraud_rate", ascending=False).head(20).iloc[::-1]
fig = go.Figure(
    go.Bar(
        x=top["fraud_rate"],
        y=top["merchant_name"] + " · " + top["merchant_id"],
        orientation="h",
        marker_color=SERIES[1],
        customdata=top[["fraud_count", "txn_count", "category"]],
        hovertemplate="%{x:.2%} fraud rate<br>%{customdata[0]:,} of %{customdata[1]:,} txns"
        "<br>%{customdata[2]}<extra></extra>",
    )
)
fig.update_xaxes(tickformat=".1%")
st.plotly_chart(
    style(fig, height=560).update_layout(
        title="20 highest fraud-rate merchants", hovermode="closest"
    ),
    use_container_width=True,
)

cols = [
    "merchant_id",
    "merchant_name",
    "category",
    "mcc_description",
    "city",
    "country",
    "txn_count",
    "fraud_count",
    "fraud_rate",
    "alert_count",
    "avg_score",
    "risk_tier",
]
st.dataframe(
    df[cols].sort_values("fraud_count", ascending=False),
    use_container_width=True,
    hide_index=True,
    column_config={
        "fraud_rate": st.column_config.NumberColumn(format="%.4f"),
        "avg_score": st.column_config.NumberColumn(format="%.4f"),
    },
)
