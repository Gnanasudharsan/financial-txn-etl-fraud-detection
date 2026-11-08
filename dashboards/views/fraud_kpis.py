from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st
from common import SERIES, load, split_filter, style

st.title("Fraud KPIs over time")
kpis = load("daily_fraud_kpis")
splits = split_filter("kpi_split")
df = kpis[kpis["split"].isin(splits)].groupby("event_date", as_index=False).sum(numeric_only=True)
df["fraud_rate_bps"] = df["fraud_count"] / df["txn_count"] * 10_000

fig = go.Figure(
    go.Scatter(
        x=df["event_date"],
        y=df["txn_count"],
        name="Transactions",
        line_color=SERIES[0],
        hovertemplate="%{y:,} txns<extra></extra>",
    )
)
st.plotly_chart(
    style(fig, y_title="transactions / day").update_layout(title="Daily transaction volume"),
    use_container_width=True,
)

fig = go.Figure(
    go.Scatter(
        x=df["event_date"],
        y=df["fraud_rate_bps"],
        name="Fraud rate",
        line_color=SERIES[1],
        hovertemplate="%{y:.1f} bps<extra></extra>",
    )
)
st.plotly_chart(
    style(fig, y_title="basis points").update_layout(
        title="Daily fraud rate (bps of transactions)"
    ),
    use_container_width=True,
)

fig = go.Figure(
    go.Bar(
        x=df["event_date"],
        y=df["fraud_amount_usd"],
        name="Fraud amount",
        marker_color=SERIES[1],
        hovertemplate="$%{y:,.0f}<extra></extra>",
    )
)
st.plotly_chart(
    style(fig, y_title="USD").update_layout(title="Fraud amount per day (USD)", bargap=0.15),
    use_container_width=True,
)

with st.expander("Table view"):
    st.dataframe(df, use_container_width=True, hide_index=True)
