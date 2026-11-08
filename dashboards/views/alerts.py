from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st
from common import SERIES, fmt_pct, load, split_filter, style

st.title("Alert volume and precision")
st.caption(
    "An alert is a transaction whose model score is at or above the threshold tuned on May "
    "(validation) for ≥ 95% recall."
)
kpis = load("daily_fraud_kpis")
splits = split_filter("alert_split", ("valid", "test"))
df = kpis[kpis["split"].isin(splits)].groupby("event_date", as_index=False).sum(numeric_only=True)
df["precision"] = df["true_positives"] / df["alert_count"]
df["recall"] = df["true_positives"] / df["fraud_count"]

c1, c2, c3 = st.columns(3)
c1.metric("Alerts", f"{int(df['alert_count'].sum()):,}")
c2.metric("Precision", fmt_pct(df["true_positives"].sum() / df["alert_count"].sum()))
c3.metric("Recall", fmt_pct(df["true_positives"].sum() / df["fraud_count"].sum()))

fig = go.Figure()
fig.add_bar(
    x=df["event_date"],
    y=df["true_positives"],
    name="True positives",
    marker_color=SERIES[2],
    hovertemplate="%{y:,} true positives<extra></extra>",
)
fig.add_bar(
    x=df["event_date"],
    y=df["alert_count"] - df["true_positives"],
    name="False positives",
    marker_color=SERIES[0],
    hovertemplate="%{y:,} false positives<extra></extra>",
)
fig.update_layout(barmode="stack", title="Daily alerts split by outcome", bargap=0.15)
st.plotly_chart(style(fig, y_title="alerts / day"), use_container_width=True)

fig = go.Figure()
fig.add_scatter(
    x=df["event_date"],
    y=df["precision"],
    name="Precision",
    line_color=SERIES[0],
    hovertemplate="%{y:.1%}<extra>precision</extra>",
)
fig.add_scatter(
    x=df["event_date"],
    y=df["recall"],
    name="Recall",
    line_color=SERIES[2],
    hovertemplate="%{y:.1%}<extra>recall</extra>",
)
fig.update_yaxes(tickformat=".0%", range=[0, 1.05])
st.plotly_chart(
    style(fig).update_layout(title="Daily precision and recall"), use_container_width=True
)

st.subheader("Capture rate by fraud pattern")
ft = load("fraud_type_capture")
ft = ft[ft["split"].isin(splits)].groupby("fraud_type", as_index=False).sum(numeric_only=True)
ft["capture_rate"] = ft["caught"] / ft["fraud"]
ft = ft.sort_values("capture_rate")
fig = go.Figure(
    go.Bar(
        x=ft["capture_rate"],
        y=ft["fraud_type"],
        orientation="h",
        marker_color=SERIES[0],
        customdata=ft[["caught", "fraud"]],
        hovertemplate="%{x:.1%} caught (%{customdata[0]:,} of %{customdata[1]:,})<extra></extra>",
    )
)
fig.update_xaxes(tickformat=".0%", range=[0, 1])
st.plotly_chart(style(fig, height=300).update_layout(hovermode="closest"), use_container_width=True)

with st.expander("Table view"):
    st.dataframe(df, use_container_width=True, hide_index=True)
    st.dataframe(ft, use_container_width=True, hide_index=True)
