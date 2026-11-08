from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st
from common import SERIES, load, style

st.title("Data quality health")
dq = load("dq_health")
qr = load("dq_quarantine_by_reason")

c1, c2, c3 = st.columns(3)
c1.metric("Rows landed", f"{int(dq['rows_total'].sum()):,}")
c2.metric("Quarantined", f"{int(dq['rows_quarantined'].sum()):,}")
c3.metric("Pass rate", f"{dq['rows_valid'].sum() / dq['rows_total'].sum():.2%}")

fig = go.Figure(
    go.Scatter(
        x=dq["landing_date"],
        y=dq["pass_rate"],
        name="Pass rate",
        line_color=SERIES[0],
        hovertemplate="%{y:.2%}<extra></extra>",
    )
)
fig.update_yaxes(tickformat=".1%")
st.plotly_chart(
    style(fig, y_title="share of rows valid").update_layout(title="Daily pass rate"),
    use_container_width=True,
)

reasons = qr.groupby("primary_reason", as_index=False)["rows"].sum().sort_values("rows")
top = reasons.tail(7)["primary_reason"].tolist()[::-1]
qr = qr.assign(reason=qr["primary_reason"].where(qr["primary_reason"].isin(top), "Other"))
daily = qr.pivot_table(index="landing_date", columns="reason", values="rows", aggfunc="sum").fillna(
    0
)
fig = go.Figure()
for i, r in enumerate([*top, "Other"]):
    if r in daily:
        fig.add_bar(
            x=daily.index, y=daily[r], name=r, marker_color=SERIES[i] if r != "Other" else "#a3a29d"
        )
fig.update_layout(barmode="stack", title="Quarantined rows per day by primary reason", bargap=0.1)
st.plotly_chart(style(fig, y_title="rows"), use_container_width=True)

fig = go.Figure(
    go.Bar(
        x=reasons["rows"],
        y=reasons["primary_reason"],
        orientation="h",
        marker_color=SERIES[0],
        hovertemplate="%{y}: %{x:,} rows<extra></extra>",
    )
)
st.plotly_chart(
    style(fig, height=420).update_layout(
        title="Quarantine totals by reason code", hovermode="closest"
    ),
    use_container_width=True,
)

with st.expander("Table view"):
    st.dataframe(
        reasons.sort_values("rows", ascending=False), use_container_width=True, hide_index=True
    )
