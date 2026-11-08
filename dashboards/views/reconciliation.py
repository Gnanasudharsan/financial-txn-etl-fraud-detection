from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st
from common import SERIES, load, style

st.title("Ledger vs settlement reconciliation")
rs = load("reconciliation_summary")
order = ["amount_mismatch", "timing", "missing_in_settlement", "missing_in_ledger", "duplicate"]
color = dict(zip(order, SERIES[:5], strict=True))

totals = rs.groupby("category", as_index=False)[["records", "abs_amount_diff"]].sum()
matched = totals[totals["category"].isin(["exact_match", "tolerance_match"])]["records"].sum()
c1, c2, c3 = st.columns(3)
c1.metric("Records reconciled", f"{int(totals['records'].sum()):,}")
c2.metric("Matched (exact + tolerance)", f"{matched / totals['records'].sum():.2%}")
c3.metric("Breaks", f"{int(totals[totals['category'].isin(order)]['records'].sum()):,}")

brk = totals[totals["category"].isin(order)].set_index("category").reindex(order).reset_index()
fig = go.Figure(
    go.Bar(
        x=brk["records"],
        y=brk["category"],
        orientation="h",
        marker_color=[color[c] for c in brk["category"]],
        hovertemplate="%{y}: %{x:,} records<extra></extra>",
    )
)
st.plotly_chart(
    style(fig, height=280).update_layout(title="Breaks by type", hovermode="closest"),
    use_container_width=True,
)

daily = (
    rs[rs["category"].isin(order)]
    .pivot_table(index="business_date", columns="category", values="records", aggfunc="sum")
    .fillna(0)
)
fig = go.Figure()
for c in order:
    if c in daily:
        fig.add_bar(x=daily.index, y=daily[c], name=c, marker_color=color[c])
fig.update_layout(barmode="stack", title="Breaks per business date", bargap=0.1)
st.plotly_chart(style(fig, y_title="records"), use_container_width=True)

with st.expander("Table view"):
    st.dataframe(
        totals.sort_values("records", ascending=False), use_container_width=True, hide_index=True
    )
