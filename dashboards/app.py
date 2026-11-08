"""Fintxn Fraud & Reconciliation dashboard (Streamlit). Run: `make dashboard`."""

from __future__ import annotations

import streamlit as st

st.set_page_config(page_title="Fintxn Lakehouse", page_icon="💳", layout="wide")

pages = [
    st.Page("views/overview.py", title="Overview", icon="🏠", default=True),
    st.Page("views/fraud_kpis.py", title="Fraud KPIs", icon="📈"),
    st.Page("views/alerts.py", title="Alerts & precision", icon="🚨"),
    st.Page("views/merchant_risk.py", title="Merchant risk", icon="🏪"),
    st.Page("views/reconciliation.py", title="Reconciliation", icon="⚖️"),
    st.Page("views/data_quality.py", title="Data quality", icon="🧪"),
    st.Page("views/audit_log.py", title="Pipeline audit", icon="🧾"),
]
st.navigation(pages).run()
