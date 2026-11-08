from __future__ import annotations

import json

import streamlit as st
from common import load

st.title("Pipeline audit log")
st.caption(
    "One row per job run from the `audit.pipeline_runs` Delta table. `tables_written` holds "
    "the exact Delta version of every table a run produced, so any run can be reproduced "
    "with time travel."
)
audit = load("pipeline_audit")
jobs = st.multiselect("Job", sorted(audit["job_name"].unique()))
status = st.multiselect("Status", sorted(audit["status"].unique()))
df = audit
if jobs:
    df = df[df["job_name"].isin(jobs)]
if status:
    df = df[df["status"].isin(status)]
st.dataframe(
    df,
    use_container_width=True,
    hide_index=True,
    column_config={"dq_pass_rate": st.column_config.NumberColumn(format="%.4f")},
)

run = st.selectbox("Inspect run", df["run_id"].tolist())
if run:
    row = df[df["run_id"] == run].iloc[0]
    st.json(json.loads(row["tables_written"] or "{}"))
    st.code(f"uv run fintxn time-travel {run}", language="bash")
