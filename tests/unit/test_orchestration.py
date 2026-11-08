"""The Airflow DAG and the Databricks Job must describe the same pipeline."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def _job() -> dict:
    spec = yaml.safe_load((ROOT / "databricks/resources/fintxn_pipeline.job.yml").read_text())
    return spec["resources"]["jobs"]["fintxn_pipeline"]


def _databricks_edges() -> set[tuple[str, str]]:
    edges = set()
    for t in _job()["tasks"]:
        for d in t.get("depends_on", []):
            if d["task_key"] != "setup_uc":
                edges.add((d["task_key"], t["task_key"]))
    return edges


def _airflow_edges() -> set[tuple[str, str]]:
    src = (ROOT / "dags/fintxn_pipeline.py").read_text()
    edges: set[tuple[str, str]] = set()
    for line in src.splitlines():
        if ">>" not in line:
            continue
        parts = [p.strip() for p in line.split(">>")]
        for a, b in zip(parts, parts[1:], strict=False):
            for x in re.findall(r"\w+", a):
                for y in re.findall(r"\w+", b):
                    edges.add((x, y))
    return edges


def test_same_tasks() -> None:
    src = (ROOT / "dags/fintxn_pipeline.py").read_text()
    airflow_tasks = set(re.findall(r'cli\("(\w+)"', src))
    databricks_tasks = {t["task_key"] for t in _job()["tasks"]} - {"setup_uc"}
    assert airflow_tasks == databricks_tasks


def test_same_dependencies() -> None:
    assert _airflow_edges() == _databricks_edges()


def test_every_task_uses_the_cli_entry_point() -> None:
    for t in _job()["tasks"]:
        wt = t["python_wheel_task"]
        assert wt["entry_point"] == "fintxn" and wt["parameters"][:2] == ["--env", "databricks"]
