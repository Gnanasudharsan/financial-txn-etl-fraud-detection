"""Airflow DAG mirroring the Databricks Job (databricks/resources/fintxn_pipeline.job.yml).

Each task shells out to the same CLI used by the Makefile and the Databricks wheel task, so the
orchestration layer holds no business logic. Runs in the `airflow` docker-compose profile:

    make airflow-up   # http://localhost:8080 (credentials printed by `airflow standalone`)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}
ROWS = "{{ dag_run.conf.get('rows', 500000) if dag_run and dag_run.conf else 500000 }}"


def cli(task_id: str, command: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"fintxn --env local {command}",
        env={"TZ": "UTC"},
        append_env=True,
    )


with DAG(
    dag_id="fintxn_pipeline",
    description="Fintxn lakehouse: generate -> bronze -> silver -> gold -> ML -> recon -> export",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["fintxn", "lakehouse", "fraud"],
) as dag:
    generate = cli("generate", f"generate --rows {ROWS}")
    bronze = cli("bronze", "bronze")
    silver = cli("silver", "silver")
    gold = cli("gold", "gold")
    train = cli("train", "train")
    score = cli("score", "score")
    recon = cli("recon", "recon")
    export = cli("export", "export")

    generate >> bronze >> silver
    silver >> gold >> train >> score
    silver >> recon
    [score, recon] >> export
