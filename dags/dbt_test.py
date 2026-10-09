import os
import subprocess
from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

DBT_PATH = "/usr/local/airflow/dbt/demo"


# approximation of how farmer's DAGs run dbt
def _run_dbt():
    # a common pattern: bare `dbt` on PATH, run through bash, env copied from os.environ
    bash_cmd = f"""
    set -euo pipefail
    cd "{DBT_PATH}"
    dbt run --fail-fast
    """
    env = {**os.environ, "EXAMPLE_DB_USER": "dummy"}
    result = subprocess.run(bash_cmd, shell=True, env=env, executable="/bin/bash")
    if result.returncode != 0:
        raise RuntimeError("dbt run failed")


with DAG(dag_id="dbt_test", schedule=None, start_date=datetime(2025, 1, 1), catchup=False):
    PythonOperator(task_id="run_dbt", python_callable=_run_dbt)
