import logging
import os

from airflow.providers.standard.operators.python import PythonOperator

log = logging.getLogger(__name__)


def _set_openlineage_parent_env(context):
    # runs in the task process right before execute(), so anything the python callable launches
    # with os.environ (e.g. `dbt` -> dbt-ol) sees the airflow task as its openlineage parent
    from airflow.providers.openlineage.plugins import macros as ol

    ti = context["ti"]
    os.environ["OPENLINEAGE_PARENT_ID"] = ol.lineage_parent_id(ti)
    os.environ["OPENLINEAGE_ROOT_PARENT_ID"] = ol.lineage_root_parent_id(ti)
    log.info(
        "set OPENLINEAGE_PARENT_ID=%s OPENLINEAGE_ROOT_PARENT_ID=%s",
        os.environ["OPENLINEAGE_PARENT_ID"],
        os.environ["OPENLINEAGE_ROOT_PARENT_ID"],
    )


def task_policy(task):
    # what a task policy is: airflow looks for a function named `task_policy` in
    # `airflow_local_settings.py` and calls it once for every task, in every dag, whenever a dag
    # file is parsed. it's a cluster-wide hook for adjusting tasks in place, so no dag file has to
    # change. it runs at parse time (not when the task runs), and it runs again in the worker
    # process that executes the task, so the change below is present when the task actually runs.
    #
    # what this policy does: for every PythonOperator, it registers _set_openlineage_parent_env as
    # an `on_execute_callback`, a hook airflow calls just before the task's python function runs.

    # only touch PythonOperator tasks (incl. @task functions); every other task is left as-is
    if not isinstance(task, PythonOperator):
        return

    # the dag author may already have set on_execute_callback to nothing, a single function, or a
    # list of functions. normalize to a list so we can add ours without dropping theirs.
    existing = task.on_execute_callback
    if existing is None:
        existing = []
    elif callable(existing):
        existing = [existing]
    else:
        existing = list(existing)

    # the policy can run more than once for the same task, so don't add our callback twice
    if _set_openlineage_parent_env in existing:
        return

    # put ours first so the env vars are set before any of the dag author's callbacks run
    task.on_execute_callback = [_set_openlineage_parent_env, *existing]
