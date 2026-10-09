"""checks that every dbt-ol event is linked to the airflow task that ran it.

usage: python3 scripts/verify_lineage.py [dag_id]
"""

import glob
import json
import os
import sys

EVENTS = os.path.join(os.path.dirname(__file__), "..", "include", "ol_events")


def load(kind):
    events = []
    for path in sorted(glob.glob(os.path.join(EVENTS, kind, "*.json"))):
        with open(path) as f:
            events.append(json.load(f))
    return events


def parent_of(event):
    return event["run"]["facets"].get("parent")


airflow_events = load("airflow")
dbt_events = load("dbt")
dag_filter = sys.argv[1] if len(sys.argv) > 1 else None

# airflow job names are "<dag_id>" for the dag run and "<dag_id>.<task_id>" for a task
dag_runs = {}
task_runs = {}
for e in airflow_events:
    name = e["job"]["name"]
    if "." in name:
        task_runs[e["run"]["runId"]] = (name, e["job"]["namespace"])
    else:
        dag_runs[e["run"]["runId"]] = name

failures = 0
dbt_run_ids = {}
print("== dbt run (wrapping) events -> airflow task")
for e in dbt_events:
    if not e["job"]["name"].startswith("dbt-run-"):
        continue
    parent = parent_of(e)
    if not parent:
        print(f"  NO PARENT  {e['eventType']:<8} {e['run']['runId']}")
        failures += 1
        continue
    task = task_runs.get(parent["run"]["runId"])
    root_id = parent.get("root", {}).get("run", {}).get("runId")
    dag = dag_runs.get(root_id)
    if dag_filter and (dag != dag_filter):
        continue
    dbt_run_ids[e["run"]["runId"]] = (task, root_id)
    ok = task is not None and dag is not None and task[0].startswith(dag + ".")
    failures += not ok
    print(
        f"  {'OK ' if ok else 'BAD'} {e['eventType']:<8} dbt run {e['run']['runId']}\n"
        f"        parent {parent['job']['namespace']}/{parent['job']['name']}/{parent['run']['runId']}"
        f"  (airflow task: {task[0] if task else 'NOT FOUND'})\n"
        f"        root   {parent['root']['job']['namespace']}/{parent['root']['job']['name']}/{root_id}"
        f"  (airflow dag run: {dag or 'NOT FOUND'})"
    )

print("== dbt model events -> dbt run -> airflow")
for e in dbt_events:
    if e["job"]["name"].startswith("dbt-run-"):
        continue
    parent = parent_of(e)
    if not parent or parent["run"]["runId"] not in dbt_run_ids:
        if not dag_filter:
            print(f"  BAD {e['eventType']:<8} {e['job']['name']} has no known dbt run parent")
            failures += 1
        continue
    task, root_id = dbt_run_ids[parent["run"]["runId"]]
    model_root = parent.get("root", {}).get("run", {}).get("runId")
    ok = model_root == root_id
    failures += not ok
    print(
        f"  {'OK ' if ok else 'BAD'} {e['eventType']:<8} {e['job']['name']}  parent=dbt run "
        f"{parent['run']['runId']}  root={model_root}  (airflow task {task[0] if task else '?'})"
    )

print(f"\n{failures} problem(s)")
sys.exit(1 if failures else 0)
