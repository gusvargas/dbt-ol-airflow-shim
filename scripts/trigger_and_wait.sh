#!/usr/bin/env bash
# usage: scripts/trigger_and_wait.sh <dag_id>
# triggers a dag run inside the local scheduler container and prints the final task states.
set -euo pipefail
DAG_ID="$1"
RUN_ID="manual__test_$(date +%Y%m%dT%H%M%S)"
SCHEDULER="$(docker ps --format '{{.Names}}' | grep -- '-scheduler-1$' | head -1)"

docker exec "$SCHEDULER" bash -c "
  set -euo pipefail
  airflow dags unpause $DAG_ID >/dev/null 2>&1
  airflow dags trigger $DAG_ID --run-id $RUN_ID >/dev/null 2>&1
  echo 'triggered $DAG_ID $RUN_ID'
  until airflow dags list-runs $DAG_ID -o plain 2>/dev/null | grep -F $RUN_ID | grep -qE ' (success|failed) '; do sleep 3; done
  airflow dags list-runs $DAG_ID -o plain 2>/dev/null | grep -F $RUN_ID | grep -oE ' (success|failed) ' | sed 's/^ */dag run state: /'
  airflow tasks states-for-dag-run $DAG_ID $RUN_ID -o json 2>/dev/null | grep '^\[' \
    | python -c 'import json,sys; [print(\"  \" + t[\"task_id\"] + \"  \" + t[\"state\"]) for t in json.load(sys.stdin)]'
"
