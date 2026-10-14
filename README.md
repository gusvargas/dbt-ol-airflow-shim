# dbt-ol-airflow-shim

A proof of concept for instrumenting dbt with `dbt-ol` (OpenLineage) **without touching any call site of `dbt`**. It targets the case where DAGs run dbt from a `PythonOperator` that shells out with `subprocess` (`dbt run ...`, env copied from `os.environ`).

It's a local Astro project (Runtime 3.2-2, openlineage-dbt 1.54.0, dbt-core 1.12.5 + duckdb) with one test DAG, `dags/dbt_test.py`.

There are two components:

1. **A dbt shim** that redirects `dbt` calls to `dbt-ol`, while keeping the original `dbt` reachable.
2. **A way to give `dbt-ol` the OpenLineage env vars** it needs to link each dbt run to the Airflow task and DAG run that started it.

## 1. The dbt shim

### The script (`dbt-shim.sh`)

```bash
#!/usr/bin/env bash

# don't re-invoke dbt-ol if it's already been invoked
if [ -n "${DBT_OL_ACTIVE:-}" ]; then
  exec /usr/local/bin/dbt "$@"
fi

# set a flag so that subsequent calls to the shim routes
# to the real dbt
export DBT_OL_ACTIVE=1
exec dbt-ol --consume-structured-logs "$@"
```

- **Recursion guard.** dbt-ol runs dbt as `subprocess.Popen(["dbt", ...])`, which looks `dbt` up on `PATH` and so finds the shim again. `DBT_OL_ACTIVE` is passed down to that child, so the inner call goes to the real dbt. Without the guard, dbt-ol keeps starting dbt-ol and dbt never runs.
- **`--consume-structured-logs`.** dbt-ol reads dbt's JSON log stream while dbt runs, instead of parsing `run_results.json` at the end (its default). Without it, failures are close to invisible:
  - **A model fails at runtime** (e.g. a missing table): only this mode puts dbt's error on the model's `FAIL` event (`Runtime Error in model … Catalog Error: Table with name … does not exist!`). The default mode sends `FAIL` with no message.
  - **dbt fails before running any model** (e.g. a compilation error): the default mode sends nothing at all, because `run_results.json` isn't written. This mode still sends the dbt run's `START`/`FAIL`, but its message is generic (`` Command `dbt run` failed at … ``); the real error is only in dbt's console output.
  - It also sends one job per SQL statement (`<model>.sql.<n>`), parented to the model run, so expect more events.
  - dbt-ol strips the flag before calling dbt, leaves dbt's console output on stdout (so `| tee` still works) and exits with dbt's exit code. It leaves an empty `dbt-logs-<uuid>` directory in the dbt project directory after each run.
- Every command goes through dbt-ol, not only `run`. Non-run commands such as `compile` or `ls` may emit empty `dbt-run-*` events or print extra dbt-ol log lines to stdout.

### Setup (`Dockerfile`)

```dockerfile
FROM astrocrpublic.azurecr.io/runtime:3.2-2

COPY --chmod=755 dbt-shim.sh /usr/local/dbt-shim/bin/dbt
ENV PATH="/usr/local/dbt-shim/bin:${PATH}"
```

- The shim gets its own directory, which goes **first** on `PATH`. The pip-installed `dbt` stays untouched, so a later `pip install` can't overwrite the shim.
- The real dbt is called by absolute path (`/usr/local/bin/dbt` on this runtime), because `dbt` on `PATH` is the shim. If dbt lives somewhere else, for example a venv, change that one path. Check it with `which -a dbt` inside a task.

## 2. Providing OpenLineage env vars to dbt-ol

There are two kinds of env vars:

- **Static (same for every run):** namespace and transport. These are ordinary worker env vars; see below.
- **Per run (parent and root run ids):** these identify *this* task attempt and *this* DAG run, so they **can't be hard-coded in the shim**. Only Airflow knows them, when the task runs, from its task instance and the OpenLineage provider's macros. The shim is a static script in the image with no access to that context. So Airflow has to work them out and export them before dbt starts, which is what the task policy (`plugins/airflow_local_settings.py`) does:

```
task process (Airflow 3.2 task runner)
 1. task_policy (plugins/airflow_local_settings.py) has put _set_openlineage_parent_env
    first in every PythonOperator's on_execute_callback list (existing callbacks are kept)
 2. right before execute(), the callback sets in os.environ:
      OPENLINEAGE_PARENT_ID      = macros.lineage_parent_id(ti)       # ns/<dag>.<task>/<task run id>
      OPENLINEAGE_ROOT_PARENT_ID = macros.lineage_root_parent_id(ti)  # ns/<dag>/<dag run id>
 3. _run_dbt: subprocess.run(..., env={**os.environ, ...})
 4. bash: `dbt run`  ->  /usr/local/dbt-shim/bin/dbt  ->  dbt-ol (env inherited)
 5. dbt-ol reads the two vars -> parent facet on the dbt run = Airflow task, root = DAG run
```

dbt-ol 1.54.0 reads only `OPENLINEAGE_CONTEXT` (JSON), `OPENLINEAGE_PARENT_ID` and `OPENLINEAGE_ROOT_PARENT_ID`. It does **not** read `OPENLINEAGE_PARENT_JOB_NAMESPACE`, `OPENLINEAGE_PARENT_JOB_NAME` or `OPENLINEAGE_PARENT_RUN_ID`.

The env vars dbt-ol needs, set on the workers:

```
OPENLINEAGE_NAMESPACE=demo
# transport: either OPENLINEAGE_URL + OPENLINEAGE_API_KEY, or
OPENLINEAGE__TRANSPORT__TYPE=http
OPENLINEAGE__TRANSPORT__URL=<datadog openlineage intake url>
OPENLINEAGE__TRANSPORT__AUTH__TYPE=api_key
OPENLINEAGE__TRANSPORT__AUTH__API_KEY=<secret>
```

dbt-ol **ignores `AIRFLOW__OPENLINEAGE__TRANSPORT`**. If no transport is set, it prints events into the task log and sends nothing. In this sandbox both dbt-ol and the Airflow listener use a file transport that writes to `include/ol_events/{dbt,airflow}/` (see `.env.example`).

Caveats:

- With remote execution or a separate worker cluster, the image, `airflow_local_settings.py` and the env vars must be on the machines that run tasks.
- Enabling the policy changes each DAG's serialized form, so every DAG gets a one-time new DAG version.

## Run and verify

You need the Astro CLI, a running Docker daemon (Docker Desktop or Colima) and `python3`.

```bash
cp .env.example .env
astro dev start --no-browser
scripts/trigger_and_wait.sh dbt_test
python3 scripts/verify_lineage.py   # checks dbt run parent = airflow task run id, root = dag run id
astro dev stop
```
