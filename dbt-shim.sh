#!/usr/bin/env bash

# don't re-invoke dbt-ol if it's already been invoked
if [ -n "${DBT_OL_ACTIVE:-}" ]; then
  exec /usr/local/bin/dbt "$@"
fi

# set a flag so that subsequent calls to the shim routes
# to the real dbt
export DBT_OL_ACTIVE=1
exec dbt-ol --consume-structured-logs "$@"
