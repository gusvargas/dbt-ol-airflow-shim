#!/usr/bin/env bash

REAL_DBT=/usr/local/bin/dbt

# don't re-invoke dbt-ol if it's already been invoked
if [ -n "${DBT_OL_ACTIVE:-}" ]; then
  exec "$REAL_DBT" "$@"
fi

# the dbt subcommand is the first argument that names one
subcommand=""
for arg in "$@"; do
  case "$arg" in
    build|clean|clone|compile|debug|deps|docs|init|list|ls|parse|retry|run|run-operation|seed|show|snapshot|source|test)
      subcommand="$arg"
      break
      ;;
  esac
done

# with --consume-structured-logs, dbt-ol only handles these subcommands. for any other
# (run-operation, deps, compile, ...) it never starts dbt and still exits 0, so those go
# straight to the real dbt
case "$subcommand" in
  run|build|test|seed|snapshot|retry) ;;
  *) exec "$REAL_DBT" "$@" ;;
esac

# set a flag so that subsequent calls to the shim routes
# to the real dbt
export DBT_OL_ACTIVE=1
exec dbt-ol --consume-structured-logs "$@"
