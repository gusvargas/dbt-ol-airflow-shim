FROM astrocrpublic.azurecr.io/runtime:3.2-2

# copy the shim into the container and update the PATH
# so that the dbt-shim is discovered first by the shell
COPY --chmod=755 dbt-shim.sh /usr/local/dbt-shim/bin/dbt
ENV PATH="/usr/local/dbt-shim/bin:${PATH}"
