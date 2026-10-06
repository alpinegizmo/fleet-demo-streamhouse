# DuckDB CLI with the lakehouse already attached: `docker compose run --rm
# duckdb` opens a SQL shell on the Iceberg tables Flink writes (see
# init.sql). Extensions are installed at build time so the shell starts
# without downloading anything.

FROM duckdb/duckdb:1.5.6

RUN ["/duckdb", "-c", "INSTALL iceberg; INSTALL httpfs; INSTALL avro;"]

COPY init.sql /init.sql

ENTRYPOINT ["/duckdb", "-init", "/init.sql"]
