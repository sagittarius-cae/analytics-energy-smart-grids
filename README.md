# energy_smart_grids_analytics

Local analytics stack: Python 3, polars, DuckDB, matplotlib, seaborn, JupyterLab.

Two data layers, deliberately kept simple:

- **bronze** — raw CSVs, one folder per business domain, untouched.
- **gold** — a single `gold/analytics.duckdb` file holding real, materialized
  tables. No intermediate cleaned-CSV or parquet layer: each domain loader reads
  its bronze CSVs, cleans them in-memory with polars, and writes the result
  straight into a gold table.

## Structure

```
data/bronze/
  power_plants/          <- drop source CSVs here, per domain
  utility_provider/
  consumer/
  smart_meters/
  distribution_network/

gold/
  analytics.duckdb        <- created automatically on first run

src/
  common/
    config.py              <- shared paths, read from .env
    duckdb_conn.py           <- one function: get_gold_connection()
  domains/
    smart_meters/load.py     <- fully implemented, the reference example
    power_plants/load.py      <- same pattern, _clean() left as TODO
    utility_provider/load.py
    consumer/load.py
    distribution_network/load.py
  run_all.py                    <- runs every domain's load() in sequence

notebooks/
  01_explore.ipynb                <- queries gold/analytics.duckdb directly
```

Each domain's `load.py` is self-contained: read bronze CSVs -> `_clean()` ->
`CREATE OR REPLACE TABLE <domain> AS SELECT * FROM <cleaned polars df>`. Rerunning
it is safe — it replaces the table, so updated CSVs just mean rerunning the loader.

## Run

```bash
docker compose up --build
```

JupyterLab is available at `http://localhost:8888` (no token — local dev only,
do not expose this port publicly as-is).

On Linux, if files written by the container end up owned by a UID you can't
edit without `sudo`, rebuild with your own UID/GID:

```bash
docker compose build --build-arg UID=$(id -u) --build-arg GID=$(id -g)
docker compose up
```

## Pipeline

1. Drop CSVs into the matching `data/bronze/<domain>/` folder.
2. From a terminal inside the container (JupyterLab has one built in):
   ```bash
   python -m src.run_all
   ```
   Or run one domain at a time: `python -m src.domains.smart_meters.load`
3. Open `notebooks/01_explore.ipynb` and query the gold tables directly —
   `con.sql("SELECT * FROM smart_meters")`.

Domains with no CSVs dropped yet are skipped automatically (`run_all.py` catches
the missing-file case per domain rather than failing the whole run).

## Notes

- Only `smart_meters/load.py` has real cleaning logic (`_clean()`) filled in, as
  the reference implementation. The other four domains follow the identical
  pattern with a `# TODO` in `_clean()` — fill each in once the real CSV
  schemas are known.
- `gold/analytics.duckdb` doesn't need to be pre-created — `duckdb.connect(path)`
  creates it on first connection, same as SQLite.
- The whole project directory is bind-mounted (`.:/workspace`) rather than
  individual subfolders — simpler, and avoids Docker's single-file-mount
  footgun (bind-mounting one file that doesn't yet exist on the host creates a
  directory in its place instead).
- Postgres for a live Looker Studio connector is intentionally left out — see
  the commented block in `docker-compose.yml` if/when that's needed.