# CAP benchmark

Same four scenarios for PostgreSQL, MongoDB, Cassandra, and Redis. Each database has its own adapter. Scenarios do not know which engine they are talking to.

| Scenario | What it does |
|---|---|
| S1 | Partition one node. A majority client and a minority client run at the same time, then the partition is removed. |
| S2 | Kill the primary (or the Cassandra coordinator) and wait until the cluster serves writes again. |
| S3 | Add delay between nodes and record latency. |
| S4 | Partition, write on both sides, reconnect, and measure how long the copies take to match. |

`fast` shortens every phase and runs each scenario once. `full` is the measurement matrix: S1 × 3, S2 × 10, S3 × 3, S4 × 3.

## Requirements

- Windows 11
- Docker Desktop, WSL2 engine, about 8 GB of RAM for the WSL virtual machine
- Python 3.11 or newer

Start Docker Desktop before any command below. Leave the terminal open for the whole run. A full matrix takes hours.

## Install

```powershell
cd cap-benchmark
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

After activation, run the tool with `python`.

## Check the machine

```powershell
python run.py verify
```

This starts a short-lived container and checks that `tc` can add delay. It should print `verify: PASS`.

## Run one database

Pick one database and finish it before starting another. The first start builds a local image, so pass `--build`.

```powershell
python run.py up --db postgres --build --purge-volumes
python run.py smoke --db postgres
```

`up` stays on `Starting postgres cluster...` until the nodes are healthy. `smoke` writes one key and reads it back. It should print `smoke: PASS`.

Then run the short matrix:

```powershell
python run.py matrix --db postgres --profile fast
```

`matrix` stops the cluster, deletes its volumes, and starts it again before every scenario. You do not need to call `up` again. When the fast matrix looks right, run the full one:

```powershell
python run.py matrix --db postgres --profile full
```

Repeat the same three commands with `--db mongo`, `--db cassandra`, or `--db redis`. Cassandra and Redis take longer to become healthy on the first start.

## What the matrix runs

Every `matrix` call needs `--db`.

| Database | Extra passes inside one matrix |
|---|---|
| postgres | One consistency setting. |
| mongo | One consistency setting (majority). |
| cassandra | Each scenario twice: consistency `one`, then `quorum`. |
| redis | S1 and S3 twice: async replication, then `WAIT 1`. S2 and S4 are async only. |

A single scenario, without the whole matrix:

```powershell
python run.py scenario --db postgres --scenario s1 --profile fast --runs 1
python run.py scenario --db cassandra --scenario s1 --profile fast --consistency quorum --runs 1
python run.py scenario --db redis --scenario s1 --profile fast --consistency wait --runs 1
```

## If a run stops

Rows with status `aborted` stay in `results/runs.csv` and are not counted. `--resume` continues until each cell has enough rows with status `ok` for that profile.

```powershell
python run.py rescue --db postgres
python run.py matrix --db postgres --profile full --resume
```

`rescue` clears network faults and starts stopped containers. Use it after Ctrl+C. Do not open `results/runs.csv` in Excel while a run is writing to it.

## Results

| File | Contents |
|---|---|
| `results/runs.csv` | One row per run: database, scenario, profile, status. |
| `results/phase_summary.csv` | Success rate and latency per phase. |
| `results/scenario_metrics.csv` | Scenario-level metrics. |
| `results/errors_breakdown.csv` | Error categories. |
| `results/operations_<run_id>.csv` | Every operation from that run. |
| `logs/` | Log of each run. |

Build the summary tables from the `full` rows:

```powershell
python run.py aggregate
```

Tables go to `results/aggregated/`. Add `--include-fast` if the short runs should be included too.

## Stop

```powershell
python run.py down --db postgres --purge-volumes
```

`--purge-volumes` deletes that database's data. Omit it to keep the volumes.

## Layout

| Path | Role |
|---|---|
| `config.yaml` | Phase lengths, workload, and both profiles. |
| `docker/compose.<db>.yml` | Nodes, networks, and ports. |
| `init/<db>/` | Schema and cluster setup. |
| `capbench/databases/` | One client adapter per database. |
| `capbench/scenarios/` | S1–S4. |
| `run.py` | `verify`, `up`, `down`, `smoke`, `scenario`, `matrix`, `aggregate`, `rescue`. |
