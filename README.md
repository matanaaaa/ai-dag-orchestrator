# AI DAG Orchestrator

A distributed DAG orchestration system built with **Go + Kafka + MySQL**.  
The system converts a user request into a DAG execution plan and orchestrates distributed workers to execute nodes with dependency scheduling.

This project demonstrates the core architecture of a **workflow orchestration platform** similar to Airflow / Argo but simplified for learning and experimentation.

---

## Architecture

Pipeline flow:

submit → plan → dispatch → execute → status → persist → query

Components:

- **API Service**
  - Accepts user requests
  - Publishes `dag.job.submit` event
  - Provides `/jobs/{id}` query API

- **Planner**
  - Consumes `dag.job.submit`
  - Generates DAG plan
  - Publishes `dag.plan.result`

- **Orchestrator**
  - Persists DAG into MySQL
  - Calculates node indegree
  - Dispatches runnable nodes to workers
  - Unlocks downstream nodes when dependencies complete

- **Worker**
  - Executes node operators
  - Reports execution status

Communication is handled via **Kafka event topics**.

---

## Tech Stack

- **Go**
- **Kafka**
- **MySQL**
- **Docker Compose**

---

## DAG Example

User request:

make hello world pipeline

Generated DAG:

download -> transform_upper -> upload

Execution result:

hello world -> HELLO WORLD -> artifact file

---

## Quick Start

### 1 Start infrastructure

```bash
docker-compose up -d
```

This starts:

- Kafka
- MySQL
- Zookeeper

### 2 Run services

Open four terminals.

#### Terminal A — Orchestrator

```powershell
$env:MYSQL_DSN="root:root@tcp(localhost:3307)/dag?parseTime=true&multiStatements=true"
$env:KAFKA_BROKERS="localhost:9092"
go run ./cmd/orchestrator
```

#### Terminal B — Planner

```powershell
$env:MYSQL_DSN="root:root@tcp(localhost:3307)/dag?parseTime=true&multiStatements=true"
$env:KAFKA_BROKERS="localhost:9092"
go run ./cmd/planner
```

#### Terminal C — Worker

```powershell
$env:MYSQL_DSN="root:root@tcp(localhost:3307)/dag?parseTime=true&multiStatements=true"
$env:KAFKA_BROKERS="localhost:9092"
$env:DATA_DIR="./data"
go run ./cmd/worker
```

#### Terminal D — API

```powershell
$env:MYSQL_DSN="root:root@tcp(localhost:3307)/dag?parseTime=true&multiStatements=true"
$env:KAFKA_BROKERS="localhost:9092"
$env:HTTP_ADDR=":8080"
go run ./cmd/api
```

### 3 Submit a job

#### POST /submit

PowerShell example:

```powershell
Invoke-RestMethod -Method Post `-Uri "http://localhost:8080/submit"`
-ContentType "application/json" `
-Body '{"user_request":"make hello world pipeline"}'
```

#### Example response:

job_4c4ec031-09d4-4043-b5da-f280ca4a7aaf

### 4 Query job status

curl http://localhost:8080/jobs/<job_id>

Example response:

```bash
{
"job": {
"job_id": "job_4c4ec031-09d4-4043-b5da-f280ca4a7aaf",
"status": "SUCCEEDED"
},
"nodes": [
{
"node_id": "download",
"status": "SUCCEEDED",
"output_text": "hello world"
},
{
"node_id": "upper",
"status": "SUCCEEDED",
"output_text": "HELLO WORLD"
},
{
"node_id": "upload",
"status": "SUCCEEDED",
"artifact_uri": "file://data/out/job_xxx.txt"
}
]
}
```

### 5 Check generated artifact

Get-ChildItem .\data\out\
Get-Content .\data\out\<file>

Expected output:

HELLO WORLD
