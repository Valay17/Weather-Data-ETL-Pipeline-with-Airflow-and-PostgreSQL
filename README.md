<div align="center">

<h1> <p>Weather ETL Pipeline — Airflow + PostgreSQL</p> </h1>
</div>


An end-to-end ETL pipeline that pulls current weather readings from the [Open-Meteo API](https://open-meteo.com/) on a daily schedule and stores them in a PostgreSQL database. Built with Apache Airflow (Astro Runtime) and containerized with Docker Compose.


## Table of Contents

- [Architecture](#architecture)
- [Features](#features)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Usage](#usage)
- [Configuration](#configuration)
- [License](#license)


## Architecture

```
Open-Meteo API
      |
      v
 [Extract Task]  — HttpHook fetches /v1/forecast JSON
      |
      v
[Transform Task] — flattens nested response into a flat record
      |
      v
  [Load Task]    — inserts record into PostgreSQL weather_data table
```

The pipeline runs as a single Airflow DAG (`weather_etl_pipeline`) on a `@daily` schedule. Each stage is an isolated TaskFlow task, keeping concerns separated and making individual stages easy to test or retry.


## Features

- Scheduled daily extraction of weather data (temperature, wind speed, wind direction, weather code) for any latitude/longitude pair.
- Transformation logic that normalizes the raw JSON response into a flat, storage-ready structure.
- Automatic table creation on first run — no manual database setup required.
- Airflow Connections for both the API and the database, keeping credentials out of source code.
- Fully containerized: Airflow runs via the Astro CLI; PostgreSQL is defined in Docker Compose.


## Project Structure

```
.
├── dags/
│   ├── etl_data.py        # Main weather ETL DAG (Extract → Transform → Load)
│   └── exampledag.py      # Astronomer example DAG (dynamic task mapping demo)
├── docker-compose.yml     # PostgreSQL service definition
├── airflow_settings.yaml  # Local Airflow Connections, Pools, and Variables
├── requirements.txt       # Python dependencies added on top of Astro Runtime
├── packages.txt           # OS-level packages added to the Astro Runtime image
├── .gitignore
└── README.md
```


## Prerequisites

- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) installed
- Docker Desktop (or Docker Engine + Docker Compose) running


## Setup

1. Clone the repository:

    ```bash
    git clone <repo-url>
    cd <repo-directory>
    ```

2. Start the local Airflow environment:

    ```bash
    astro dev start
    ```

    This spins up five containers:

    | Container     | Role                                  |
    |---------------|---------------------------------------|
    | Postgres      | Airflow metadata database             |
    | Scheduler     | Monitors DAGs and triggers task runs  |
    | DAG Processor | Parses and validates DAG files        |
    | API Server    | Serves the Airflow UI and REST API    |
    | Triggerer     | Handles deferred / async tasks        |

3. Open the Airflow UI at [http://localhost:8080](http://localhost:8080) (default credentials: `admin` / `admin`).


## Usage

| Command | Description |
|---|---|
| `astro dev start` | Start all Airflow services |
| `astro dev stop` | Stop all services (preserves data) |
| `astro dev restart` | Restart after making code changes |
| `astro dev logs` | Stream logs from all containers |

To change the target location, update `LATITUDE` and `LONGITUDE` at the top of [dags/etl_data.py](dags/etl_data.py).

## Configuration

Airflow Connections are pre-configured in `airflow_settings.yaml` for local development:

| Connection ID    | Type     | Purpose                        |
|------------------|----------|--------------------------------|
| `open_meteo_api` | HTTP     | Open-Meteo forecast endpoint   |
| `postgres`       | Postgres | Weather data storage database  |

For production deployments, configure these connections through the Airflow UI or environment variables — do not rely on `airflow_settings.yaml`, which is for local development only.


## License

Free to use and modify for personal or internal purposes. Redistribution or public distribution of modified versions requires explicit permission from the author.