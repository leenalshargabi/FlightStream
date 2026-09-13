# FlightStream

Real-time flight-data streaming and analytics pipeline built with OpenSky, Apache Kafka, Apache Spark (Scala), Spark ML, PostgreSQL, and Power BI.

## Project goal

FlightStream collects live aircraft state data from the OpenSky Network, streams the records through Kafka, processes and cleans them with Spark Structured Streaming, applies Spark ML, stores the processed results in PostgreSQL, and exposes the results for Power BI dashboards.

## Architecture

```text
OpenSky Network
      |
      |
Python API / ingestion layer
      |
      |
Kafka topic: flight_states
      |
      |
Apache Spark (Scala)
      |
      |--- Cleaning & validation
      |--- Transformation / feature engineering
      |--- Spark ML
      |
      |
PostgreSQL
      |
      |
Power BI dashboard
```

## Technologies

- Python
- OpenSky Network REST API
- Apache Kafka
- Apache Spark / Scala
- Spark ML
- PostgreSQL
- Power BI
- Docker / Docker Compose

## Current status

- [x] OpenSky account and API client configured
- [x] Secure local environment variables
- [x] OAuth2 authentication test
- [x] Live aircraft data retrieval test
- [x] Final OpenSky API client
- [x] Kafka producer
- [x] Kafka topic and streaming integration
- [x] Spark Structured Streaming consumer
- [x] Spark cleaning and transformations
- [x] Spark ML
- [x] PostgreSQL storage
- [x] Power BI dashboard

## Setup

### 1. Clone the repository

```bash
git clone <repository-url>
cd FlightStream
```

### 2. Create the environment file

Copy `.env.example` to `.env` and add the OpenSky API client credentials.

```text
OPENSKY_CLIENT_ID=your_client_id
OPENSKY_CLIENT_SECRET=your_client_secret
```

Never commit `.env` or expose the client secret.

### 3. Install Python dependencies

```bash
python -m pip install -r requirements.txt
```

### 4. Test the OpenSky API layer

From the project root:

```bash
python api/api.py
```

The command should authenticate with OpenSky, retrieve the current aircraft state snapshot, and print the number of normalized aircraft records plus one sample record.

## OpenSky notes

FlightStream uses the authenticated OpenSky OAuth2 client-credentials flow. OpenSky access tokens expire after 30 minutes; the API client refreshes them automatically before expiry.

The global `/states/all` endpoint is credit-limited, so the streaming layer intentionally polls at a controlled interval rather than making requests continuously.

## Security

The repository should contain `.env.example`, never the real `.env` file. API credentials, passwords, tokens, and other secrets must not be committed to Git.

## License / data attribution

OpenSky data is used according to the OpenSky Network's terms and attribution requirements. See the official OpenSky documentation for current usage and licensing information.
