# FlightStream

**Real-time flight-data streaming and analytics pipeline** built with OpenSky Network, Apache Kafka, Apache Spark (Scala), Spark ML, PostgreSQL, Power BI, and Docker.

## Overview

FlightStream collects live aircraft data from the OpenSky Network and processes it through an end-to-end Big Data pipeline.

The data flows through Python and Kafka into Spark Structured Streaming, where it is cleaned, transformed, standardized, and analyzed using K-Means clustering. The processed results are stored in PostgreSQL and visualized through Power BI.

## Architecture

```text
OpenSky Network
      |
Python Producer
      |
Kafka: flight_states
      |
Spark Structured Streaming
      |
Cleaning & Validation
      |
Feature Engineering
      |
StandardScaler
      |
K-Means (Spark ML)
      |
PostgreSQL
      |
Power BI
```

## Technologies

| Technology      | Role                                         |
| --------------- | -------------------------------------------- |
| Python          | API ingestion, normalization, Kafka producer |
| OpenSky Network | Live aircraft data source                    |
| Apache Kafka    | Real-time data streaming                     |
| Apache Spark    | Stream processing                            |
| Scala           | Spark application                            |
| Spark MLlib     | Machine learning                             |
| K-Means         | Aircraft-data clustering                     |
| PostgreSQL      | Processed data storage                       |
| Power BI        | Dashboard and visualization                  |
| Docker          | Infrastructure and services                  |

## Machine Learning

FlightStream uses **K-Means clustering** from Spark MLlib.

The model uses four numerical features:

* Latitude
* Longitude
* Altitude
* Velocity

The features are standardized with `StandardScaler` before K-Means training.

The current model uses **K = 4** and assigns each processed aircraft record to one of four clusters: `0`, `1`, `2`, or `3`.

## Project Structure

```text
FlightStream/
|--- api/
|--- producer/
|--- database/
|--- spark/
|--- tests/
|--- docs/
|   |--- PROJECT_NOTES.md
|   |--- screenshots/
|--- powerbi/
|--- .env.example
|--- .gitignore
|--- requirements.txt
|--- README.md
```

## Setup

### 1. Configure environment variables

Create a local `.env` file from `.env.example`:

```text
OPENSKY_CLIENT_ID=your_client_id
OPENSKY_CLIENT_SECRET=your_client_secret
```

Never commit the real `.env` file.

### 2. Install Python dependencies

```bash
python -m pip install -r requirements.txt
```

### 3. Start the infrastructure

Start the Kafka and Spark/PostgreSQL Docker Compose environments.

### 4. Start the producer

From the project root:

```bash
python -m producer.producer
```

The producer retrieves live aircraft states and publishes them to the Kafka topic:

```text
flight_states
```

### 5. Run Spark

The Scala Spark application consumes the Kafka stream, processes the flight records, applies K-Means clustering, and writes the results to PostgreSQL.

## Results

The complete pipeline was successfully verified:

* OpenSky authentication and live data retrieval 
* Python -> Kafka streaming 
* Kafka -> Spark Structured Streaming 
* Data cleaning and transformation 
* StandardScaler + K-Means clustering 
* Spark -> PostgreSQL 
* Power BI visualization 

During final verification, PostgreSQL contained **130,000+ processed flight records**, with K-Means predictions across clusters `0-3`.

## Dashboard

The Power BI dashboard provides:

* Total flight records
* Countries detected
* Average altitude
* Average velocity
* Geographic flight distribution
* Top countries by flight records
* ML cluster distribution
* Altitude vs. velocity by cluster

Screenshots and detailed project documentation are available in [`docs/`](docs/).

## Limitations & Future Improvements

The OpenSky API is subject to usage limits, so the producer uses controlled polling.

K-Means clusters are unsupervised groups and do not represent predefined real-world flight categories.

Future improvements could include more advanced flight features, model evaluation, additional clustering methods, monitoring, database optimization, and cloud deployment.

## Security

The repository contains `.env.example` but never the real `.env`.

API credentials, database passwords, tokens, and other secrets must not be committed to Git.

## Documentation

- [Project Presentation](docs/presentation/FlightStream_Project_Presentation.pptx)
- [Detailed Project Notes](docs/PROJECT_NOTES.md)
- [Project Screenshots](docs/screenshots/)

## Data Attribution

FlightStream uses data from the **OpenSky Network** and follows its applicable terms and attribution requirements.
