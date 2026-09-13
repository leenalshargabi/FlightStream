# FlightStream - Project Notes

## 1. Project Overview

FlightStream is a real-time flight data project. I built it as a complete Big Data pipeline using live aircraft data from the OpenSky Network.

The main flow is:

```text
OpenSky Network
      |
Python API / Producer
      |
Kafka - flight_states
      |
Spark Structured Streaming
      |
Data Cleaning & Processing
      |
Spark ML - K-Means
      |
PostgreSQL
      |
Power BI
```

The goal of the project is to connect different Big Data technologies together and process real flight data from the API until it becomes useful information in a dashboard.

---

## 2. Technologies Used

* **Python** - gets the data from OpenSky, normalizes it, and sends it to Kafka
* **OpenSky Network** - provides live aircraft data
* **Apache Kafka** - receives and stores the streaming flight records
* **Apache Spark** - reads and processes the Kafka stream
* **Scala** - used to write the Spark application
* **Spark MLlib** - used for machine learning
* **K-Means** - groups flight records into clusters
* **PostgreSQL** - stores the processed results
* **Power BI** - displays the final results
* **Docker** - runs the main services and containers

---

# 3. How the Parts Connect

## 3.1 OpenSky Network -> Python

The project starts by getting live aircraft data from the OpenSky Network API.

I use OpenSky OAuth2 authentication with a client ID and secret. The credentials are stored locally in the `.env` file and are not uploaded to GitHub.

The API provides information such as:

* ICAO24 aircraft ID
* Callsign
* Origin country
* Latitude
* Longitude
* Altitude
* Velocity
* True track
* Vertical rate
* Ground/air status
* Time information

Before sending the data to Kafka, I normalize the API records so that the data has a consistent structure.

---

## 3.2 Python -> Kafka

After the data is normalized, the Python producer converts each aircraft record into JSON and sends it to the Kafka topic:

```text
flight_states
```

The producer gets a live snapshot of the aircraft, sends the records to Kafka, waits 120 seconds, and then gets another snapshot.

During one successful run, the producer showed:

```text
Aircraft received: 12409
Aircraft sent to Kafka: 12409
```

This confirmed that the producer was receiving the live data from OpenSky and successfully sending it to Kafka.

---

## 3.3 Kafka -> Spark

Kafka is the middle part of the pipeline.

The flight records are stored in the `flight_states` topic until Spark reads them.

Spark Structured Streaming connects to Kafka and reads the records.

The Kafka values are JSON, so Spark uses a defined schema to convert them into proper columns.

---

# 4. Spark Processing

After Spark receives the flight data, I clean and validate it before using it for further processing.

Some of the checks include:

* `icao24` cannot be null
* latitude must be between `-90` and `90`
* longitude must be between `-180` and `180`
* altitude must exist
* altitude cannot be negative
* velocity must exist
* velocity cannot be negative

I also create an `altitude` column.

If barometric altitude is available, it is used. If it is missing, geographic altitude is used instead.

I also create a `flight_status` field:

```text
GROUND
```

when the aircraft is on the ground, and:

```text
AIRBORNE
```

when it is not.

---

# 5. Machine Learning

Spark ML was required as part of the project, so I added K-Means clustering.

I chose K-Means because there is no predefined target label for the aircraft records. Instead of predicting an existing category, K-Means can find groups in the data based on the flight features.

The four features used for clustering are:

```text
latitude
longitude
altitude
velocity
```

These features have very different scales. For example, altitude can be thousands of meters while latitude is between `-90` and `90`.

Because of this, I use `StandardScaler` before training K-Means.

The K-Means model uses:

```text
K = 4
```

This means that the model creates four clusters.

Each processed flight record receives a cluster number:

```text
0
1
2
3
```

These numbers are cluster IDs created by the model. They are not predefined aircraft types.

---

# 6. Spark -> PostgreSQL

After Spark processes the records and K-Means assigns a cluster to each record, the results are written to PostgreSQL.

The main table is:

```text
flight_clusters
```

The table stores information such as:

* Aircraft ID
* Callsign
* Origin country
* Latitude
* Longitude
* Altitude
* Velocity
* True track
* Vertical rate
* Ground status
* Snapshot time
* Cluster
* Processing time

Spark connects to PostgreSQL using JDBC.

I checked the database directly using `psql` and confirmed that records were actually being inserted.

During verification, the table contained more than **130,000 processed flight records**.

I also checked the newest rows and confirmed that they contained recent processing timestamps and K-Means cluster values.

---

# 7. PostgreSQL -> Power BI

The final step is Power BI.

Power BI connects to the PostgreSQL database and uses the `flight_clusters` table.

The connection uses:

```text
Server: localhost:6432
Database: postgres
Username: postgres
```

The dashboard contains:

* Total Flight Records
* Countries Detected
* Average Altitude
* Average Velocity
* Flight locations on a map
* Top 10 countries by flight records
* ML cluster distribution
* Altitude vs. velocity by ML cluster

The final dashboard is saved in:

```text
powerbi/FlightStream_Dashboard.pbix
```

---

# 8. Project Structure

The main project structure is:

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

### `api/`

Contains the OpenSky API code and the function used to normalize aircraft records.

### `producer/`

Contains the Python Kafka producer.

### `database/`

Contains the PostgreSQL-related setup.

### `spark/`

Contains the Scala Spark application.

The main file is:

```text
spark/src/main/scala/FlightStreaming.scala
```

### `tests/`

Contains tests for the Python API layer.

### `docs/`

Contains the project notes and screenshots.

### `powerbi/`

Contains the final Power BI dashboard.

---

# 9. Problems I Faced and How I Fixed Them

This section documents the main problems I actually faced while building and testing FlightStream.

## 9.1 OpenSky Authentication

At the beginning, I needed to make sure that the OpenSky API was working before connecting it to the rest of the project.

I tested the OAuth2 authentication first.

The authentication request returned:

```text
HTTP 200
```

and gave me a Bearer token.

I then tested the live aircraft endpoint and successfully received thousands of aircraft records.

This confirmed that the API connection was working before I moved on to Kafka.

---

## 9.2 Python Import Error

While testing the Python API code, I got this error:

```text
cannot import name 'state_to_record' from 'api'
```

The problem was related to the way the Python module and function were organized and imported.

I fixed the module structure and the import so that the producer could correctly use `state_to_record`.

After fixing it, I ran the tests again.

The result was:

```text
2 tests passed
```

The tests checked that invalid short records were skipped and that valid aircraft states were correctly normalized.

---

## 9.3 Missing Values in the OpenSky Data

The OpenSky API does not always provide every value for every aircraft.

Some aircraft can have missing altitude, position, or other information.

Because of this, I could not assume that every API record was valid.

I added normalization in the Python layer and additional validation in Spark.

For altitude, Spark uses barometric altitude when it is available and geographic altitude as a backup.

This helps prevent invalid records from reaching the machine learning stage.

---

## 9.4 Kafka Producer Warning

While running the Python producer, I saw a warning related to the Kafka `value_serializer`:

```text
DeprecationWarning
```

The warning looked serious at first, but it did not stop the producer.

The producer still connected to Kafka and successfully sent the aircraft records.

Since it was only a warning and the pipeline was working, I did not change working code just to remove the warning.

---

## 9.5 Kafka Broker Problem After Restarting Docker

One of the biggest problems happened after restarting Docker.

The Kafka containers were not all running correctly.

I checked the containers and found that `kafka2` had exited.

I then checked the `flight_states` topic and saw:

```text
Leader: none
```

Because the topic had no active leader, the producer could not get normal Kafka metadata and became stuck trying to refresh its metadata.

I checked the Kafka2 logs to find the actual reason.

The important error was:

```text
NodeExistsException
```

The logs also mentioned:

```text
/brokers/ids/2
```

This meant that ZooKeeper still had an old registration for Kafka2 from a previous Kafka session.

In simple terms, Kafka2 was trying to start again, but ZooKeeper still had the old broker registration.

---

## 9.6 Fixing the Kafka2 and ZooKeeper Problem

First, I checked that ZooKeeper itself was working.

The ZooKeeper health check returned:

```text
imok
```

I then checked the Kafka2 logs and confirmed that the actual Kafka log data was still there.

So I knew that the problem was not lost Kafka data.

Instead of deleting the Kafka topic or Docker volumes, I safely restarted the Kafka stack using:

```text
docker compose stop
docker compose start
```

After restarting, the Kafka containers came back up.

I checked the topic again and got:

```text
Leader: 2
Replicas: 2
Isr: 2
```

The Topic ID also stayed the same.

This was important because it confirmed that the existing topic was recovered instead of being deleted and recreated.

I avoided commands such as:

```text
docker compose down -v
```

because I did not want to remove the Docker volumes and lose the stored Kafka data.

---

## 9.7 Spark Did Not Have the Normal Scala Tools

When I started working with Scala, I found that the Spark container did not have the normal:

```text
scala
scalac
sbt
```

commands available on the PATH.

Instead of rebuilding the environment, I checked the Spark installation and found the Scala compiler JARs already inside the container.

I found:

```text
scala-compiler-2.11.12.jar
scala-library-2.11.12.jar
scala-reflect-2.11.12.jar
```

I used these JARs directly to compile `FlightStreaming.scala`.

The compilation worked, and I created the Spark JAR:

```text
flightstream.jar
```

This allowed me to run the Scala Spark application using the existing Spark environment.

---

## 9.8 Wrong Spark Submit Package Command

I also had a problem with the `spark-submit` command.

The Kafka package argument had been written with incorrect escaping.

The working command was:

```text
docker exec -e FLIGHTSTREAM_DB_PASSWORD=itversity itvdelab spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 --master local[*] --class FlightStreaming /tmp/flightstream/flightstream.jar
```

After correcting the command, Spark started successfully and was able to use the Kafka connector.

---

## 9.9 Spark Checkpoint Collision

Another important Spark problem happened when more than one `FlightStreaming` process was running.

Both processes were trying to use the same Structured Streaming checkpoint:

```text
/tmp/flightstream/checkpoint
```

This caused:

```text
FileAlreadyExistsException
```

The problem was that two Spark streaming processes were using the same checkpoint at the same time.

I checked the running processes and found the duplicate Spark process.

I stopped the duplicate process and made sure that only one `FlightStreaming` process was running.

I then removed only the affected Spark checkpoint and started one clean Spark process.

After that, the batches were processed normally and PostgreSQL started receiving the records again.

Because of this problem, I learned that only one copy of this streaming application should use the checkpoint at a time.

---

## 9.10 PostgreSQL Connection

After Spark processing and K-Means were working, I connected Spark to PostgreSQL using JDBC.

The PostgreSQL database was running in Docker, so Spark connected to it through the Docker network.

I then checked the database directly using `psql`.

The `flight_clusters` table contained the processed records.

I also checked the latest rows and saw:

* Real aircraft IDs
* Callsigns
* Countries
* Coordinates
* Altitude
* Velocity
* Cluster numbers
* Recent processing timestamps

This confirmed that Spark was actually saving the processed results in PostgreSQL.

---

## 9.11 Power BI Map Problem

While creating the Power BI dashboard, I first looked at the available map visuals.

Azure Maps required a work or school account.

I did not need Azure Maps for this project, so I used the normal Power BI Map visual instead.

I enabled the required map visual settings and used the latitude and longitude fields from PostgreSQL.

The normal Map visual worked for the dashboard.

---

## 9.12 Keeping the API Credentials Safe

The OpenSky client ID and secret are stored locally in:

```text
.env
```

The `.env` file is ignored by Git.

I checked:

```text
git ls-files .env
```

and it returned no output.

This confirmed that `.env` was not being tracked by Git.

The repository contains:

```text
.env.example
```

instead.

This shows the required environment variable names without exposing the real credentials.

---

# 10. Testing and Verification

I tested the project step by step instead of only checking the final dashboard.

### OpenSky

OpenSky authentication worked successfully and live aircraft data was received.

### Python

The API normalization tests passed:

```text
2 tests passed
```

### Kafka

The `flight_states` topic was checked and real JSON aircraft records were visible.

### Spark

Spark successfully:

* Read the Kafka records
* Parsed the JSON
* Cleaned the data
* Created additional fields
* Standardized the ML features
* Trained K-Means
* Generated cluster predictions

The predictions included:

```text
0
1
2
3
```

### PostgreSQL

The `flight_clusters` table was checked directly using PostgreSQL.

More than 130,000 processed records were present during verification.

### Power BI

Power BI successfully connected to PostgreSQL and displayed the processed flight data in the dashboard.

---

# 11. Following One Flight Record Through the Project

One way to understand the whole project is to follow one aircraft record from the beginning to the dashboard.

First, OpenSky provides the aircraft state.

Python receives the data and normalizes it.

The producer converts the record to JSON and sends it to:

```text
flight_states
```

Kafka stores the message.

Spark reads the message and converts the JSON into columns.

Spark checks the values and removes invalid records.

The cleaned record is then used to create the ML feature vector:

```text
latitude
longitude
altitude
velocity
```

`StandardScaler` standardizes these values.

K-Means assigns the record to one of four clusters.

Spark adds the cluster number and processing time.

The processed record is written to:

```text
flight_clusters
```

Finally, Power BI reads the PostgreSQL table and uses the record in the dashboard.

The complete path is:

```text
OpenSky
   |
Python
   |
Kafka
   |
Spark
   |
Cleaning
   |
StandardScaler
   |
K-Means
   |
PostgreSQL
   |
Power BI
```

---

# 12. Final Project Status

The main pipeline is working end to end.

* [x] OpenSky authentication
* [x] Live aircraft data retrieval
* [x] Python normalization
* [x] Python tests
* [x] Kafka producer
* [x] Kafka topic
* [x] Spark Structured Streaming
* [x] Data cleaning
* [x] Feature engineering
* [x] StandardScaler
* [x] K-Means clustering
* [x] PostgreSQL storage
* [x] Power BI dashboard
* [x] Project screenshots
* [x] Project documentation

Final pipeline:

```text
OpenSky Network
      |
Python Producer
      |
Kafka - flight_states
      |
Spark Structured Streaming
      |
Cleaning & Feature Engineering
      |
StandardScaler
      |
K-Means
      |
PostgreSQL
      |
Power BI
```

The main goal of FlightStream was not just to make each technology work separately. I connected them into one complete Big Data pipeline and verified that real flight data could travel through the whole system.
