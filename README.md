# MqttFlinkPipeline

**MqttFlinkPipeline** is a comprehensive Java project for real-time data streaming and processing using Apache Flink and MQTT. It works together with a REST API and a separate frontend repository to provide end-to-end sensor data visualization and alert management.

This backend repository includes the Flink pipeline and REST API, while the frontend repository provides the user interface for monitoring and managing the sensor data.

---

## Table of Contents

1. [Features](#features)
2. [Technologies](#technologies)
3. [Setup](#setup)
4. [Project Structure](#project-structure)
5. [Usage](#usage)
6. [Data Flow](#data-flow)
7. [Configuration](#configuration)
8. [API](#API)
9. [Frontend Integration](#frontend-integration)
10. [License](#license)

---

## Features

* Real-time MQTT data ingestion and processing
* JSON data parsing and threshold monitoring
* Sensor activity tracking
* Calculation of 30-second average temperatures in Fahrenheit and publishing to output MQTT topic
* REST API exposing `sensor_alerts` and `sensor_activity` tables
* Frontend integration for visualizing and managing sensor data, including deleting alerts

---

## Technologies

* [Java 17+](https://www.oracle.com/java/technologies/javase-jdk17-downloads.html)
* [Apache Flink 1.17+](https://flink.apache.org/downloads.html)
* [Maven](https://maven.apache.org/)
* IDE: [IntelliJ IDEA](https://www.jetbrains.com/idea/) or [Eclipse](https://www.eclipse.org/)
* MQTT Broker: [Mosquitto](https://mosquitto.org/download/)
* JSON library: Jackson
* REST API: FastAPI (Python)
* Frontend: Separate repository (see [Frontend Repository](https://github.com/yourusername/frontend-repo))

---

## Setup

### 1. Install Java

```bash
java -version
```

Java 17 or higher is recommended.

### 2. Install Maven

```bash
mvn -version
```

### 3. Set up Apache Flink

1. Download Flink from [here](https://flink.apache.org/downloads.html).
2. Extract it to a folder and set `$FLINK_HOME`.
3. Start the Flink cluster:

```bash
bin/start-cluster.sh    # Linux/Mac
bin\start-cluster.bat  # Windows
```

### 4. Set up MQTT Broker

Install Mosquitto or any MQTT broker:

```bash
mosquitto
```

### 5. Clone Project & Install Dependencies

```bash
git clone https://github.com/aliefecakir/FlinkMqttDataProcessing.git
cd MqttFlinkPipeline
mvn clean install
```

### 6. Frontend Repository

The frontend repository is maintained separately and should be cloned and set up according to its own README. It consumes the REST API provided by this backend to display sensor data and alerts.

---

## Project Structure

```
MqttFlinkPipeline/
├── src/
│   ├── main/
│   │   ├── API/
│   │   │   └── PostgRestAPI.py     # REST API code (Python/FastAPI)
│   │   └── java/
│   │       └── MqttFlinkProject/
│   │           ├── Connector/      # MQTT Source and Sink
│   │           └── Flink/          # Flink stream processing
│   └── main/
│       └── resources/
│           └── config.properties   # Broker URL, topics, credentials
├── pom.xml                         # Maven dependencies
└── README.md
```

---

## Usage

### 1. Sending Data

Use **MQTT Explorer** or the [JsonDataPublisher](https://github.com/aliefecakir/JsonDataPublisher) Python script from my repositories to publish sensor data to the input topic.

### 2. Configure Properties

Edit `src/main/resources/config.properties`:

```properties
mqtt.broker=tcp://localhost:1883
mqtt.source.topic=sensor/input/data
mqtt.sink.topic=sensor/output/data
mqtt.source.clientId=flink-client-source
mqtt.sink.clientId=flink-client-sink

db.rest.url=postgresql://{username}:{password}@localhost:5432/{database-name}
db.jdbc.url=jdbc:postgresql://localhost:5432/{database-name}
db.user={username}
db.password={password}

ntfy.url=your-ntfy-url
```

### 3. Run Flink Job

```bash
mvn compile exec:java -Dexec.mainClass="MqttFlinkProject.Flink.Mamvn compile exec:java -Dexec.mainClass="MqttFlinkProject.Flink.Main"in"
```

### 4. REST API

Start the FastAPI backend to expose `sensor_alerts` and `sensor_activity` tables.

```bash
cd API
uvicorn PostgRestAPI:app --reload
```

### 5. Frontend

Clone and set up the frontend repository, which consumes the REST API to display sensor tables and manage alerts.

---

## Data Flow

1. Sensor data is published via MQTT Explorer or `JsonDataPublisher`.
2. Flink pipeline ingests data, checks thresholds, verifies sensor activity, and calculates the 30-second average temperature (in Fahrenheit).
3. Processed data is published to the output MQTT topic.
4. REST API retrieves data from `sensor_alerts` and `sensor_activity` tables.
5. Frontend displays data and allows deleting alerts.

---

## Configuration

All connection parameters, MQTT topics, and credentials are set in `config.properties`. REST API configuration is managed within the `API` folder.

---

## API

The backend API is built with **FastAPI** and provides endpoints for managing sensor data.  
It connects to a PostgreSQL database and exposes REST endpoints for **alerts** and **sensor activity**.

---

### 📂 Endpoints

#### 1. Sensor Alerts
- **GET** `/sensor_alerts/`
  - Returns a list of alerts.
  - Query parameters:
    - `skip` (int, optional) → default `0`
    - `limit` (int, optional) → default `20`
    - `sensor_id` (string, optional) → filter by sensor
- **DELETE** `/sensor_alerts/{alert_id}`
  - Deletes an alert by its ID.

#### 2. Sensor Activity
- **GET** `/sensor_activity/`
  - Returns the activity logs of sensors.
  - For each sensor, only the **latest activity timestamp** is used in the frontend.

---

### ⚙️ Running the API

1. Create and activate a virtual environment:

```bash
python -m venv venv
source venv/bin/activate    # Linux / Mac
venv\Scripts\activate       # Windows
````

2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

3. Configure the database URL in `resources/config.properties`:

```
db.rest.url=postgresql://username:password@localhost:5432/sensor_output_db
```

4. Start the FastAPI server:

```bash
uvicorn PostgRestAPI:app --reload --port 8000
```

5. Once the server is running, you can access the interactive API documentation:

* Swagger UI: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)


## Frontend Integration

This backend is designed to work with a separate frontend repository for visualization. Refer to the [Frontend Repository](https://github.com/aliefecakir/frontend-repo) for setup and usage.

---

## License

MIT License © Ali Efe Çakır
