# 🚕 NYC Yellow Taxi Analysis

A comprehensive data engineering project analyzing NYC Yellow Taxi trip data from 2022-2025 using Apache Spark, Airflow, MinIO, Trino, and Metabase. This project processes over 159 million taxi trip records to provide insights into location patterns, payment trends, and temporal analysis.

![Project Architecture](docker\sandbox\results\NYC_Yellow_Taxi_analysis_Diagram.png)

## 📊 Project Overview

This project implements a complete data pipeline that:
- Extracts NYC Yellow Taxi trip data from parquet files (2022-2024 full years, 2025 Jan-Oct)
- Standardizes schemas across different years to handle data inconsistencies
- Cleans and transforms ~159M records into ~131M quality records
- Stores processed data in MinIO (S3-compatible storage)
- Enables SQL queries via Trino
- Visualizes insights through interactive Metabase dashboards

### Key Insights Delivered

- **Location Analysis**: Trip distribution across NYC boroughs, top pickup/dropoff zones
- **Vendor & Payment Analysis**: Revenue by vendor, payment method preferences, surcharge breakdowns
- **Time Analysis**: Temporal patterns, peak hours, seasonal trends, trip metrics over time

## 🏗️ Architecture

### Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Data Processing** | Apache Spark (PySpark) | Distributed data transformation and cleaning |
| **Orchestration** | Apache Airflow | Workflow scheduling and task management |
| **Storage** | MinIO | S3-compatible object storage for staged/cleaned data |
| **Query Engine** | Trino | Distributed SQL query engine |
| **Visualization** | Metabase | Business intelligence and dashboards |
| **Development** | Jupyter Notebook | Data exploration and testing |
| **Infrastructure** | Docker Compose | Container orchestration |

### Data Pipeline Flow

```
Source Data (Parquet Files)
    ↓
[Spark Job 1] Schema Standardization & Union
    ↓
MinIO Bucket: raw_data_std_schema
    ↓
[Spark Job 2] Data Cleaning & Transformation
    ↓
MinIO Bucket: cleaned_data
    ↓
Trino (SQL Interface)
    ↓
Metabase Dashboards
```

## 📁 Project Structure

```
NYC_Yellow_Taxi_Analysis/
├── docker/
│   ├── docker-compose.yml          # Multi-container orchestration
│   ├── Dockerfile.airflow          # Airflow custom image
│   ├── Dockerfile.spark            # Spark cluster image
│   └── sandbox/
│       ├── dags/                   # Airflow DAG definitions
│       │   └── nyc_taxi_dag.py
│       ├── spark/
│       │   ├── app/                # PySpark applications
│       │   │   ├── nyc_taxi_read_all_years.py
│       │   │   └── nyc_taxi_clean_data.py
│       │   └── resources/          # Source data and lookup tables
│       │       ├── taxi_zone_lookup.csv
│       │       └── NYC_Yellow_Taxi_Trips/ (gitignored)
│       ├── notebooks/              # Jupyter exploration notebooks
│       └── logs/                   # Airflow logs
├── trino/
│   └── etc/                        # Trino configuration
├── metabase-data/                  # Metabase application data
├── minio_data/                     # MinIO storage (gitignored)
└── README.md
```

## 🚀 Getting Started

### Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- At least 8GB RAM allocated to Docker
- 50GB+ free disk space

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/SherifElshafeyy/NYC_Yellow_Taxi_Analysis.git
cd NYC_Yellow_Taxi_Analysis
```

2. **Download source data**

Download NYC Yellow Taxi trip data from the [NYC Taxi & Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page):
- 2022: January - December
- 2023: January - December
- 2024: January - December
- 2025: January - October

Place parquet files in:
```
docker/sandbox/spark/resources/NYC_Yellow_Taxi_Trips/
├── 2022/
│   ├── yellow_tripdata_2022-01.parquet
│   └── ...
├── 2023/
├── 2024/
└── 2025/
```

3. **Download taxi zone lookup table**

Download `taxi_zone_lookup.csv` from NYC TLC and place in:
```
docker/sandbox/spark/resources/taxi_zone_lookup.csv
```

4. **Start the infrastructure**
```bash
cd docker
docker-compose up -d
```

This will start all services:
- Airflow (Webserver, Scheduler, PostgreSQL)
- Spark (Master, Worker)
- Jupyter Notebook
- MinIO
- Trino
- Metabase

5. **Verify services are running**
```bash
docker-compose ps
```

### Service Access

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8082 | admin / admin |
| Spark Master UI | http://localhost:18080 | - |
| Jupyter Notebook | http://localhost:8888 | No password |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Trino | http://localhost:8085 | - |
| Metabase | http://localhost:3000 | Setup on first access |

## 📊 Running the Pipeline

### Option 1: Via Airflow UI

1. Access Airflow at http://localhost:8082
2. Navigate to DAGs
3. Enable the `nyc_taxi_Cleaning` DAG
4. Trigger the DAG manually
5. Monitor task execution in the Graph or Tree view

### Option 2: Via Command Line

```bash
# Trigger the DAG
docker exec -it airflow-webserver airflow dags trigger nyc_taxi_Cleaning

# Check DAG status
docker exec -it airflow-webserver airflow dags list-runs -d nyc_taxi_Cleaning
```

### Pipeline Tasks

The DAG consists of three sequential tasks:

1. **Standarize_Schema_Union_Years**
   - Reads parquet files for each year and month
   - Handles schema inconsistencies (data types, column names)
   - Enforces standard schema across all years
   - Unions all data into single dataset
   - Writes to MinIO: `s3a://nyc/raw_data_std_schema`
   - Duration: ~15-20 minutes

2. **clean_data**
   - Reads staged data from MinIO
   - Performs data cleaning operations:
     - Renames columns for clarity
     - Calculates trip duration
     - Fills null values appropriately
     - Filters invalid records (bad distances, durations, amounts)
     - Adds derived columns (vendor names, payment methods, distance in km)
     - Enriches with zone information from lookup table
   - Writes to MinIO: `s3a://nyc/cleaned_data`
   - Duration: ~20-25 minutes

3. **create_trino_cleaned_table**
   - Creates Trino table schema
   - Maps to cleaned data in MinIO
   - Enables SQL queries on the data

## 🔍 Data Schema

### Source Data Challenges

The project addresses several schema inconsistencies across years:

| Column | 2022-2023 | 2024-2025 | Solution |
|--------|-----------|-----------|----------|
| VendorID | long | integer | Cast to integer |
| passenger_count | double | long | Cast to long |
| RatecodeID | double | long | Cast to long |
| PULocationID, DOLocationID | long | integer | Cast to integer |
| airport_fee | lowercase | Airport_fee | Rename to Airport_fee |
| cbd_congestion_fee | N/A | Present (2025 only) | Add with null for earlier years |

### Final Cleaned Schema

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| **Vendor_ID** | integer | Taxi vendor identifier (1=Creative Mobile Technologies, 2=Curb Mobility, 6=Myle Technologies, 7=Helix, 99=Unknown) |
| **Vendor_Name** | varchar | Human-readable vendor name |
| **Trip_Pickup_DateTime** | timestamp | Date and time when the meter was engaged |
| **Trip_Dropoff_DateTime** | timestamp | Date and time when the meter was disengaged |
| **passenger_count** | bigint | Number of passengers in the vehicle (1-6) |
| **Pickup_Location_ID** | integer | TLC Taxi Zone ID where trip started |
| **Pickup_Borough** | varchar | NYC borough where trip started (Manhattan, Queens, Brooklyn, Bronx, Staten Island) |
| **Pickup_Zone** | varchar | Specific zone name where trip started |
| **Pickup_Service_Zone** | varchar | Service zone type for pickup (Yellow Zone, Boro Zone, Airports, EWR) |
| **Dropoff_Location_ID** | integer | TLC Taxi Zone ID where trip ended |
| **Dropoff_Borough** | varchar | NYC borough where trip ended |
| **Dropoff_Zone** | varchar | Specific zone name where trip ended |
| **Dropoff_Service_Zone** | varchar | Service zone type for dropoff |
| **Trip_Distance_Km** | double | Trip distance in kilometers (converted from miles) |
| **trip_distance_miles** | double | Trip distance in miles as recorded by taximeter |
| **Ratecode_ID** | bigint | Rate code for the trip (1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group ride, 99=Unknown) |
| **Ratecode_Description** | varchar | Human-readable rate code description |
| **payment_type** | bigint | Payment method code (0=Flex Fare, 1=Credit Card, 2=Cash, 3=No Charge, 4=Dispute, 5=Unknown, 6=Voided) |
| **Payment_Method** | varchar | Human-readable payment method |
| **trip_duration_min** | double | Trip duration in minutes (calculated from pickup/dropoff times) |
| **fare_amount** | double | Base fare calculated by the meter |
| **extra_charges** | double | Miscellaneous extras and surcharges (rush hour, overnight charges) |
| **mta_tax** | double | $0.50 MTA tax triggered by metered rate |
| **tip_amount** | double | Tip amount (credit card tips only, cash tips not included) |
| **tolls_amount** | double | Total tolls paid during trip |
| **improvement_surcharge** | double | $0.30 improvement surcharge |
| **total_amount** | double | Total amount charged to passenger (excluding cash tips) |
| **congestion_surcharge** | double | Congestion surcharge for trips in Manhattan south of 96th Street |
| **Airport_fee** | double | $1.75 fee for pickups at LaGuardia and JFK airports |
| **cbd_congestion_fee** | double | Central Business District congestion fee (introduced 2025) |
| **Year** | integer | Year of trip pickup (2022-2025) |
| **Month** | varchar | Month of trip pickup (3-letter abbreviation) |

## 📈 Data Quality

### Cleaning Results

- **Original Records**: 159,372,196
- **Cleaned Records**: 130,902,515
- **Filtered Records**: 28,469,681 (17.86%)

### Data Quality Rules

Records are filtered based on:
- Passenger count: 1-6
- Trip distance: 0.1-200 miles
- Trip duration: 0.01-120.1 minutes
- Pickup ≠ Dropoff location
- Non-negative amounts for all fees
- Valid location IDs

## 📊 Dashboards

### 1. Time Analysis
![Time Dashboard](docker\sandbox\results\metabase_Dashboard_NYC_Yellow_Taxi_Time_Analysis.png.png)

**Insights:**
- 130.9M total trips generating $3.6B revenue
- Average trip: 5.77 km, 16.82 minutes, $3.47 tip
- Peak hours: 6-8 PM consistently highest
- Monthly patterns show seasonal variations



### 2. Location Analysis
![Location Dashboard](docker\sandbox\results\metabase_Dashboard_NYC_Yellow_Location_Analysis.png.png)

**Insights:**
- Manhattan dominates with ~89% of both pickups and dropoffs
- Top pickup zone: JFK Airport (~7M trips)
- Queens accounts for ~10% of trips, primarily airport-related

### 2. Vendor & Payment Analysis
![Payment Dashboard](docker\sandbox\results\metabase_Dashboard_NYC_Yellow_Taxi_Payment_Analysis.png)

**Insights:**
- Curb Mobility, LLC handles 77% of trips
- Credit cards account for 83% of payments
- Standard rate code dominates revenue
- Surcharges show consistent patterns across years



## 🔧 Configuration

### MinIO Buckets

The pipeline uses two MinIO buckets:
- `nyc/raw_data_std_schema` - Staged data after schema standardization
- `nyc/cleaned_data` - Final cleaned and transformed data

### Spark Configuration

Key Spark settings for MinIO connectivity:
```python
.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
.config("spark.hadoop.fs.s3a.access.key", "minioadmin")
.config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
.config("spark.hadoop.fs.s3a.path.style.access", "true")
.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
```

### Trino Catalog

Trino is configured to read from MinIO using the Hive connector:
```sql
CREATE TABLE IF NOT EXISTS hive.nyc.cleaned_data (...)
WITH (
    external_location = 's3a://nyc/cleaned_data',
    format = 'PARQUET'
)
```

## 🧪 Development & Testing

### Jupyter Notebooks

Use Jupyter for exploration and testing:

1. Access Jupyter at http://localhost:8888
2. Navigate to the `work` directory
3. Create or open notebooks
4. Connect to Spark cluster:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("NYC_Taxi_Analysis") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
```

### Query Data with Trino

Connect to Trino and query the cleaned data:

```sql
-- Sample queries
SELECT COUNT(*) FROM hive.nyc.cleaned_data;

SELECT 
    Pickup_Borough, 
    COUNT(*) as trip_count,
    AVG(trip_distance_miles) as avg_distance
FROM hive.nyc.cleaned_data
GROUP BY Pickup_Borough
ORDER BY trip_count DESC;

SELECT 
    Year, 
    Month,
    SUM(total_amount) as revenue
FROM hive.nyc.cleaned_data
GROUP BY Year, Month
ORDER BY Year, Month;
```



## 📚 Resources

- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Trino Documentation](https://trino.io/docs/current/)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)
- [Metabase Documentation](https://www.metabase.com/docs/latest/)

## 🤝 Contributing

Contributions are welcome! Feel free to:
- Report bugs
- Suggest new features
- Submit pull requests
- Improve documentation



## 👤 Author

**Sherif Elshafey**

[![GitHub](https://img.shields.io/badge/GitHub-SherifElshafeyy-181717?style=for-the-badge&logo=github)](https://github.com/SherifElshafeyy)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Sherif%20Elshafey-0077B5?style=for-the-badge&logo=linkedin)](https://www.linkedin.com/in/sherif-elshafey-763467226)

Feel free to reach out for questions, collaboration, or discussions about data engineering!


---

**⭐ If you find this project helpful, please consider giving it a star!**