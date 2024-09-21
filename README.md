# Spotify End-To-End Data Engineering Project

### Introduction
This project demonstrates an end-to-end data pipeline that extracts data from the Spotify API, processes it with AWS Glue and Apache Spark, stores it in Snowflake, and visualizes it in Power BI. The entire workflow is orchestrated using Apache Airflow running on Docker.

### Architecture Overview
![Architecture Diagram](https://github.com/Rajeshwar1399/spotify-end-to-end-data-engineering-project/blob/main/Architecture.jpg)

### Data Source
- **Spotify API**: The pipeline starts by extracting data from the Spotify API using AWS Lambda.

### Services Used
1. **AWS Lambda Purpose**: Handles real-time data ingestion from the Spotify API. Trigger: Automatically triggered at regular intervals to fetch streaming data. Output: Stores the raw data in an S3 bucket.
2. **S3 Purpose**: Serves as the storage layer for raw and processed data. Raw Data Storage: Ingested data from Spotify is stored here. Processed Data Storage: Transformed data is saved here before loading into Snowflake.
3. **AWS Glue**: The data is transformed using AWS Glue, leveraging Apache Spark for distributed data processing. Transformed data is stored back in Amazon S3.
4. **Snowflake**: The transformed data is ingested into Snowflake via Snowpipe for further analysis.
5. **Power BI**: Snowflake data is visualized in Power BI, enabling interactive and real-time dashboards.
6. **Apache Airflow**: The data pipeline is orchestrated using Apache Airflow, deployed on Docker. Airflow schedules and manages tasks to automate the entire ETL process.

### Future Enhancements
Integration with other music platforms for a broader dataset. Implementation of machine learning models for recommendation systems. Real-time analytics using AWS Kinesis or Kafka.
