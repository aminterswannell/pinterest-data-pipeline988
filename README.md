# Pinterest Data Pipeline
## Contents:
- [Project Description](#project-description)
- [Installation Instructions](#installation-instructions)
- [Usage Instructions](#usage-instructions)
- [File Structure of the Project](#file-structure-of-the-project)
- [License Information](#license-information)
  
## Project Description
Pinterest crunches billions of data points every day to decide how to provide more value to their users. In this project, you'll create a similar system using the AWS Cloud.

### Project Architecture Overview

## Installation instructions
1. Clone the repository.
2. Set up required tools for this project.

### Tools required for this project:
- Apache Kafka
- AWS MSK
- AWS MSK Connect
- Kafka REST Proxy
- AWS API Gateway
- Databricks
- Apache Spark
- MWAA (Managed Workflow for Apache Airflow)
- AWS Kinesis
- PySpark

## Usage instructions
- Create a .pem key file locally and connect to your EC2 instance.
- Set up Kafka and create relevant topics on your EC2 instance.
- Create a custom plug-in and connector with MSK Connect.
- Build a Kafka REST proxy integration method for the API and set up the Kafka REST proxy on the EC2 instance.
- Modify user_emulation_posting.py file to send data to Kafka topics. (pinterest_data_pipeline/user_posting_emulation.py)
- Mount the s3 bucket containing the generated data to a Databricks notebook and clean the dataframes. (pinterest_data_pipeline/databricks_tasks/mount_s3_load_data.py)
- Run queries on the dataframes. (pinterest_data_pipeline/databricks_tasks/querying_data.py)
- Create a DAG that triggers a Databricks notebook daily and upload it to MWAA environment. (pinterest_data_pipeline/129fb6ae3c55_dag.py)
- Configure an API with Kinsesis proxy integration.
- Send data to the Kinesis streams. (pinterest_data_pipeline/user_posting_emulation_streaming.py)
- Read and transform the data from Kinesis streams in a Databricks notebook, before writing the data to Delta tables.
  (pinterest_data_pipeline/databricks_tasks/streaming-data-kinesis.py)

## File structure of the project
- pinterest_data_pipeline: Folder containing all project files except README and License Information.
  - .gitignore
  - 129fb6ae3c55_dag.py: DAG uploaded to MWAA environment to run Databricks notebook daily.
  - user_posting_emulation.py: File used to generate pin, geo and user data and send to Kafka topics.
  - user_posting_emulation_streaming.py: File used to generate pin, geo and user streaming data and send to Kinesis.
  - databricks_tasks: Folder containing tasks completed in databricks.
    - mount_s3_load_data.ipynb: Notebook containing code run to mount an s3 bucket, load, clean and query the dataframes.
    - cleaning_data.ipynb: Notebook containing just the code run to clean the loaded dataframes from mount_s3_load_data.ipynb notebook.
    - query_data.ipynb: Notebook containing just the code run to query the clean dataframes from mount_s3_load_data.ipynb notebook.
    - streaming_data_kinesis: Notebook containing code to load and clean streamed data from Kinsesis and turn into Delta tables.

## License information
An MIT License was used for this project.

