# Iceberg-File-Integration
This repo demonstrates how to integrate existing files in object storage into Iceberg files as metadata-only operations using the Iceberg Java API.
## Setup
In order to connect to your glue catalog you will need to provide your access key and secret access key, and region  in the Dockerfile.
You will also need to set the location of your warehouse in src/main/java/AppConfig.java. Once these changes are done you can build and run your docker image
```python
docker build -t iceberg-s3 .
docker run -d --name my_iceberg_container iceberg-s3
docker exec -it my_iceberg_container bash
```
Now that you are in, you can either use python or mvn exec to achieve desired functionality

## Python Script for Maven Command Execution

This section describes a Python script run_mvn.py inside scripts designed to facilitate the execution of Maven commands for specific tasks such as creating tables, appending new files, and reading directory schemas within a Java project.

### Overview

The script provides a convenient way to run Maven commands from within Python, targeting a Maven project. It includes functions for:

- Creating a new table in a database.
- Appending new files to an existing table.
- Reading schema information from a directory.

### Key Functions

- `run_create_table(file_path, database_name, table_name)`: Initializes the table schema using a specified Parquet file.
- `run_new_file_appender(file_path, database_name, table_name)`: Appends a new file to an existing table.
- `run_directory_schema_reader(directory_path, database_name, table_name)`: Reads and processes the schema from a directory of files.

### Usage

To use this script, ensure Maven is installed and accessible from your environment. Then, call the desired function with appropriate arguments. For example:

```python
file_path = "s3a://path/to/my_file.parquet"
directory_path = "s3a://path/to/directory/"
database_name = "my_database"
table_name = "my_table"

# Create a new table with schema derived from `my_file.parquet`
run_create_table(file_path, database_name, table_name)

# Append a new file to the existing table
run_new_file_appender(file_path, database_name, table_name)

# Read and process schema from a directory of files
run_directory_schema_reader(directory_path, database_name, table_name)
```
## Running MVN Commands
Alternatively you can just execute the classes with the needed arguments to acheieve the same functionality, for example to create an iceberg table from a parquet in your S3:
```python
mvn exec:java -Dexec.mainClass="CreateTable" -Dexec.args="s3a://PathName/file_name.parquet db_name table_name"
```
