# Data Modeling with Airflow Data Pipeline Project

This project showcases an Airflow-based data pipeline designed for loading and processing data from a table into a Redshift data warehouse. The pipeline consists of two DAGs: one for the initial load and another for incremental load.

## Project Overview
===================

The project involves the following components and technologies:

- Airflow for workflow orchestration (using astro)
- Kaggle dataset for initial load
- Redshift as the data warehouse
- Amazon S3 as the staging area
- dbt (data build tool) for data transformation
- Soda for data quality monitoring
- Faker library for incremental load data generation

The data modeling follows a typical star schema approach, with facts and dimensions.

## DAGs

### Initial Load DAG

The Initial Load DAG is responsible for the initial data load from a Kaggle dataset into Redshift. It comprises the following tasks:

1. **Add Column Task**: A PythonOperator task (`add_column_task`) to add a record date column to the dataset.
2. **Local to S3 Task**: A LocalFilesystemToS3Operator task (`create_local_to_s3_job`) to upload the dataset to an S3 bucket.
3. **Create Schema Task Group**: A TaskGroup (`create_schema_task_group`) containing two RedshiftSQLOperator tasks to create schemas for staging and public dimensions.
4. **Create Staging Table Task**: A RedshiftSQLOperator task (`create_staging_table`) to create a staging table in Redshift.
5. **S3 to Redshift Stage Task**: An S3ToRedshiftOperator task (`s3_to_redshift_stage`) to copy data from S3 to the Redshift staging table.
6. **Transform Data Task Group**: A DbtTaskGroup (`transform_data`) to execute dbt transformations on the staged data.
7. **Delete Staging Table Task**: A RedshiftSQLOperator task (`delete_staging_table`) to clean up the staging table after data transformation.


