
# Python ETL Pipeline – CSV & Excel to Snowflake

This project implements a Python-based ETL pipeline that synchronizes data from CSV and Excel sources, applies data cleansing rules in a Raw Layer, and generates a filtered, joined Final Layer that is loaded into Snowflake using high-performance bulk loading.



## Architecture Overview
 . Source Layer: input data from csv and Excel files
 . Raw Layer: Data cleansing and standardization,age calculation, audit columns
 . Final Layer: Inner join,age filtering, column selection
 . Target: Snowflake tables loaded using write_pandas


## Part 1: Extraction & Raw Layer (Cleansing)

### Source file: The pipeline expects the following input files in the project directory
- source_data.csv
- source_data.xlsx

### Extraction & Raw Layer Processing(cleansing)
Extraction
. CSV files and Excel files are loaded into pandas DataFrames
. Column names are trimmed and converted to uppercase for consitstency
. Both datasets are combined into a single DataFrame
## Data cleansing rules
. Gender standardization(first the headers will be converte to lowercase and spaces will also be deleted using strip())
 . Male,male,m --> M
 . Female,female,f --> F
 . Null or unknown values --> 0
## Data Standardization
. Data Standardization
 . Age is calculated using the current date
 . Handles month/day comparision correctly
## Audit column
. Load_timestamp is added using the current execution timestamp

### Snowflake Table
- RAW_USER_DATA : stores the fully cleansed raw dataset

## Part 2: Final Layer (Join & Business Logic)

## Join Logic
. An inner join is performed between CSV and Excel datasets
. join key:USER_ID
. Pands merge() is ued (no row-level loops)

### Business Rules
. Gender and DOB are selected safely form CSV or Excel columns
. Age id recalculated after join
. Only users older than 18 years are included.
. Required columns are selected explicitly.
## Audit columns
. LOAD_TIMESTAM is added to the final dataset

### Snowflake Table
- FINAL_USER_DATA : Stored is added to the final dataset

## Part 3: Snowflake Loading

### Loading Method
. Uses snowflake-connector-python
. Uses write_pandas for bulk loding(high performance)

## WareHouse Usage
. A Medium Warehouse is used explicitly to:
  . Maintain optimized metdata
  . Avoid 100% partition scan issued

## Table created
. RAW_USER_DATA: python scripts automatically loads this table to snowflake
. FINAL_USER_DATA: python scrips automatically loads this table to snowflake


## Technology Stack(for usage)
- I had used Python 3.8+ version to connect with snowflake smoothly.
- More importantly i had used PandasData cleaning and Standardization 
- snowflake-connector-python
- openpyxl


## Environment Setup

### Install Dependencies
pip install pandas snowflake-connector-python openpyxl

---

## Environment Variables

### Windows (PowerShell)
setx SNOWFLAKE_ACCOUNT "<account_name>"
setx SNOWFLAKE_USER "<username>"
setx SNOWFLAKE_PASSWORD "<password>"
setx SNOWFLAKE_WAREHOUSE "MEDIUM_WH"
setx SNOWFLAKE_DATABASE "<database_name>"
setx SNOWFLAKE_SCHEMA "<schema_name>"


## Execution
python etl_pipeline.py

---

## Output Tables
- RAW_USER_DATA: Cleansed raw dataset
- FINAL_USER_DATA: Joined and filtered dataset (Age > 18)
