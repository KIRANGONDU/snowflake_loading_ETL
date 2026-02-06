import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


# ENVIRONMENT INITIALIZATION


load_dotenv()
pd.set_option('display.max_columns', None)

execution_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# SOURCE DATA INGESTION

customer_csv_df = pd.read_csv("company1.csv")
customer_excel_df = pd.read_excel("company2.xlsx")

customer_csv_df.columns = customer_csv_df.columns.str.strip().str.upper()
customer_excel_df.columns = customer_excel_df.columns.str.strip().str.upper()

# RAW LAYER PROCESSING

raw_customer_df = pd.concat(
    [customer_csv_df, customer_excel_df],
    ignore_index=True
)

def normalize_gender_value(gender_input):
    if pd.isna(gender_input):
        return 'O'
    gender_input = str(gender_input).strip().lower()
    if gender_input in ['male', 'm']:
        return 'M'
    elif gender_input in ['female', 'f']:
        return 'F'
    return 'O'

raw_customer_df['GENDER'] = raw_customer_df['GENDER'].apply(normalize_gender_value)

# Date of Birth conversion
raw_customer_df['DOB'] = pd.to_datetime(raw_customer_df['DOB'], errors='coerce')

current_processing_date = pd.Timestamp.today()

# Age calculation
raw_customer_df['AGE'] = current_processing_date.year - raw_customer_df['DOB'].dt.year
raw_customer_df['AGE'] -= (
    (current_processing_date.month < raw_customer_df['DOB'].dt.month) |
    (
        (current_processing_date.month == raw_customer_df['DOB'].dt.month) &
        (current_processing_date.day < raw_customer_df['DOB'].dt.day)
    )
)

raw_customer_df['DOB'] = raw_customer_df['DOB'].dt.strftime('%d-%m-%Y')
raw_customer_df['LOAD_TIMESTAMP'] = execution_timestamp
raw_customer_df = raw_customer_df.reset_index(drop=True)

# FINAL LAYER PROCESSING

merged_customer_df = pd.merge(
    customer_csv_df,
    customer_excel_df,
    on="USER_ID",
    how="inner",
    suffixes=('_SRC_CSV', '_SRC_XLSX')
)

# Gender resolution
merged_customer_df['GENDER'] = merged_customer_df['GENDER_SRC_CSV'].combine_first(
    merged_customer_df['GENDER_SRC_XLSX']
)
merged_customer_df['GENDER'] = merged_customer_df['GENDER'].apply(normalize_gender_value)

# DOB resolution
merged_customer_df['DOB'] = merged_customer_df['DOB_SRC_CSV'].combine_first(
    merged_customer_df['DOB_SRC_XLSX']
)
merged_customer_df['DOB'] = pd.to_datetime(merged_customer_df['DOB'], errors='coerce')

# Age calculation
merged_customer_df['AGE'] = current_processing_date.year - merged_customer_df['DOB'].dt.year
merged_customer_df['AGE'] -= (
    (current_processing_date.month < merged_customer_df['DOB'].dt.month) |
    (
        (current_processing_date.month == merged_customer_df['DOB'].dt.month) &
        (current_processing_date.day < merged_customer_df['DOB'].dt.day)
    )
)

eligible_customer_df = merged_customer_df[merged_customer_df['AGE'] > 18].copy()

# Name resolution
if 'NAME_SRC_CSV' in eligible_customer_df.columns:
    eligible_customer_df['NAME'] = eligible_customer_df['NAME_SRC_CSV']
elif 'NAME_SRC_XLSX' in eligible_customer_df.columns:
    eligible_customer_df['NAME'] = eligible_customer_df['NAME_SRC_XLSX']

eligible_customer_df['DOB'] = eligible_customer_df['DOB'].dt.strftime('%d-%m-%Y')
eligible_customer_df['LOAD_TIMESTAMP'] = execution_timestamp

final_customer_df = eligible_customer_df[
    [
        'USER_ID',
        'NAME',
        'EMAIL',
        'GENDER',
        'DOB',
        'AGE',
        'LOAD_TIMESTAMP'
    ]
].reset_index(drop=True)


# SNOWFLAKE LOADING


snowflake_conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

snowflake_cursor = snowflake_conn.cursor()
snowflake_cursor.execute(f"USE WAREHOUSE {os.getenv('SNOWFLAKE_WAREHOUSE')}")

# Create RAW table
snowflake_cursor.execute("""
CREATE OR REPLACE TABLE CUSTOMER_USER_DATA (
    USER_ID NUMBER,
    NAME STRING,
    GENDER STRING,
    DOB STRING,
    CITY STRING,
    EMAIL STRING,
    COUNTRY STRING,
    AGE NUMBER,
    LOAD_TIMESTAMP TIMESTAMP
)
""")

# Create FINAL table
snowflake_cursor.execute("""
CREATE OR REPLACE TABLE CUSTOMER_FINAL_DATA (
    USER_ID NUMBER,
    NAME STRING,
    EMAIL STRING,
    GENDER STRING,
    DOB STRING,
    AGE NUMBER,
    LOAD_TIMESTAMP TIMESTAMP
)
""")

# Load RAW layer
_, _, raw_row_count, _ = write_pandas(
    snowflake_conn,
    raw_customer_df,
    "CUSTOMER_USER_DATA"
)
print(f"CUSTOMER_USER_DATA loaded successfully: {raw_row_count} rows")

# Load FINAL layer
_, _, final_row_count, _ = write_pandas(
    snowflake_conn,
    final_customer_df,
    "CUSTOMER_FINAL_DATA"
)
print(f"CUSTOMER_FINAL_DATA loaded successfully: {final_row_count} rows")

snowflake_cursor.close()
snowflake_conn.close()