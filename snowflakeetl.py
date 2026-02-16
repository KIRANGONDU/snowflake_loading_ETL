import pandas as pd
import numpy as np
import os
from datetime import datetime
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


# ENVIRONMENT

load_dotenv()
pd.set_option('display.max_columns', None)

execution_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
current_processing_date = pd.Timestamp.today()

# SOURCE FILES

customer_csv_df = pd.read_csv("company1.csv")
customer_excel_df = pd.read_excel("company2.xlsx")

customer_csv_df.columns = customer_csv_df.columns.str.strip().str.upper()
customer_excel_df.columns = customer_excel_df.columns.str.strip().str.upper()


# RAW LAYER


raw_customer_df = pd.concat(
    [customer_csv_df, customer_excel_df],
    ignore_index=True
)

### Vectorized Gender Normalization 

gender_map = {
    'male': 'M',
    'm': 'M',
    'female': 'F',
    'f': 'F'
}

raw_customer_df['GENDER'] = (
    raw_customer_df['GENDER']
    .astype(str)
    .str.strip()
    .str.lower()
    .map(gender_map)
    .fillna('O')
)

# -------- DOB Conversion --------

raw_customer_df['DOB'] = pd.to_datetime(
    raw_customer_df['DOB'],
    errors='coerce'
)

# -------- Age Calculation (Vectorized) --------

raw_customer_df['AGE'] = (
    current_processing_date.year - raw_customer_df['DOB'].dt.year
)

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


### FINAL LAYER PROCESSING



merged_customer_df = pd.merge(
    customer_csv_df,
    customer_excel_df,
    on="USER_ID",
    how="inner",
    suffixes=('_SRC_CSV', '_SRC_XLSX')
)

# -------- Safe Gender Resolution --------

gender_csv = 'GENDER_SRC_CSV' if 'GENDER_SRC_CSV' in merged_customer_df.columns else None
gender_xlsx = 'GENDER_SRC_XLSX' if 'GENDER_SRC_XLSX' in merged_customer_df.columns else None

if gender_csv and gender_xlsx:
    merged_customer_df['GENDER'] = merged_customer_df[gender_csv].combine_first(
        merged_customer_df[gender_xlsx]
    )
elif gender_csv:
    merged_customer_df['GENDER'] = merged_customer_df[gender_csv]
elif gender_xlsx:
    merged_customer_df['GENDER'] = merged_customer_df[gender_xlsx]
elif 'GENDER' in merged_customer_df.columns:
    merged_customer_df['GENDER'] = merged_customer_df['GENDER']
else:
    merged_customer_df['GENDER'] = None

merged_customer_df['GENDER'] = (
    merged_customer_df['GENDER']
    .astype(str)
    .str.strip()
    .str.lower()
    .map(gender_map)
    .fillna('O')
)

# -------- Safe DOB Resolution --------

dob_csv = 'DOB_SRC_CSV' if 'DOB_SRC_CSV' in merged_customer_df.columns else None
dob_xlsx = 'DOB_SRC_XLSX' if 'DOB_SRC_XLSX' in merged_customer_df.columns else None

if dob_csv and dob_xlsx:
    merged_customer_df['DOB'] = merged_customer_df[dob_csv].combine_first(
        merged_customer_df[dob_xlsx]
    )
elif dob_csv:
    merged_customer_df['DOB'] = merged_customer_df[dob_csv]
elif dob_xlsx:
    merged_customer_df['DOB'] = merged_customer_df[dob_xlsx]
elif 'DOB' in merged_customer_df.columns:
    merged_customer_df['DOB'] = merged_customer_df['DOB']
else:
    merged_customer_df['DOB'] = None

merged_customer_df['DOB'] = pd.to_datetime(
    merged_customer_df['DOB'],
    errors='coerce'
)

# -------- Age Calculation --------

merged_customer_df['AGE'] = (
    current_processing_date.year - merged_customer_df['DOB'].dt.year
)

merged_customer_df['AGE'] -= (
    (current_processing_date.month < merged_customer_df['DOB'].dt.month) |
    (
        (current_processing_date.month == merged_customer_df['DOB'].dt.month) &
        (current_processing_date.day < merged_customer_df['DOB'].dt.day)
    )
)

# -------- Eligible Customers (>18) --------

eligible_customer_df = merged_customer_df[
    merged_customer_df['AGE'] > 18
].copy()

# -------- Safe Name Resolution --------

name_csv = 'NAME_SRC_CSV' if 'NAME_SRC_CSV' in eligible_customer_df.columns else None
name_xlsx = 'NAME_SRC_XLSX' if 'NAME_SRC_XLSX' in eligible_customer_df.columns else None

if name_csv and name_xlsx:
    eligible_customer_df['NAME'] = eligible_customer_df[name_csv].combine_first(
        eligible_customer_df[name_xlsx]
    )
elif name_csv:
    eligible_customer_df['NAME'] = eligible_customer_df[name_csv]
elif name_xlsx:
    eligible_customer_df['NAME'] = eligible_customer_df[name_xlsx]
elif 'NAME' in eligible_customer_df.columns:
    eligible_customer_df['NAME'] = eligible_customer_df['NAME']
else:
    raise ValueError("NAME column not found in any source")

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

# -------- Create Raw Table --------

snowflake_cursor.execute("""
CREATE OR REPLACE TABLE RAW_LAYER_DT (
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

# -------- Create Final Table --------

snowflake_cursor.execute("""
CREATE OR REPLACE TABLE FNL_LAYER_DT (
    USER_ID NUMBER,
    NAME STRING,
    EMAIL STRING,
    GENDER STRING,
    DOB STRING,
    AGE NUMBER,
    LOAD_TIMESTAMP TIMESTAMP
)
""")

# -------- Load Raw Layer --------

_, _, raw_row_count, _ = write_pandas(
    snowflake_conn,
    raw_customer_df,
    "RAW_LAYER_DT"
)

print(f"RAW_LAYER_DT loaded successfully: {raw_row_count} rows")

# -------- Load Final Layer --------

_, _, final_row_count, _ = write_pandas(
    snowflake_conn,
    final_customer_df,
    "FNL_LAYER_DT"
)

print(f"FNL_LAYER_DT loaded successfully: {final_row_count} rows")

snowflake_cursor.close()
snowflake_conn.close()
