import pandas as pd
import os
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ENVIRONMENT SETUP

load_dotenv()
pd.set_option('display.max_columns', None)


# SOURCE DATA INGESTION


company1_df = pd.read_csv("company1.csv")
company2_df = pd.read_excel("company2.xlsx")

company1_df.columns = company1_df.columns.str.strip().str.upper()
company2_df.columns = company2_df.columns.str.strip().str.upper()


# RAW LAYER TRANSFORMATIONS


raw_user_df = pd.concat(
    [company1_df, company2_df],
    ignore_index=True
)

def standardize_gender_value(gender_value):
    if pd.isna(gender_value):
        return 'O'
    gender_value = str(gender_value).strip().lower()
    if gender_value in ['male', 'm']:
        return 'M'
    elif gender_value in ['female', 'f']:
        return 'F'
    return 'O'

raw_user_df['GENDER'] = raw_user_df['GENDER'].apply(standardize_gender_value)

raw_user_df['DOB'] = pd.to_datetime(raw_user_df['DOB'], errors='coerce')

processing_date = pd.Timestamp.today()

raw_user_df['AGE'] = processing_date.year - raw_user_df['DOB'].dt.year
raw_user_df['AGE'] -= (
    (processing_date.month < raw_user_df['DOB'].dt.month) |
    (
        (processing_date.month == raw_user_df['DOB'].dt.month) &
        (processing_date.day < raw_user_df['DOB'].dt.day)
    )
)

raw_user_df['DOB'] = raw_user_df['DOB'].dt.strftime('%d-%m-%Y')

raw_user_df['LOAD_TIMESTAMP'] = pd.Timestamp.utcnow().tz_localize(None)


# FINAL LAYER TRANSFORMATIONS


joined_user_df = pd.merge(
    company1_df,
    company2_df,
    on="USER_ID",
    how="inner",
    suffixes=('_SRC1', '_SRC2')
)

joined_user_df['GENDER'] = joined_user_df['GENDER_SRC1'].combine_first(
    joined_user_df['GENDER_SRC2']
)
joined_user_df['GENDER'] = joined_user_df['GENDER'].apply(standardize_gender_value)

joined_user_df['DOB'] = joined_user_df['DOB_SRC1'].combine_first(
    joined_user_df['DOB_SRC2']
)
joined_user_df['DOB'] = pd.to_datetime(joined_user_df['DOB'], errors='coerce')

joined_user_df['AGE'] = processing_date.year - joined_user_df['DOB'].dt.year
joined_user_df['AGE'] -= (
    (processing_date.month < joined_user_df['DOB'].dt.month) |
    (
        (processing_date.month == joined_user_df['DOB'].dt.month) &
        (processing_date.day < joined_user_df['DOB'].dt.day)
    )
)

eligible_user_df = joined_user_df[joined_user_df['AGE'] > 18].copy()

if 'NAME_SRC1' in eligible_user_df.columns:
    eligible_user_df['NAME'] = eligible_user_df['NAME_SRC1']
elif 'NAME_SRC2' in eligible_user_df.columns:
    eligible_user_df['NAME'] = eligible_user_df['NAME_SRC2']

eligible_user_df['DOB'] = eligible_user_df['DOB'].dt.strftime('%d-%m-%Y')
eligible_user_df['LOAD_TIMESTAMP'] = pd.Timestamp.now()

final_user_df = eligible_user_df[
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


# SNOWFLAKE DATA LOAD


snowflake_connection = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

snowflake_cursor = snowflake_connection.cursor()

snowflake_cursor.execute(f"USE DATABASE {os.getenv('SNOWFLAKE_DATABASE')}")
snowflake_cursor.execute(f"USE SCHEMA {os.getenv('SNOWFLAKE_SCHEMA')}")
snowflake_cursor.execute(f"USE WAREHOUSE {os.getenv('SNOWFLAKE_WAREHOUSE')}")

write_pandas(
    snowflake_connection,
    raw_user_df,
    "RAW_USER",
    use_logical_type=True
)

write_pandas(
    snowflake_connection,
    final_user_df,
    "FINAL_USER",
    use_logical_type=True
)

print("Snowflake Load Completed Successfully")

snowflake_cursor.close()
snowflake_connection.close()