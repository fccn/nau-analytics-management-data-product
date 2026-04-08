from pyspark.sql import DataFrame,SparkSession #type:ignore
import pyspark.sql.functions as F #type:ignore
from pyspark.sql.types import TimestampType,StringType,IntegerType,BooleanType #type: ignore
from nau_analytics_data_product_utils_lib import start_iceberg_session,get_required_env #type: ignore
from google.oauth2 import service_account #type: ignore
from googleapiclient.discovery import build #type: ignore
import base64
from typing import Any, Dict
import json
import pandas as pd #type: ignore
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

def get_google_service_sheet(service_account_json:str) -> Any:
    SCOPES = [
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/spreadsheets.readonly"
    ]
    SERVICE_ACCOUNT_INFO = json.loads(service_account_json)
  
    credentials = service_account.Credentials.from_service_account_info(
        SERVICE_ACCOUNT_INFO,
        scopes=SCOPES
    )
    sheets_service = build("sheets", "v4", credentials=credentials)
    return sheets_service

def get_google_sheet_as_pd_df(google_sheet_id: str,sheets_service: Any, renamed_columns:Dict[str,str]) -> pd.DataFrame:
    RANGE = "A:Z"
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=google_sheet_id,
        range=RANGE
    ).execute()
    values = result.get("values", [])
    df = pd.DataFrame(values[1:], columns=values[0])
    df = df[df['From (Lisbon time)'] != ""]
    df = df.rename(columns=renamed_columns) #type: ignore
    return df 

def create_spark_df_from_pandas_df(spark: SparkSession , pd_df: pd.DataFrame) -> DataFrame:
    spark_df = spark.createDataFrame(pd_df)
    spark_df = spark_df \
        .withColumn("from_lisbon_time", F.col("from_lisbon_time").cast(TimestampType())) \
        .withColumn("to_lisbon_time", F.col("to_lisbon_time").cast(TimestampType())) \
        .withColumn("impact", F.col("impact").cast(StringType())) \
        .withColumn("duration", F.col("duration").cast(StringType())) \
        .withColumn("duration_minutes", F.col("duration_minutes").cast(IntegerType())) \
        .withColumn("expected", F.col("expected").cast(BooleanType())) \
        .withColumn("detected_by_nagios", F.col("detected_by_nagios").cast(BooleanType())) \
        .withColumn("detected_by_icinga", F.col("detected_by_icinga").cast(BooleanType())) \
        .withColumn("detected_by_uptimerobot", F.col("detected_by_uptimerobot").cast(BooleanType())) \
        .withColumn("description", F.col("description").cast(StringType())) \
        .withColumn("affected_applications", F.col("affected_applications").cast(StringType())) \
        .withColumn("lms_nau_edu_pt_and_studio_nau_edu_pt", F.col("lms_nau_edu_pt_and_studio_nau_edu_pt").cast(BooleanType())) \
        .withColumn("www_nau_edu_pt", F.col("www_nau_edu_pt").cast(BooleanType())) \
        .withColumn("only_some_sub_service_affected", F.col("only_some_sub_service_affected").cast(BooleanType()))
    return spark_df

def get_max_timestamp_for_table(spark_session: SparkSession, table_name:str,env:str) -> str:
    try:
        START_DATE = spark_session.sql(f"SELECT NVL(max(last_execution_ts),'1900-01-01 00:00:00') as ts FROM bronze{env}.audit.pipeline_run_ctrl WHERE table_name = '{table_name}' and pipeline = 'gestao'").first()["ts"]
    except Exception:
        raise ValueError("last execution ts not found on table")
    return START_DATE

def update_ctrl_table(spark_session: SparkSession, table_name:str,current_timestamp: str ,number_of_records:int,env:str) -> bool:
    try:
        spark_session.sql(f"""
            INSERT INTO bronze{env}.audit.pipeline_run_ctrl
            VALUES('gestao', '{table_name}', '{current_timestamp}', {number_of_records})
            """)
        return True 
    except Exception:
        return False

def add_ingestion_metadata_column(df: DataFrame,table: str,current_timestamp:str) -> DataFrame:
    tmp_df = df.withColumn("ingestion_date", F.lit(current_timestamp)).withColumn("source_name", F.lit(table))
    return tmp_df

def validate_ingestion_values(spark_session:SparkSession,src_table_df: DataFrame,table_name:str,env:str) -> int:
    src_count = src_table_df.count()
    tgt_count = spark_session.sql(f"SELECT count(*) as count FROM bronze{env}.gestao.{table_name}").first()["count"]
    if src_count != tgt_count:
        raise Exception(
            f"Count mismatch! Source = {src_count}, Target = {tgt_count}. Aborting pipeline."
        )
    return tgt_count

def main():
    #Define variables
    GOOGLE_ACCOUNT_JSON = get_required_env("GOOGLE_ACCOUNT_JSON")
    GOOGLE_SHEET_ID = get_required_env("DOWNTIMES_GOOGLE_SHEET_ID")
    ENVIRONMENT = get_required_env("ENVIRONMENT")

    GOOGLE_ACCOUNT_JSON = base64.b64decode(GOOGLE_ACCOUNT_JSON).decode()
    GOOGLE_SHEET_ID = base64.b64decode(GOOGLE_SHEET_ID).decode()
    service_sheet = get_google_service_sheet(GOOGLE_ACCOUNT_JSON)

    renamed_columns = {'From (Lisbon time)':"from_lisbon_time", 
                       'To (Lisbon time)':"to_lisbon_time", 
                       'Impact':"impact", 
                       'Duration':"duration",
                       'Duration in minutes':"duration_minutes", 
                       'Expected':"expected", 
                       'Detected by Nagios':"detected_by_nagios",
                       'Detected by Icinga':"detected_by_icinga", 
                       'Detected by UptimeRobot':"detected_by_uptimerobot", 
                       'Description':"description",
                       'Affected Applications':"affected_applications", 
                       'lms.nau.edu.pt & studio.nau.edu.pt':"lms_nau_edu_pt_and_studio_nau_edu_pt",
                       'www.nau.edu.pt':"www_nau_edu_pt", 
                       'Only some sub-service(s) affected':"only_some_sub_service_affected"}

    tgt_layer = f"bronze{ENVIRONMENT}"
    tgt_pipeline = "gestao"
    tgt_table_name = "downtimes"

    #start processing
    spark = start_iceberg_session("Downtimes_ingestion")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS bronze{ENVIRONMENT}.gestao.downtimes (
            from_lisbon_time TIMESTAMP,
            to_lisbon_time TIMESTAMP,
            impact STRING,
            duration STRING,
            duration_minutes INT,
            expected BOOLEAN,
            detected_by_nagios BOOLEAN,
            detected_by_icinga BOOLEAN,
            detected_by_uptimerobot BOOLEAN,
            description STRING,
            affected_applications STRING,
            lms_nau_edu_pt_and_studio_nau_edu_pt BOOLEAN,
            www_nau_edu_pt BOOLEAN,
            only_some_sub_service_affected BOOLEAN,
            ingestion_date TIMESTAMP NOT NULL,
            source_name STRING NOT NULL
        ) USING ICEBERG;
    """)

    pd_df = get_google_sheet_as_pd_df(google_sheet_id=GOOGLE_SHEET_ID,sheets_service=service_sheet,renamed_columns=renamed_columns)
    df = create_spark_df_from_pandas_df(spark=spark,pd_df=pd_df)

    current_timestamp = spark.sql("SELECT current_timestamp() as c").first()["c"]
    start_date = get_max_timestamp_for_table(spark, tgt_table_name, ENVIRONMENT)
    unfiltered_df = df
    df = df.filter(f"from_lisbon_time >= '{start_date}'")
    df = add_ingestion_metadata_column(df, tgt_table_name, current_timestamp)
    df.write.format("iceberg").mode("append").saveAsTable(f"{tgt_layer}.{tgt_pipeline}.{tgt_table_name}")

    nr = validate_ingestion_values(spark_session=spark,src_table_df=unfiltered_df,table_name=tgt_table_name,env=ENVIRONMENT)

    result = update_ctrl_table(spark_session=spark,table_name=tgt_table_name,current_timestamp=current_timestamp,env=ENVIRONMENT,number_of_records=nr)
    if not result:
        raise Exception("ERROR UPDATING METADATA TABLE")

if __name__ == "__main__":
    main()
