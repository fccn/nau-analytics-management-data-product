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

def validate_ingestion_values(spark_session:SparkSession,src_table_df: DataFrame,tgt_layer:str ,tgt_pipeline:str, tgt_table_name:str, env:str) -> int:
        src_count = src_table_df.count()
        tgt_count = spark_session.sql(F"SELECT  count(DISTINCT `key`)  as count FROM {tgt_layer}.{tgt_pipeline}.{tgt_table_name}").first()["count"]
        if src_count != tgt_count:
            raise Exception(
                f"Count mismatch! Source = {src_count}, Target = {tgt_count}. Aborting pipeline."
            )
        return tgt_count
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

def get_google_sheet_as_pd_df(google_sheet_id: str,sheets_service: Any, renamed_columns:Dict[str,str],sheet_name: str) -> pd.DataFrame:
    RANGE = f"{sheet_name}!A:Z"
    result = sheets_service.spreadsheets().values().get(
    spreadsheetId=google_sheet_id,
    range=RANGE
    ).execute()
    values = result.get("values", [])
    df = pd.DataFrame(values[1:], columns=values[0])
    df = df.rename(columns=renamed_columns) #type: ignore
    return df

def add_ingestion_metadata_column(df: DataFrame,table: str,current_timestamp:str) -> DataFrame:
    tmp_df = df.withColumn("ingestion_date", F.lit(current_timestamp)).withColumn("source_name", F.lit(table))
    return tmp_df
def update_ctrl_table(spark_session: SparkSession, table_name:str,current_timestamp: str ,number_of_records:int,env:str) -> bool:
    try:
        spark_session.sql(f"""
            INSERT INTO  bronze{env}.audit.pipeline_run_ctrl
            VALUES('gestao', '{table_name}', '{current_timestamp}', {number_of_records})
        """)
        return True 
    except Exception:
        return False
def cast_to_target_schema(df,table):
    if table=="NSC":
        return (
        df
        .withColumn("key", F.col("key").cast(StringType()))
        .withColumn("summary", F.col("summary").cast(StringType()))
        .withColumn("assignee", F.col("assignee").cast(StringType()))
        .withColumn("reporter", F.col("reporter").cast(StringType()))
        .withColumn("priority", F.col("priority").cast(StringType()))
        .withColumn("status", F.col("status").cast(StringType()))
        .withColumn("team", F.col("team").cast(StringType()))
        .withColumn("updated", F.to_timestamp(F.col("updated"), "M/d/yyyy H:mm:ss"))
        .withColumn("created", F.to_timestamp(F.col("created"), "M/d/yyyy H:mm:ss"))
        .withColumn("status_category", F.col("status_category").cast(StringType()))
        .withColumn("satisfaction", F.col("satisfaction").cast(IntegerType()))
        .withColumn("time_to_first_response", F.col("time_to_first_response").cast(StringType()))
        .withColumn("time_to_resolution", F.col("time_to_resolution").cast(StringType()))
        )
    if table =="NSN":
        return (
        df
        .withColumn("issue_type", F.col("issue_type").cast(StringType()))
        .withColumn("key", F.col("key").cast(StringType()))
        .withColumn("summary", F.col("summary").cast(StringType()))
        .withColumn("assignee", F.col("assignee").cast(StringType()))
        .withColumn("reporter", F.col("reporter").cast(StringType()))
        .withColumn("priority", F.col("priority").cast(StringType()))
        .withColumn("status", F.col("status").cast(StringType()))
        .withColumn("updated", F.to_timestamp(F.col("updated"), "M/d/yyyy H:mm:ss"))
        .withColumn("created", F.to_timestamp(F.col("created"), "M/d/yyyy H:mm:ss"))
        .withColumn("course", F.col("course").cast(StringType()))
        .withColumn("time_to_first_response", F.col("time_to_first_response").cast(StringType()))
        .withColumn("time_to_resolution", F.col("time_to_resolution").cast(StringType()))
        .withColumn("topics", F.col("topics").cast(StringType()))
        .withColumn("organizations", F.col("organizations").cast(StringType()))
        .withColumn("resolution", F.to_timestamp(F.col("resolution"), "M/d/yyyy H:mm:ss"))
        .withColumn("resolved", F.to_timestamp(F.col("resolved"), "M/d/yyyy H:mm:ss"))
        .withColumn("satisfaction", F.col("satisfaction").cast(IntegerType()))
        .withColumn("request_participants", F.col("request_participants").cast(StringType()))
        .withColumn("chart_date_of_first_response", F.to_timestamp(F.col("chart_date_of_first_response"), "M/d/yyyy H:mm:ss")
    ))
    return (
     df.withColumn("issue_type", F.col("issue_type").cast("string"))
    .withColumn("key", F.col("key").cast("string"))
    .withColumn("summary", F.col("summary").cast("string"))
    .withColumn("assignee", F.col("assignee").cast("string"))
    .withColumn("reporter", F.col("reporter").cast("string"))
    .withColumn("priority", F.col("priority").cast("string"))
    .withColumn("status", F.col("status").cast("string"))
    .withColumn("updated", F.to_timestamp(F.col("updated"), "M/d/yyyy H:mm:ss"))
    .withColumn("course", F.col("course").cast("string"))
    .withColumn("created", F.to_timestamp(F.col("created"), "M/d/yyyy H:mm:ss"))
    .withColumn("time_to_first_response", F.col("time_to_first_response").cast("string"))
    .withColumn("time_to_resolution", F.col("time_to_resolution").cast("string"))
    .withColumn("topics", F.col("topics").cast("string"))
    .withColumn("resolution", F.col("resolution").cast("string"))
    .withColumn("resolved", F.to_timestamp(F.col("resolved"), "M/d/yyyy H:mm:ss"))
    .withColumn("satisfaction", F.col("satisfaction").cast("int"))
    .withColumn("chart_date_of_first_response", F.to_timestamp(F.col("chart_date_of_first_response"), "M/d/yyyy H:mm:ss")))


def main():
    GOOGLE_ACCOUNT_JSON = get_required_env("GOOGLE_ACCOUNT_JSON")
    GOOGLE_SHEET_ID = get_required_env("GOOGLE_SHEET_ID")
    ENVIRONMENT = get_required_env("ENVIRONMENT")
    
    GOOGLE_ACCOUNT_JSON = base64.b64decode(GOOGLE_ACCOUNT_JSON).decode()
    GOOGLE_SHEET_ID = base64.b64decode(GOOGLE_SHEET_ID).decode()
    service_sheet = get_google_service_sheet(GOOGLE_ACCOUNT_JSON)
    spark = start_iceberg_session("jira_tickets_ingestion")
    spark.sql(F"""
        CREATE TABLE IF NOT EXISTS bronze{ENVIRONMENT}.gestao.NSC (
        key STRING NOT NULL,
        summary STRING,
        assignee STRING,
        reporter STRING,
        priority STRING,
        status STRING,
        team STRING,
        created TIMESTAMP NOT NULL,
        updated TIMESTAMP NOT NULL,
        status_category STRING,
        satisfaction INT,
        time_to_first_response STRING,
        time_to_resolution STRING,
        ingestion_date TIMESTAMP NOT NULL,
        source_name STRING NOT NULL
    )
    USING ICEBERG;
    """)
    spark.sql(F"""
        CREATE TABLE IF NOT EXISTS bronze{ENVIRONMENT}.gestao.NSN (
            issue_type STRING,
            key STRING NOT NULL,
            summary STRING,
            assignee STRING,
            reporter STRING,
            priority STRING,
            status STRING,
            updated TIMESTAMP NOT NULL,
            course STRING,
            created TIMESTAMP NOT NULL,
            time_to_first_response STRING,
            time_to_resolution STRING,
            topics STRING,
            organizations STRING,
            resolution STRING,
            resolved TIMESTAMP,
            satisfaction INT,
            request_participants STRING,
            chart_date_of_first_response TIMESTAMP,
            ingestion_date TIMESTAMP NOT NULL,
            source_name STRING NOT NULL

        )
        USING ICEBERG;
        """)
    spark.sql(F"""
        CREATE TABLE IF NOT EXISTS bronze{ENVIRONMENT}.gestao.NSU (
            issue_type STRING,
            key STRING NOT NULL,
            summary STRING,
            assignee STRING,
            reporter STRING,
            priority STRING,
            status STRING,
            updated TIMESTAMP NOT NULL,
            course STRING,
            created TIMESTAMP NOT NULL,
            time_to_first_response STRING,
            time_to_resolution STRING,
            topics STRING,
            resolution STRING,
            resolved TIMESTAMP,
            satisfaction INT,
            chart_date_of_first_response TIMESTAMP,
            ingestion_date TIMESTAMP NOT NULL,
            source_name STRING NOT NULL

        )
        USING ICEBERG;
        """)
    renamed_columns = {
        "Key": "key",
        "Summary": "summary",
        "Assignee": "assignee",
        "Reporter": "reporter",
        "Priority": "priority",
        "Status": "status",
        "Team": "team",
        "Created": "created",
        "Updated": "updated",
        "Status Category": "status_category",
        "Satisfaction": "satisfaction",
        "Time to first response": "time_to_first_response",
        "Time to resolution": "time_to_resolution",
        "Issue Type": "issue_type",
        "Course": "course",
        "Topics": "topics",
        "Organizations": "organizations",
        "Resolution": "resolution",
        "Resolved": "resolved",
        "Request participants": "request_participants",
        "[CHART] Date of First Response": "chart_date_of_first_response",
        "before_dash": "before_dash"
    }
    tables = ["NSC","NSN","NSU"]
    for t in tables:
        current_timestamp = spark.sql("SELECT current_timestamp() as c").first()["c"]
        pd_df = get_google_sheet_as_pd_df(google_sheet_id=GOOGLE_SHEET_ID,sheets_service=service_sheet,renamed_columns=renamed_columns,sheet_name=t)
        df = spark.createDataFrame(pd_df)
        df = cast_to_target_schema(df,t)
        tgt_layer = f"bronze{ENVIRONMENT}"
        tgt_pipeline = "gestao"
        tgt_table_name = t
        df.write.format("iceberg").mode("overwrite").saveAsTable(f"{tgt_layer}.{tgt_pipeline}.{tgt_table_name}")
        nr =  validate_ingestion_values(spark_session=spark,src_table_df=df,tgt_layer=tgt_layer,tgt_pipeline=tgt_pipeline,tgt_table_name=tgt_table_name,env=ENVIRONMENT)
        result = update_ctrl_table(spark_session=spark,table_name=tgt_table_name,current_timestamp = current_timestamp,env=ENVIRONMENT,number_of_records=nr)
        if not result:
            raise Exception("ERROR UPDATING METADATA TABLE")
if __name__ == "__main__":
    main()