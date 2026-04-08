from pyspark.sql import DataFrame,Window #type:ignore
import pyspark.sql.functions as F #type:ignore
from typing import List, Union, Optional,Tuple
from pyspark.sql import SparkSession #type: ignore
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

def get_max_timestamp_for_table(spark_session: SparkSession, table_name: str, env: str) -> str:
    try:
        START_DATE = spark_session.sql(
            f"SELECT NVL(max(last_execution_ts),'1900-01-01 00:00:00') as ts "
            f"FROM bronze{env}.audit.pipeline_run_ctrl "
            f"WHERE table_name = '{table_name}' and pipeline = 'gestao'"
        ).first()["ts"]
    except Exception:
        raise ValueError("last execution ts not found on table")
    return START_DATE


def update_ctrl_table(spark_session: SparkSession, table_name: str, current_timestamp: str, number_of_records: int, env: str) -> bool:
    try:
        spark_session.sql(f"""
            INSERT INTO bronze{env}.audit.pipeline_run_ctrl
            VALUES('gestao', '{table_name}', '{current_timestamp}', {number_of_records})
            """)
        return True
    except Exception:
        return False
