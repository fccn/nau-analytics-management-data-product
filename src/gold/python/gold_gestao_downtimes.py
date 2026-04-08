from pyspark.sql import DataFrame #type:ignore
import pyspark.sql.functions as F #type:ignore
from pyspark.sql.functions import col
from nau_analytics_data_product_utils_lib import start_iceberg_session,get_required_env #type: ignore
from utils.gold_utils_functions import update_ctrl_table,get_max_timestamp_for_table
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

def with_time_interval(df, start_col, end_col, output_col="interval_seconds"):
    """
    Adds a column with the difference between two timestamp columns in seconds.

    :param df: Spark DataFrame
    :param start_col: name of the start timestamp column
    :param end_col: name of the end timestamp column
    :param output_col: name of the output column (default: interval_seconds)
    :return: DataFrame with new column
    """
    return df.withColumn(
        output_col,
        col(end_col).cast("long") - col(start_col).cast("long")
    )

def main():
    ENVIRONMENT = get_required_env("ENVIRONMENT")

    spark = start_iceberg_session("gold_gestao_downtimes")

    #Variables
    src_layer = f"silver{ENVIRONMENT}"
    tgt_layer = f"gold{ENVIRONMENT}"
    pipeline = "gestao"
    tgt_table_name = "downtimes"

    current_timestamp = spark.sql("SELECT current_timestamp() as c").first()["c"]

    last_execution_timestamp = get_max_timestamp_for_table(spark_session=spark,table_name=tgt_table_name,env=ENVIRONMENT)

    logging.info(f"Starting process from {last_execution_timestamp}")

    #Initial creation of the table (only useful for first run)
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {tgt_layer}.{pipeline}.{tgt_table_name} (
        day_key DATE NOT NULL,
        from_lisbon_time TIMESTAMP NOT NULL,
        to_lisbon_time TIMESTAMP NOT NULL,
        is_financial_manager BOOLEAN NOT NULL,
        is_lms_affected BOOLEAN,
        is_mkt_site_affected BOOLEAN,
        duration_seconds BIGINT NOT NULL,
        ingestion_date TIMESTAMP NOT NULL
    )
    USING ICEBERG
    """)

    #Load source dataframe
    src_table = spark.sql(f"SELECT * FROM {src_layer}.{pipeline}.{tgt_table_name}")

    df_src_data = src_table.select(
        "day_key",
        "from_lisbon_time",
        "to_lisbon_time",
        "is_financial_manager",
        "is_lms_affected",
        "is_mkt_site_affected",
        "duration_seconds",
        "ingestion_date"
    )

    new_or_update_records = df_src_data.count()
    logging.info(f"Number of new or updated records = {new_or_update_records}")

    #Write data to target
    df_src_data.write.format("iceberg").mode("overwrite").saveAsTable(f"{tgt_layer}.{pipeline}.{tgt_table_name}")

    #Finally, we update the control table with the number of records that were inserted or updated in this run.
    update_ctrl_table(spark_session=spark,table_name=tgt_table_name,current_timestamp=current_timestamp,number_of_records=new_or_update_records,env=ENVIRONMENT)

if __name__ == "__main__":
    main()
