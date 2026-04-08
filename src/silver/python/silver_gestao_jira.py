from pyspark.sql import DataFrame #type:ignore
import pyspark.sql.functions as F #type:ignore
from pyspark.sql.functions import col #type:ignore 
from nau_analytics_data_product_utils_lib import start_iceberg_session,get_required_env #type: ignore
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

def main():
    ENVIRONMENT = get_required_env("ENVIRONMENT")

    spark = start_iceberg_session("silver_gestao_jira")
    
    
    src_layer = f"bronze{ENVIRONMENT}"
    tgt_layer = f"silver{ENVIRONMENT}"
    pipeline = "gestao"
    spark.sql(
    f"""
     CREATE TABLE IF NOT EXISTS {tgt_layer}.{pipeline}.NSC (
        ticket_type_origin        STRING NOT NULL,
        key                       STRING NOT NULL,
        assignee                  STRING,
        reporter                  STRING,
        status                    STRING,
        created                   TIMESTAMP NOT NULL,
        resolution                STRING,
        resolved                  TIMESTAMP,
        satisfaction              INT,
        time_to_resolve           STRING,   -- stored as string instead of interval
        time_to_resolve_seconds   BIGINT
    )
    USING ICEBERG;   

        """
    )
    spark.sql(
    f"""
     CREATE TABLE IF NOT EXISTS {tgt_layer}.{pipeline}.NSN (
        ticket_type_origin        STRING NOT NULL,
        key                       STRING NOT NULL,
        assignee                  STRING,
        reporter                  STRING,
        status                    STRING,
        created                   TIMESTAMP NOT NULL,
        resolution                STRING,
        resolved                  TIMESTAMP,
        satisfaction              INT,
        time_to_resolve           STRING,   -- stored as string instead of interval
        time_to_resolve_seconds   BIGINT
    )
    USING ICEBERG;   

        """
    )
    spark.sql(
    f"""
     CREATE TABLE IF NOT EXISTS {tgt_layer}.{pipeline}.NSU (
        ticket_type_origin        STRING NOT NULL,
        key                       STRING NOT NULL,
        assignee                  STRING,
        reporter                  STRING,
        status                    STRING,
        created                   TIMESTAMP NOT NULL,
        resolution                STRING,
        resolved                  TIMESTAMP,
        satisfaction              INT,
        time_to_resolve           STRING,   -- stored as string instead of interval
        time_to_resolve_seconds   BIGINT
    )
    USING ICEBERG;   

        """
    )
    src_table_NSC = spark.sql(F"SELECT * FROM {src_layer}.{pipeline}.NSC")
    src_table_NSN = spark.sql(F"SELECT * FROM {src_layer}.{pipeline}.NSN")
    src_table_NSU = spark.sql(F"SELECT * FROM {src_layer}.{pipeline}.NSU")
    src_table_NSU = src_table_NSU.withColumn(
        "time_to_resolve",
        F.when(F.col("resolved").isNotNull(), F.col("resolved") - F.col("created")).otherwise(F.expr("INTERVAL 0 SECONDS"))
    )
    src_table_NSU = src_table_NSU.withColumn("time_to_resolve_seconds",F.col("time_to_resolve").cast("long"))
    src_table_NSN = src_table_NSN.withColumn(
        "time_to_resolve",
        F.when(F.col("resolved").isNotNull(), F.col("resolved") - F.col("created")).otherwise(F.expr("INTERVAL 0 SECONDS"))
    )
    src_table_NSN = src_table_NSN.withColumn("time_to_resolve_seconds",F.col("time_to_resolve").cast("long"))

    src_table_NSC = src_table_NSC.withColumn(
        "time_to_resolve",
        F.expr("""
        CASE
        WHEN status IN ('Resolved', 'Closed')
            THEN updated - created
        ELSE INTERVAL 0 SECONDS
        END AS time_to_resolve
    """))
    src_table_NSC = src_table_NSC.withColumn("time_to_resolve_seconds",F.col("time_to_resolve").cast("long"))
    
    src_table_NSU = src_table_NSU.withColumn("ticket_type_origin",F.expr("UPPER(TRIM(LEFT(key ,3)))"))
    src_table_NSN = src_table_NSN.withColumn("ticket_type_origin",F.expr("UPPER(TRIM(LEFT(key ,3)))"))
    src_table_NSC = src_table_NSC.withColumn("ticket_type_origin",F.expr("UPPER(TRIM(LEFT(key ,3)))"))
    src_table_NSC = src_table_NSC.withColumn("resolution",F.lit(None))
    src_table_NSC = src_table_NSC = src_table_NSC.withColumn(
        "resolved",
        F.expr("""
        CASE
        WHEN status IN ('Resolved', 'Closed')
            THEN updated 
        ELSE NULL
    END AS resolved
    """))
    src_table_NSU = src_table_NSU.select("ticket_type_origin","key","assignee","reporter","status","created","resolution","resolved","satisfaction","time_to_resolve","time_to_resolve_seconds")
    src_table_NSN = src_table_NSN.select("ticket_type_origin","key","assignee","reporter","status","created","resolution","resolved","satisfaction","time_to_resolve","time_to_resolve_seconds")
    src_table_NSC = src_table_NSC.select("ticket_type_origin","key","assignee","reporter","status","created","resolution","resolved","satisfaction","time_to_resolve","time_to_resolve_seconds")
    src_table_NSU.write.format("iceberg").mode("overwrite").saveAsTable(f"{tgt_layer}.{pipeline}.NSU")
    src_table_NSN.write.format("iceberg").mode("overwrite").saveAsTable(f"{tgt_layer}.{pipeline}.NSN")
    src_table_NSC.write.format("iceberg").mode("overwrite").saveAsTable(f"{tgt_layer}.{pipeline}.NSC")

if __name__ == "__main__":
    main()