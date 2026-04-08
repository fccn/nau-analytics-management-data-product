from pyspark.sql import DataFrame #type:ignore
import pyspark.sql.functions as F #type:ignore
from pyspark.sql.functions import col #type:ignore
from nau_analytics_data_product_utils_lib import start_iceberg_session,get_required_env #type: ignore
import logging


def main():
    ENVIRONMENT = get_required_env("ENVIRONMENT")

    spark = start_iceberg_session("silver_gestao_downtimes")
    src_layer = f"silver{ENVIRONMENT}"
    tgt_layer = f"gold{ENVIRONMENT}"
    pipeline = "gestao"
    table_name = "jira_tickets"
    spark.sql(
    f"""
     CREATE TABLE IF NOT EXISTS {tgt_layer}.{pipeline}.{table_name} (
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
        time_to_resolve_seconds   BIGINT,
        is_corporativo            INT NOT NULL
    )
    USING ICEBERG;   

        """
    )
    src_table_NSC = spark.sql(F"SELECT * FROM {src_layer}.{pipeline}.NSC")
    src_table_NSN = spark.sql(F"SELECT * FROM {src_layer}.{pipeline}.NSN")
    src_table_NSU = spark.sql(F"SELECT * FROM {src_layer}.{pipeline}.NSU")
    src_table = src_table_NSU.unionByName(src_table_NSN).unionByName(src_table_NSC)
    src_table = src_table.withColumn("is_corporativo", F.when(F.col("ticket_type_origin")=="NSC",1).otherwise(0))
    tgt_nr = src_table.count()
    src_nr = src_table_NSU.count()+src_table_NSN.count()+src_table_NSC.count()
    if tgt_nr != src_nr:
        raise ValueError(f"Data loss in aggregation src_nr = {src_nr} tgt_nr = {tgt_nr}")
    src_table.write.format("iceberg").mode("overwrite").saveAsTable(f"{tgt_layer}.{pipeline}.{table_name}")

if __name__ == "__main__":
    main()