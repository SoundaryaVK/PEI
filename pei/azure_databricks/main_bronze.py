# Databricks notebook source
import os
import pathlib
import sys
notebook_path = os.getcwd()
sys.path.append(os.path.abspath('../../../'))
from pyspark.sql import SparkSession, DataFrame
from azure_databricks.utilities import (
    read_csv_file,
    read_json_file,
    write_to_delta_table,
    clean_dataframe_column_names
)

# COMMAND ----------

notebook_path

# COMMAND ----------

# create the spark session 
spark = SparkSession.builder.appName("bronze").getOrCreate()

# COMMAND ----------

# MAGIC %run ./config.py

# COMMAND ----------

#processing the bronze layer 
def process_bronze(source_file_path: str, source_file_type:str, target_table_name:str, catalogue_name:str, schema_name:str, spark: SparkSession)-> DataFrame:
    """ This function processes the bronze layer of the data lake.
        source_file_path : The path to the source file.
        source_file_type : The type of the source file eg : csv, json.
        target_table_name : The name of the target table
        catalogue_name : The name of the catalogue
        schema_name : The name of the schema
        spark : The spark session
    """
    if source_file_type == "csv":
        df = read_csv_file(spark, source_file_path)
    elif source_file_type == "json":
        df = read_json_file(spark, source_file_path)
    else:
        raise ValueError("Invalid source file type")
    try:
        df = clean_dataframe_column_names(df)
        write_to_delta_table(spark, catalogue_name, schema_name, target_table_name, df)
    except Exception as e:
        print(f"Error writing to delta table: {e}")
    return df

# COMMAND ----------

# processing products table
products_df = process_bronze(products_source_path, products_source_type, bronze_products_dst_table_name, dst_catalogue_name, bronze_dst_schema_name, spark)


# COMMAND ----------

# processing orders table 
orders_df = process_bronze(orders_source_path, orders_source_type, bronze_orders_dst_table_name, dst_catalogue_name, bronze_dst_schema_name, spark)

# COMMAND ----------

# MAGIC %md 
# MAGIC Given the constraints of Databricks Free Edition's serverless compute, which restricts custom library installations, directly processing the customer table from its original XLSX file source was not feasible due to the typical requirement for a Maven library. To address this, the dataset was ingested using Databricks' built-in ingestion feature, specifically "ingest via partner -> Fivetran." This process directly created a Delta table within a new schema named "google_drive." Consequently, the bronze layer will directly consume data from this source schema's Delta table.

# COMMAND ----------

# reading customer table from schema google_drive and writing it to our bronze layer
customer_df = spark.sql(f"""
                            select * from workspace.google_drive.customer_worksheet
                        """)
write_to_delta_table(spark, dst_catalogue_name, bronze_dst_schema_name, bronze_customer_dst_table_name, customer_df)
