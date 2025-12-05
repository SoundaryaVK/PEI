# Databricks notebook source
import os
import pathlib
import sys
notebook_path = os.getcwd()
sys.path.append(os.path.abspath("/Workspace/pei/"))
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import when, col, split, regexp_replace, round, try_to_timestamp, coalesce
from azure_databricks.utilities import (
    write_to_delta_table,
    read_table_from_catalogue,
    enrich_customer_data,
    enrich_products_data,
    enrich_orders_data

)

# COMMAND ----------

# create the spark session 
spark = SparkSession.builder.appName("silver").getOrCreate()


# COMMAND ----------

# MAGIC %run ./config.py

# COMMAND ----------

# reading the bronze data for customer, products and orders
try:
    customer_df = read_table_from_catalogue(spark, dst_catalogue_name, bronze_dst_schema_name, bronze_customer_dst_table_name)
    orders_df = read_table_from_catalogue(spark, dst_catalogue_name, bronze_dst_schema_name, bronze_orders_dst_table_name)
    products_df = read_table_from_catalogue(spark, dst_catalogue_name, bronze_dst_schema_name, bronze_products_dst_table_name)
except Exception as e:
    print(f"Error while loading the data from bronze : {e}")

# COMMAND ----------

if ((customer_df.count() == 0) or (orders_df.count() == 0) or (products_df.count() == 0)):
    raise ValueError ("No data in bronze tables")

# COMMAND ----------

# process the silver layer
def process_silver(spark: SparkSession , customer_df: DataFrame, orders_df: DataFrame, products_df: DataFrame, silver_orders_dst_table_name: str, silver_customer_dst_table_name: str, silver_products_dst_table_name: str, silver_dst_schema_name: str, dst_catalogue_name: str) -> DataFrame:
    """
    Function to process the silver layer
    1. Enrich the customer data
    2. Enrich the products data
    3. Enrich the orders data
    
    Args:
    customer_df : Bronze Customer Dataframe
    orders_df : Bronze Orders Dataframe
    products_df :  Bronze Products Dataframe
    silver_orders_dst_table_name: Destination table name for silver orders
    silver_customer_dst_table_name: Destination table name for silver customer
    silver_products_dst_table_name: Destination table name for silver products
    silver_dst_schema_name: Destination schema name for silver tables
    dst_catalogue_name: Destination catalogue name for silver tables
    """
    silver_customer_df = enrich_customer_data(customer_df)
    silver_products_df = enrich_products_data(products_df)
    silver_orders_df = enrich_orders_data(orders_df, silver_customer_df, silver_products_df)
    # writing it to silver tables 
    try:
        if (silver_customer_df.count() > 0):
            write_to_delta_table(spark , dst_catalogue_name, silver_dst_schema_name, silver_customer_dst_table_name, silver_customer_df)
        else:
            raise ValueError ("No data in silver customer dataframe")
        if (silver_products_df.count() > 0):
            write_to_delta_table(spark , dst_catalogue_name, silver_dst_schema_name, silver_products_dst_table_name, silver_products_df)
        else:
            raise ValueError("No data in silver products dataframe")
        if (silver_orders_df.count() > 0):
            write_to_delta_table(spark , dst_catalogue_name, silver_dst_schema_name, silver_orders_dst_table_name, silver_orders_df)
        else:
            raise ValueError("No data in silver orders dataframe")
    except Exception as e:
        raise e
    

# COMMAND ----------

process_silver(spark, customer_df, orders_df, products_df, silver_orders_dst_table_name, silver_customer_dst_table_name, silver_products_dst_table_name, silver_dst_schema_name, dst_catalogue_name)
