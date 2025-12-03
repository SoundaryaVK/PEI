# Databricks notebook source
import os
import pathlib
import sys
notebook_path = os.getcwd()
sys.path.append(os.path.abspath("/Workspace/pei/"))
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import when, col, split, regexp_replace, round, year, sum, try_to_date, coalesce
from azure_databricks.utilities import (
    write_to_delta_table,
    read_table_from_catalogue,
    enrich_gold_profit_df
)

# COMMAND ----------

# create the spark session 
spark = SparkSession.builder.appName("gold").getOrCreate()


# COMMAND ----------

# MAGIC %run ./config.py

# COMMAND ----------

# reading the silver data
silver_orders_df = read_table_from_catalogue(spark, dst_catalogue_name, silver_dst_schema_name, silver_orders_dst_table_name)

# COMMAND ----------

def process_gold_table(spark, silver_orders_df: DataFrame, dst_catalogue_name: str, gold_dst_schema_name: str, gold_dst_table_name: str):
    """
       This function process the gold layer as per the profit aggregation
       Args:
       spark: SparkSession
       silver_orders_df: Spark Dataframe
       dst_catalogue_name: str
       gold_dst_schema_name: str
       gold_dst_table_name: str
    """
    profit_df = enrich_gold_profit_df(silver_orders_df)
    write_to_delta_table(spark , dst_catalogue_name, gold_dst_schema_name, gold_dst_table_name, profit_df)

# COMMAND ----------

process_gold_table(spark, silver_orders_df, dst_catalogue_name, gold_dst_schema_name, gold_dst_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC Profit By Year

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   Year, 
# MAGIC   round(sum(profit) , 2)as total_profit_by_year
# MAGIC from 
# MAGIC   workspace.gold.profit
# MAGIC group by 
# MAGIC   Year
# MAGIC order by 
# MAGIC   Year desc

# COMMAND ----------

# MAGIC %md
# MAGIC Profit by year and product category

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   Year, Product_Category,
# MAGIC   round(sum(profit) , 2) as total_profit_by_year
# MAGIC from 
# MAGIC   workspace.gold.profit
# MAGIC group by 
# MAGIC   Year, Product_Category
# MAGIC order by 
# MAGIC   Year desc

# COMMAND ----------

# MAGIC %md
# MAGIC Profit by customer

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   Customer_ID, customer_name,
# MAGIC   round(sum(profit) , 2) as total_profit_by_year
# MAGIC from 
# MAGIC   workspace.gold.profit
# MAGIC group by 
# MAGIC  Customer_ID, customer_name
# MAGIC order by 
# MAGIC   total_profit_by_year desc
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Profit By Customer + Year

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   Year,
# MAGIC   Customer_ID, 
# MAGIC   round(sum(profit) , 2) as total_profit_by_year
# MAGIC from 
# MAGIC   workspace.gold.profit
# MAGIC group by 
# MAGIC  Year, Customer_ID
# MAGIC order by 
# MAGIC   Year desc
# MAGIC
