# Databricks notebook source
import os
import pathlib
import sys
notebook_path = os.getcwd()
sys.path.append(os.path.abspath("/Workspace/pei/"))
from pyspark.sql import SparkSession, DataFrame
import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import split, col, sum, col, try_to_date
from azure_databricks.utilities import (
    read_table_from_catalogue,
    clean_dataframe_column_names,
    enrich_customer_data,
    enrich_products_data,
    enrich_orders_data,
    enrich_gold_profit_df
)
from azure_databricks.unit_test_cases import(
    primary_key_null_check,
    primary_key_unique_check,
    check_customer_name_format,
    check_date_time_format,
    check_decimal_format, 
    check_column_availability,
    check_empty_dataframe,
    check_sum_profit_across_layers,
    check_unique_product_category_sub_category,
    check_proper_grouping_gold_profit_data
)

# COMMAND ----------

# create the spark session 
spark = SparkSession.builder.appName("unit_test").getOrCreate()

# COMMAND ----------

# MAGIC %run ./config.py

# COMMAND ----------

# MAGIC %md
# MAGIC # Preparing the test data
# MAGIC

# COMMAND ----------

test_customer_df = read_table_from_catalogue(spark, dst_catalogue_name, bronze_dst_schema_name, bronze_customer_dst_table_name)
test_orders_df = read_table_from_catalogue(spark, dst_catalogue_name, bronze_dst_schema_name, bronze_orders_dst_table_name)
test_products_df = read_table_from_catalogue(spark, dst_catalogue_name, bronze_dst_schema_name, bronze_products_dst_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC # Test Case testing null values and duplicates in the primary key column 
# MAGIC In customer data the primary key column is customer_id 
# MAGIC In product data the primary key column is product_id

# COMMAND ----------

test_enriched_customer_df = enrich_customer_data(test_customer_df)
test_enriched_products_df = enrich_products_data(test_products_df)
# null check in primary key column for customer_data
primary_key_null_check(test_enriched_customer_df, "customer_id")
# duplicates check in primary key column for customer data 
primary_key_unique_check(test_enriched_customer_df, "customer_id")
# null check in primary key column for customer_data
primary_key_null_check(test_enriched_products_df, "product_id")
# duplicates check in primary key column for customer data 
primary_key_unique_check(test_enriched_products_df, "product_id")

# COMMAND ----------

# MAGIC %md
# MAGIC #Test the customer_name format in customer_data

# COMMAND ----------

check_customer_name_format(test_enriched_customer_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Test Case to check the datetype of the column 
# MAGIC In orders data the column order_date is string , to extact the year from it we need to convert the it to date and column holds multiple date formats . hence it should be tested after the transformation . 
# MAGIC

# COMMAND ----------

test_enriched_orders_df = enrich_orders_data(test_orders_df, test_enriched_customer_df, test_enriched_products_df)

# COMMAND ----------

check_date_time_format(test_enriched_orders_df, "Order_Date")

# COMMAND ----------

# MAGIC %md
# MAGIC # Test Case to check the decimal value of the Profit column in orders table is round of 2 decimal places 

# COMMAND ----------

check_decimal_format(test_enriched_orders_df, "Profit")

# COMMAND ----------

# MAGIC %md
# MAGIC #Test the availability of the column in enriched dataframes 
# MAGIC

# COMMAND ----------

# Testing for column availability for customer data 
check_column_availability(test_customer_df , ["customer_id", "customer_name", "country"])
# Testing for column availability for products data 
check_column_availability(test_enriched_products_df , ["Product_ID", "Category", "Sub_Category"])
check_column_availability(test_enriched_orders_df , ["Product_ID", "Customer_ID", "Discount", "Order_ID", "Price", "Profit", "Quantity", "Row_ID", "Ship_Date", "Ship_Mode", "Order_Date", "customer_name", "country", "Category", "Sub_Category"])
check_column_availability

# COMMAND ----------

# MAGIC %md
# MAGIC #Test the empty table check for enriched tables 
# MAGIC
# MAGIC

# COMMAND ----------

#customer_data
check_empty_dataframe(test_customer_df)
#products_data
check_empty_dataframe(test_enriched_products_df)
#orders_data
check_empty_dataframe(test_enriched_orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Test the sum of the profit across the layers should be same 
# MAGIC

# COMMAND ----------

test_gold_profit_df = enrich_gold_profit_df(test_enriched_orders_df)

# COMMAND ----------

check_sum_profit_across_layers(test_orders_df, test_enriched_orders_df, test_gold_profit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Test the unique entry for Product Category and PRoduct Sub Category between Bronze and Silver 

# COMMAND ----------

check_unique_product_category_sub_category(test_enriched_products_df, test_products_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Test the proper grouping of the gold profit data

# COMMAND ----------

check_proper_grouping_gold_profit_data(test_gold_profit_df)

# COMMAND ----------


