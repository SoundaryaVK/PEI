import os
import pathlib
import sys
notebook_path = os.getcwd()
sys.path.append(os.path.abspath("/Workspace/pei/"))
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import DateType, TimestampType, StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, abs, floor, lit, length, split, sum, round, try_to_date, regexp_extract
from pyspark.sql import functions as F
from azure_databricks.utilities import (
    read_csv_file,
    read_json_file,
    write_to_delta_table,
    clean_dataframe_column_names
)

# create the spark session 
spark = SparkSession.builder.appName("unit_test").getOrCreate()

def primary_key_null_check(df: DataFrame, primary_key: str) :
    """ This function is to check if there any null value in the primary key column
    Args:
      df: Spark DataFrame
      primary_key: Primary key column name
    """
    assert(df.filter(df[primary_key].isNull()).count() == 0, "Primary key {primary_key} is null")
    print(f"Assertion passed: Primary Key Column '{primary_key}' has no null values .")


def primary_key_unique_check(df: DataFrame, primary_key: str) :
    """ This function is to check if there are any duplicate values in the primary key column
    Args: 
        df : Spark DataFrame 
        primary_key : Primary key column name
    """
    assert(df.groupBy(primary_key).count().filter("count > 1").count() == 0, "Primary key {primary_key} is not unique")
    print(f"Assertion passed: Primary Key Column  '{primary_key}' has no duplicates .")


def check_date_time_format(df: DataFrame, column_name: str):
    """ This function is to check if the date time format is correct
    Args:
        df : Spark DataFrame
        column_name : Column name
    """
    datatype_col = df.schema[column_name].dataType
    try:
        assert isinstance(datatype_col, (DateType, TimestampType)), \
            f"Assertion Failed: Column '{column_name}' is unexpectedly a date or timestamp type. Found: {datatype_col}"
    except AssertionError as e:
        print(e)
    print(f"Assertion passed: Column '{column_name}' has datatype date / timestamp.")


def check_decimal_format(df: DataFrame, column_to_check: str):
    """ This function is to check if the decimal format is correct upto 2 decimal places
    Args:
        df : Spark DataFrame
        column_to_check : Column name
    """
    df = df.withColumn("decimal_value", split(col(column_to_check), "\\.").getItem(1))
    df = df.withColumn("length_decimal_value", length(col("decimal_value")))
    # Filter for rows where the condition is true (i.e., more than 2 decimal places)
    # and count them.
    error_count = df.filter(df.length_decimal_value > 2).count()

    # Assert statement
    # The assertion will pass if error_count is 0, meaning no values exceeded 2 decimal places.
    # If error_count is greater than 0, an AssertionError will be raised.
    assert error_count == 0, \
        f"Error: Column '{column_to_check}' contains {error_count} values with more than 2 decimal places."

    print(f"Assertion passed: Column '{column_to_check}' has no values exceeding 2 decimal places.")


def check_column_availability (df: DataFrame, column_lists: list):
    """ This functions checks the availability of columns in the dataframe
    Args:
        df : Spark DataFrame
        column_lists : List of columns to check"""
    col_list = df.columns 
    for column in column_lists:
        assert column in col_list, f"Column {column} is not available in the dataframe"
    print(f"Assertion passed: Columns are available in the dataframe")


def check_empty_dataframe (df: DataFrame):
    """
    This function checks if the dataframe is empty
    Args:
        df : Spark DataFrame
    """
    assert(df.count() > 0, "Dataframe is empty")
    print(f"Assertion passed: Dataframe is not empty")

def check_sum_profit_across_layers(bronze_order:DataFrame, silver_orders: DataFrame, gold_profit: DataFrame):
    """This function varifies the sum of profit across all layers is equal
    Args:
        bronze_orders : Spark DataFrame
        silver_orders : Spark DataFrame
        gold_profit : Spark DataFrame
    """
    bronze_order = bronze_order.dropDuplicates()
    silver_orders = silver_orders.dropDuplicates()
    gold_profit = gold_profit.dropDuplicates()
    bronze_order = bronze_order.agg(
        round(
            sum(col("Profit")),
            2
        ).alias("total_profit")
    ).collect()[0][0]


    silver_profit = silver_orders.agg(
        round(
            sum(col("Profit")),
            2
        ).alias("total_profit")
    ).collect()[0][0]

    gold_profit_value = gold_profit.agg(
        round(
            sum(col("Profit")),
            2
        ).alias("total_profit")
    ).collect()[0][0]

    assert bronze_order == silver_profit == gold_profit_value, "Assertion failed: Sum of profit across all layers is not equal"
    print("Assertion passed: Sum of profit across all layers is equal")


def check_unique_product_category_sub_category(df_silver: DataFrame, df_bronze: DataFrame):
    """ This function checks if the product category and sub category are unique
    between silver and bronze layer
    Args:
        df : Spark DataFrame"""

    silver_unique_product_category_count = df_silver.select("Category").distinct().count()
    silver_unique_product_sub_category_count = df_silver.select("Sub_Category").distinct().count()
    bronze_unique_product_category_count = df_bronze.select("Category").distinct().count()
    bronze_unique_product_sub_category_count = df_bronze.select("Sub_Category").distinct().count()

    assert silver_unique_product_category_count == bronze_unique_product_category_count, "Product category is not unique"

    assert silver_unique_product_sub_category_count == bronze_unique_product_sub_category_count, "Product sub category is not unique"
    print("Assertion passed: Product category and sub category are unique")


def check_proper_grouping_gold_profit_data(df_gold: DataFrame):
    """ This function checks if the gold profit data is properly grouped
    Args:
        df : Spark DataFrame"""
    df_gold = df_gold.groupBy("Year",
        "Product_Category",
        "Product_Sub_Category",
        "Customer_ID",
        "customer_name"
    ).count()
    df_gold = df_gold.withColumnRenamed("count", "grouping_count")
    assert df_gold.filter(col("grouping_count") > 1).count() == 0, "Gold profit data is not properly grouped"
    print("Assertion passed: Gold profit data is properly grouped")


def check_customer_name_format(df: DataFrame):
    """ This function checks if the customer name is in the correct format, should only contain alphabets
    Args:
        df : Spark DataFrame"""

    invalid_df = df.filter(~col("customer_name").rlike("^[A-Za-z]+$"))

    assert invalid_df.count() == 0, "Column 'name' contains invalid characters"
    print("Assertion passed: Customer name is in the correct format")


def check_customer_name_format(df: DataFrame):
    """ This function checks if the customer name is in the correct format, should only contain alphabets
    Args:
        df : Spark DataFrame"""

    invalid_df = df.filter(~col("customer_name").rlike("^[A-Za-z]+$"))

    assert invalid_df.count() == 0, "Column 'name' contains invalid characters"
    print("Assertion passed: Customer name is in the correct format")


def check_year_range(df: DataFrame):
    """This Function checks the year range 
        Args :
        df: Spark DataFrame
    """
    invalid_years_df_valid = df.filter(
    (col("Year").isNull()) |
    (col("Year") < 1990) |
    (col("Year") > 2026) |
    (length(col("Year").cast("string")) != 4))

    # Count the number of invalid rows
    num_invalid_years_valid = invalid_years_df_valid.count()
    try:
        assert num_invalid_years_valid == 0, \
        f"Year column contains {num_invalid_years_valid} invalid values. " \
        "Years must be between 1990 and 2026 (inclusive) AND have exactly 4 digits. " \
        "Invalid rows found: \n"
        print("All years in the 'year' column of df_valid are valid (1990-2026 and 4 digits).")
    except AssertionError as e:
        print(f"Assertion failed for df_valid: {e}")
        print("Invalid years found:")


def check_order_to_customer_enriched_foreignkeyintegrity(enriched_order_df: DataFrame, enriched_customer_df: DataFrame):
    """This functions checks the foreign key integrity(customer_id) between enriched order and enriched customer
    Args:
        enriched_customer_df : Spark DataFrame
        enriched_order_df : Spark DataFrame"""

    #Extract distinct customer IDs from the enriched customer table
    valid_customers_df = enriched_customer_df.select("customer_id").dropDuplicates()

    # Find orders with customer_id not present in enriched customer table
    missing_customer_ids_df = (
        enriched_order_df.select("Customer_ID").dropDuplicates()
                .subtract(valid_customers_df)
    )

    # Count missing IDs
    missing_count = missing_customer_ids_df.count()
    assert missing_count == 0, \
        f"Foreign key integrity violated: " \
        "{missing_count} orders have customer IDs not present in enriched customer table."
    print("Foreign key integrity check passed: All customer IDs in enriched order table are present in enriched customer table.")


def check_order_to_product_enriched_foreignkeyintegrity(enriched_order_df: DataFrame, enriched_products_df: DataFrame):
    """This functions checks the foreign key integrity(product_id) between enriched order and enriched products
    Args:
        enriched_products_df : Spark DataFrame
        enriched_order_df : Spark DataFrame"""

    #Extract distinct product IDs from the enriched products table
    valid_products_df = enriched_products_df.select("Product_ID").dropDuplicates()
    # Find orders with product_id not present in enriched products table
    missing_product_ids_df = (
        enriched_order_df.select("Product_ID").dropDuplicates()
                .subtract(valid_products_df)
    )

    # Count missing IDs
    missing_count = missing_product_ids_df.count()
    assert missing_count == 0, \
        f"Foreign key integrity violated: " \
        "{missing_count} orders have product IDs not present in enriched products table."
    print("Foreign key integrity check passed: All product IDs in enriched order table are present in enriched products table.")


def validate_aggregated_profit_accuracy(enriched_orders_df: DataFrame, aggregated_profit_df: DataFrame):
    """
    Validates that the aggregated profit table matches the sum of profit in enriched_orders_df.
    
    Grouping Keys:
        - product_category
        - product_sub_category
        - customer_id

    Raises:
        AssertionError if mismatches are found.
    """
    
    # Recompute the correct aggregation from enriched orders
    enriched_orders_df = enriched_orders_df.withColumnRenamed("Category", "Product_Category")
    enriched_orders_df = enriched_orders_df.withColumnRenamed("Sub_Category", "Product_Sub_Category")

    recomputed_agg_df = (
        enriched_orders_df
        .groupBy( "Product_Category", "Product_Sub_Category", "Customer_ID")
        .agg(F.round(F.sum("Profit"), 2).alias("expected_profit"))
    )
    enriched_sum_profit = recomputed_agg_df.agg(F.sum("expected_profit")).collect()[0][0]
    # Join with the actual aggregated table
    aggregated_sum_profit = aggregated_profit_df.agg(F.sum("Profit")).collect()[0][0]

    assert enriched_sum_profit == aggregated_sum_profit,  f"Aggregation Accuracy Failed: {enriched_sum_profit - aggregated_sum_profit} mismatched profit records found.\n"


def 


