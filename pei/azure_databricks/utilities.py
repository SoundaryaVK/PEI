import os
import pathlib
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import when, col, split, regexp_replace, round, sum, year, try_to_date, coalesce

def read_csv_file(
    spark: SparkSession,
    csv_file_path: str,
    header: bool = True,
    infer_schema: bool = True,
    delimiter: str = ","
) -> DataFrame:
    """
    Reads a CSV file (or directory of CSV files) into a PySpark DataFrame.

    Args:
        spark (SparkSession): The active SparkSession.
        csv_file_path (str): The path to the CSV file(s) or directory.
                              This can be a local path, HDFS, S3, DBFS, Unity Catalog Volume, etc.
        header (bool, optional): Whether the first line of the CSV file is a header.
                                 If True, Spark uses the first line as column names. Defaults to True.
        infer_schema (bool, optional): Whether Spark should infer the schema by sampling the data.
                                       If True, Spark tries to determine the data types of columns.
                                       Defaults to True.
        delimiter (str, optional): The character used to separate values in the CSV file.
                                   Defaults to "," (comma).

    Returns:
        pyspark.sql.DataFrame: A DataFrame containing the data from the CSV file(s).

    Raises:
        FileNotFoundError: If the specified CSV file path does not exist.
        Exception: For other errors encountered during the read operation.
    """
    try:
        print(f"Attempting to read CSV file(s) from: {csv_file_path}")

        df = spark.read \
            .option("header", header) \
            .option("inferSchema", infer_schema) \
            .option("delimiter", delimiter) \
            .csv(csv_file_path)

        print("Successfully read CSV file(s) into PySpark DataFrame.")
        return df

    except Exception as e:
        # Basic check for FileNotFoundError based on common error messages
        if "Path does not exist" in str(e) or "No such file or directory" in str(e):
            raise FileNotFoundError(f"Error: The file or directory '{csv_file_path}' was not found. Please check the path.")
        else:
            raise Exception(f"An error occurred while reading the CSV file(s): {e}")

def read_json_file(
    spark: SparkSession,
    json_file_path: str,
    infer_schema: bool = True,
    multi_line: bool = True
) -> DataFrame:
    """
    Reads a JSON file (or directory of JSON files) into a PySpark DataFrame.

    This function is configured to infer the schema and handle multi-line JSON records by default.

    Args:
        spark (SparkSession): The active SparkSession.
        json_file_path (str): The path to the JSON file(s) or directory.
                              This can be a local path, HDFS, S3, DBFS, Unity Catalog Volume, etc.
        infer_schema (bool, optional): Whether Spark should infer the schema by sampling the data.
                                       Defaults to True.
        multi_line (bool, optional): Whether to treat each JSON record as potentially spanning multiple lines.
                                     Set to True for pretty-printed JSON arrays or objects.
                                     Set to False for JSON Lines format (one JSON object per line).
                                     Defaults to True.

    Returns:
        pyspark.sql.DataFrame: A DataFrame containing the data from the JSON file(s).

    Raises:
        FileNotFoundError: If the specified JSON file path does not exist.
        Exception: For other errors encountered during the read operation.
    """
    try:
        print(f"Attempting to read JSON file(s) from: {json_file_path}")

        df = spark.read \
            .option("inferSchema", infer_schema) \
            .option("multiLine", multi_line) \
            .json(json_file_path)

        print("Successfully read JSON file(s) into PySpark DataFrame.")
        return df

    except Exception as e:
        # Basic check for FileNotFoundError based on common error messages
        if "Path does not exist" in str(e) or "No such file or directory" in str(e):
            raise FileNotFoundError(f"Error: The file or directory '{json_file_path}' was not found. Please check the path.")
        else:
            raise Exception(f"An error occurred while reading the JSON file(s): {e}")


def write_to_delta_table(spark: SparkSession, catalog: str, schema: str, table_name : str, df: DataFrame):
    """Removes the duplicate records in a delta table after reading and later write to the same path
    
    Args:
        spark: SparkSession
        catalog: str
        schema: str
        table_name: str
        df: Dataframe
    Return:
        None
    """
    table_path = f"{catalog}.{schema}.{table_name}"
    df = df.dropDuplicates()
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_path)


def clean_dataframe_column_names(df: DataFrame) -> DataFrame:
    """
    Cleans column names in a PySpark DataFrame by replacing spaces and hyphens with underscores.

    Args:
        df (DataFrame): The input PySpark DataFrame.

    Returns:
        DataFrame: A new PySpark DataFrame with cleaned column names.
    """
    # Get the current column names
    current_columns = df.columns
    
    # Initialize a DataFrame to store the result
    cleaned_df = df

    # Iterate through each column name
    for old_col_name in current_columns:
        # Replace spaces with underscores
        new_col_name = old_col_name.replace(" ", "_")
        # Replace hyphens with underscores
        new_col_name = new_col_name.replace("-", "_")
        
        # If the column name has changed, rename it
        if old_col_name != new_col_name:
            cleaned_df = cleaned_df.withColumnRenamed(old_col_name, new_col_name)
            
    return cleaned_df

def read_table_from_catalogue(spark: SparkSession, catalog: str, schema: str, table_name: str):
    """Reads a table from the bronze layer.

    Args:
        spark (SparkSession): The SparkSession object.
        catalog (str)       : The catalog name.
        schema (str)        : The schema name.
        table_name (str)
        :return: DataFrame
    """
    table_path = f"{catalog}.{schema}.{table_name}"
    df = spark.read.table(table_path)
    return df

def enrich_customer_data(customer_df: DataFrame)-> DataFrame:
    """ Function to clean the customer data
        1. Checking if the 'customer_name' column is null.
        2. If it is null, it takes the part of the 'email_id' before the '@' symbol.
        3. From this extracted part, it removes all non-alphabetic characters (numbers, symbols, etc.).
        4. If 'customer_name' is not null, its original value is retained.
        5. Assuming customer_id is the primary key
        6. As per our requirement we just need customer_id, customer_name and country , we need to drop all other columns because it has PII data 
        Args:
        customer_df : Customer Dataframe
    """
    enriched_customer_df = customer_df.withColumn(
        "customer_name",
        when(col("customer_name").isNull(),
             # Split email_id by '@' and get the first part (local part).
             # Then, remove all characters that are NOT a-z or A-Z from this local part.
             regexp_replace(split(col("email"), "@").getItem(0), "[^a-zA-Z]", "")
        ).otherwise(col("customer_name")))
    enriched_customer_df = customer_df.withColumn("customer_name",regexp_replace(col("customer_name"), "[^A-Za-z]", "" ))
    enriched_customer_df = enriched_customer_df.select("customer_id", "customer_name", "country")
    enriched_customer_df = enriched_customer_df.dropDuplicates()
    return enriched_customer_df

def enrich_products_data(product_df: DataFrame) -> DataFrame:
    """ Function to clean the products data
        1. As per our requirement we are not worried about the price per product
        2. we need the information of product_id , Category , Sub Category, Product Name
        
        """
    product_df = product_df.select("Product_ID", "Category", "Sub_Category")
    product_df = product_df.dropDuplicates()
    return product_df

def enrich_orders_data(order_df: DataFrame, customer_df: DataFrame, product_df: DataFrame) -> DataFrame:
    """ Function to clean the orders data
        1. We need to join the orders data with customer data and product data, customer_df, product_df """
    order_df = order_df.withColumn(
    "order_date_parsed",
    coalesce(
        try_to_date(col("Order_Date"), "dd/M/yyyy"),    # Try this format first
        try_to_date(col("Order_Date"), "dd/MM/yyyy"),  # Then try this
        try_to_date(col("Order_Date"), "d/M/yyyy"),  # Then this

    ))
    order_df = order_df.drop("Order_Date")
    order_df = order_df.withColumnRenamed("order_date_parsed", "Order_Date")
    order_df = order_df.withColumn("Profit", round(order_df.Profit, 2))
    customer_df = customer_df.withColumnRenamed("customer_id", "Customer_ID")
    result_df = order_df.join(customer_df, on = "Customer_ID", how = "left")
    result_df = result_df.join(product_df, on = "Product_ID", how = "left")
    result_df = result_df.dropDuplicates()
    return result_df


def enrich_gold_profit_df(silver_orders_df: DataFrame) -> DataFrame:
    """This functions get the gold profit dataframe which shows profit by 
        Year, 
        "Product_Category",
        "Product_Sub_Category",
        "Customer_ID",
        "customer_name" 
        Args:
        silver_orders_df : Silver Orders Dataframe 
    """
    silver_orders_df = silver_orders_df.withColumn("Year", year(col("Order_Date")))
    silver_orders_df = silver_orders_df.withColumnRenamed("Category", "Product_Category")
    silver_orders_df = silver_orders_df.withColumnRenamed("Sub_Category", "Product_Sub_Category")
    agg_gold_profit_df = silver_orders_df.groupby(
        "Year",
        "Product_Category",
        "Product_Sub_Category",
        "Customer_ID",
        "customer_name"
    ).agg(
        round(sum("Profit"), 2).alias("Profit")
    )
    agg_gold_profit_df = agg_gold_profit_df.dropDuplicates()
    return agg_gold_profit_df
