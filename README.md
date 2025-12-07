# PEI
PEI Assigment
In this Project we follow MEdallion Architecture - Bronze , Silver, Gold ,
All layers are in Unity Catalogue in the below Schema Names 

1 .Bronze Schema have 3 raw tables namely bronze_customers, bronze_products, bronze_orders

2. Silver Schema have 3 enriched tables namely silver_customer, silver_orders, silver_products
  
3. Gold Schema have aggregated table named Profit which holds the profit information

I have developed the logic with test case driven approach . 

Here are the following testcases . 
primary_key_null_check,
primary_key_unique_check,
check_customer_name_format,
check_date_time_format,
check_decimal_format, 
check_column_availability,
check_empty_dataframe,
check_sum_profit_across_layers,
check_unique_product_category_sub_category,
check_proper_grouping_gold_profit_data,
check_year_range,
check_order_to_customer_enriched_foreignkeyintegrity,
validate_aggregated_profit_accuracy,
check_order_to_product_enriched_foreignkeyintegrity, check_string_datatype_column
