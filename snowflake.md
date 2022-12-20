# Competitive Hands-On Training - 2023 Q1
# Snowflake

## Setup
https://of53892.us-east-1.snowflakecomputing.com/

## Create Warehouse
```
create or replace warehouse tdalpha 
    warehouse_size=xsmall
    initially_suspended=true
    min_cluster_count=1
    max_cluster_count=1
    auto_suspend=300;
```

