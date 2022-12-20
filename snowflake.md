# Competitive Hands-On Training - 2023 Q1
# Snowflake

## Setup
https://of53892.us-east-1.snowflakecomputing.com/

## Hands-On

### Create Warehouse
```
create or replace warehouse tdalpha 
    warehouse_size=xsmall
    initially_suspended=true
    min_cluster_count=1
    max_cluster_count=1
    auto_suspend=300;
```

### Create Database and Tables
```
create or replace database tdalpha;
use tdalpha;

--nation
create or replace table nation
     (
      n_nationkey integer,
      n_name varchar(25),
      n_regionkey integer,
      n_comment varchar(152)
     );

--region
create or replace table region
     (
      r_regionkey integer,
      r_name varchar(25),
      r_comment varchar(152)
     );

--supplier
create or replace table supplier
     (
      s_suppkey integer,
      s_name varchar(25),
      s_address varchar(40),
      s_nationkey integer,
      s_phone varchar(15),
      s_acctbal decimal(15,2),
      s_comment varchar(101)
     );

--customer
create or replace table customer
     (
      c_custkey integer,
      c_name varchar(25),
      c_address varchar(40),
      c_nationkey integer,
      c_phone varchar(15),
      c_acctbal decimal(15,2),
      c_mktsegment char(10),
      c_comment varchar(117)
     );

--parttbl
create or replace table parttbl
     (
      p_partkey bigint,
      p_name varchar(55),
      p_mfgr char(25),
      p_brand char(10),
      p_type varchar(25),
      p_size integer,
      p_container char(10),
      p_retailprice decimal(15,2),
      p_comment varchar(23)
     );

--partsupp
create or replace table partsupp
     (
      ps_partkey bigint,
      ps_suppkey integer,
      ps_availqty integer,
      ps_supplycost decimal(15,2),
      ps_comment varchar(199)
     );

--ordertbl
create or replace table ordertbl
     (
      o_orderkey bigint,
      o_custkey integer,
      o_orderstatus char(1),
      o_totalprice decimal(15,2),
      o_orderdate date,
      o_orderpriority char(15),
      o_clerk varchar(15),
      o_shippriority integer,
      o_comment varchar(79)
     );

--lineitem
create or replace table lineitem
     (
      l_orderkey bigint,
      l_partkey bigint,
      l_suppkey integer,
      l_linenumber integer,
      l_quantity decimal(15,2),
      l_extendedprice decimal(15,2),
      l_discount decimal(15,2),
      l_tax decimal(15,2),
      l_returnflag char(1),
      l_linestatus char(1),
      l_shipdate date,
      l_commitdate date,
      l_receiptdate date,
      l_shipinstruct char(25),
      l_shipmode char(10),
      l_comment varchar(44)
     );
```

