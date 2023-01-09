# Competitive Hands-On Training - 2023 Q1
# BigQuery

## Login
https://console.cloud.google.com/welcome?project=striking-domain-287814

### Gcloud Auth, Create a Dataset, and BigQuery CLI Config

<details>

```bash
gcloud auth login
gcloud config set project striking-domain-287814
```

```bash
bq ls -d
```

```bash
mydataset=tdalpha
```

```bash
cat > .bigqueryrc <<EOF
project_id = striking-domain-287814
format=pretty
location=US
dataset_id=${mydataset}

[query]
use_legacy_sql=false
max_rows=100
maximum_bytes_billed=10000000
EOF
```

</details>

## Basic Functionality

### Create Tables

<details>

```sql
--NATION
create or replace table tdalpha.NATION
     (
      n_nationkey int64,
      n_name string,
      n_regionkey int64,
      n_comment string
     );

--REGION
create or replace table tdalpha.REGION
     (
      r_regionkey int64,
      r_name string,
      r_comment string
     );

--SUPPLIER
create or replace table tdalpha.SUPPLIER
     (
      s_suppkey int64,
      s_name string,
      s_address string,
      s_nationkey int64,
      s_phone string,
      s_acctbal numeric,
      s_comment string
     );

--CUSTOMER
create or replace table tdalpha.CUSTOMER
     (
      c_custkey int64,
      c_name string,
      c_address string,
      c_nationkey int64,
      c_phone string,
      c_acctbal numeric,
      c_mktsegment string,
      c_comment string
     );

--PARTTBL
create or replace table tdalpha.PARTTBL
     (
      p_partkey bigint,
      p_name string,
      p_mfgr string,
      p_brand string,
      p_type string,
      p_size int64,
      p_container string,
      p_retailprice numeric,
      p_comment string
     );

--PARTSUPP
create or replace table tdalpha.PARTSUPP
     (
      ps_partkey bigint,
      ps_suppkey int64,
      ps_availqty int64,
      ps_supplycost numeric,
      ps_comment string
     );

--ORDERTBL
create or replace table tdalpha.ORDERTBL
     (
      o_orderkey int64,
      o_custkey int64,
      o_orderstatus string,
      o_totalprice numeric,
      o_orderdate date,
      o_orderpriority string,
      o_clerk string,
      o_shippriority int64,
      o_comment string
     );

--LINEITEM
create or replace table tdalpha.LINEITEM
     (
      l_orderkey int64,
      l_partkey int64,
      l_suppkey int64,
      l_linenumber int64,
      l_quantity numeric,
      l_extendedprice numeric,
      l_discount numeric,
      l_tax numeric,
      l_returnflag string,
      l_linestatus string,
      l_shipdate date,
      l_commitdate date,
      l_receiptdate date,
      l_shipinstruct string,
      l_shipmode string,
      l_comment string
     );
```

</details>

### Loading Data (Directly from GCS)

<details>

```bash
cat > nation.json <<EOF
[
	{
		"description": "nation nbr",
		"mode": "REQUIRED",
		"name": "N_NATIONKEY",
		"type": "INTEGER"
	},
	{
		"description": "nation name",
		"mode": "REQUIRED",
		"name": "N_NAME",
		"type": "STRING"
	},
	{
		"description": "region nbr",
		"mode": "REQUIRED",
		"name": "N_REGIONKEY",
		"type": "INTEGER"
	},
	{
		"description": "nation comment",
		"mode": "REQUIRED",
		"name": "N_COMMENT",
		"type": "STRING"
	}
]
EOF
bq load --source_format=CSV --field_delimiter='|' NATION gs://mcg-tdc2/tpch/30gb/nation* ./nation.json
```

```bash
cat > region.json <<EOF
[
	{
		"description": "region nbr",
		"mode": "REQUIRED",
		"name": "R_REGIONKEY",
		"type": "INTEGER"
	},
	{
		"description": "region name",
		"mode": "REQUIRED",
		"name": "R_NAME",
		"type": "STRING"
	},
	{
		"description": "region comment",
		"mode": "REQUIRED",
		"name": "R_COMMENT",
		"type": "STRING"
	}
]
EOF
bq load --source_format=CSV --field_delimiter='|' REGION gs://mcg-tdc2/tpch/30gb/region* ./region.json
```

```bash
cat > supplier.json <<EOF
[
	{
		"description": "supplier nbr",
		"mode": "REQUIRED",
		"name": "S_SUPPKEY",
		"type": "INTEGER"
	},
	{
		"description": "supplier name",
		"mode": "REQUIRED",
		"name": "S_NAME",
		"type": "STRING"
	},
	{
		"description": "supplier address",
		"mode": "REQUIRED",
		"name": "S_ADDRESS",
		"type": "STRING"
	},
	{
		"description": "nation nbr",
		"mode": "REQUIRED",
		"name": "S_NATIONKEY",
		"type": "INTEGER"
	},
	{
		"description": "supplier phone",
		"mode": "REQUIRED",
		"name": "S_PHONE",
		"type": "STRING"
	},
	{
		"description": "supplier account balance",
		"mode": "REQUIRED",
		"name": "S_ACCTBAL",
		"type": "NUMERIC"
	},
	{
		"description": "supplier comment",
		"mode": "REQUIRED",
		"name": "S_COMMENT",
		"type": "STRING"
	}
]
EOF
bq load --source_format=CSV --field_delimiter='|' SUPPLIER gs://mcg-tdc2/tpch/30gb/supplier* ./supplier.json
```

```bash
cat > parttbl.json <<EOF
[
	{
		"description": "part nbr",
		"mode": "REQUIRED",
		"name": "P_PARTKEY",
		"type": "INTEGER"
	},
	{
		"description": "part name",
		"mode": "REQUIRED",
		"name": "P_NAME",
		"type": "STRING"
	},
	{
		"description": "part mfgr",
		"mode": "REQUIRED",
		"name": "P_MFGR",
		"type": "STRING"
	},
	{
		"description": "part brand",
		"mode": "REQUIRED",
		"name": "P_BRAND",
		"type": "STRING"
	},
	{
		"description": "part TYPE",
		"mode": "REQUIRED",
		"name": "P_TYPE",
		"type": "STRING"
	},
	{
		"description": "part size",
		"mode": "REQUIRED",
		"name": "P_SIZE",
		"type": "INTEGER"
	},
	{
		"description": "part container",
		"mode": "REQUIRED",
		"name": "P_CONTAINER",
		"type": "STRING"
	},
	{
		"description": "part retailprice",
		"mode": "REQUIRED",
		"name": "P_RETAILPRICE",
		"type": "NUMERIC"
	},
	{
		"description": "part comment",
		"mode": "REQUIRED",
		"name": "P_COMMENT",
		"type": "STRING"
	}
]
EOF
bq load --source_format=CSV --field_delimiter='|' PARTTBL gs://mcg-tdc2/tpch/30gb/parttbl* ./parttbl.json
```

```bash
cat > partsupp.json <<EOF
[
	{
		"description": "part nbr",
		"mode": "REQUIRED",
		"name": "PS_PARTKEY",
		"type": "INTEGER"
	},
	{
		"description": "supplier nbr",
		"mode": "REQUIRED",
		"name": "PS_SUPPKEY",
		"type": "INTEGER"
	},
	{
		"description": "partsupp avail qty",
		"mode": "REQUIRED",
		"name": "PS_AVAILQTY",
		"type": "INTEGER"
	},
	{
		"description": "partsupp supply cost",
		"mode": "REQUIRED",
		"name": "PS_SUPPLYCOST",
		"type": "NUMERIC"
	},
	{
		"description": "partsupp comment",
		"mode": "REQUIRED",
		"name": "PS_COMMENT",
		"type": "STRING"
	}
]
EOF
bq load --source_format=CSV --field_delimiter='|' PARTSUPP gs://mcg-tdc2/tpch/30gb/partsupp* ./partsupp.json
```

```bash
cat > customer.json <<EOF
[
	{
		"description": "customer nbr",
		"mode": "REQUIRED",
		"name": "C_CUSTKEY",
		"type": "INTEGER"
	},
	{
		"description": "customer name",
		"mode": "REQUIRED",
		"name": "C_NAME",
		"type": "STRING"
	},
	{
		"description": "customer address",
		"mode": "REQUIRED",
		"name": "C_ADDRESS",
		"type": "STRING"
	},
	{
		"description": "nation nbr",
		"mode": "REQUIRED",
		"name": "C_NATIONKEY",
		"type": "INTEGER"
	},
	{
		"description": "customer phone",
		"mode": "REQUIRED",
		"name": "C_PHONE",
		"type": "STRING"
	},
	{
		"description": "customer account balance",
		"mode": "REQUIRED",
		"name": "C_ACCTBAL",
		"type": "NUMERIC"
	},
	{
		"description": "customer market segment",
		"mode": "REQUIRED",
		"name": "C_MKTSEGMENT",
		"type": "STRING"
	},
	{
		"description": "customer comment",
		"mode": "REQUIRED",
		"name": "C_COMMENT",
		"type": "STRING"
	}
]
EOF
bq load --source_format=CSV --field_delimiter='|' CUSTOMER gs://mcg-tdc2/tpch/30gb/customer* ./customer.json
```

```bash
cat > ordertbl.json <<EOF
[
	{
		"description": "order nbr",
		"mode": "REQUIRED",
		"name": "O_ORDERKEY",
		"type": "INTEGER"
	},
	{
		"description": "customer nbr",
		"mode": "REQUIRED",
		"name": "O_CUSTKEY",
		"type": "INTEGER"
	},
	{
		"description": "order status",
		"mode": "REQUIRED",
		"name": "O_ORDERSTATUS",
		"type": "STRING"
	},
	{
		"description": "order totalprice",
		"mode": "REQUIRED",
		"name": "O_TOTALPRICE",
		"type": "NUMERIC"
	},
	{
		"description": "order date",
		"mode": "REQUIRED",
		"name": "O_ORDERDATE",
		"type": "DATE"
	},
	{
		"description": "order priority",
		"mode": "REQUIRED",
		"name": "O_ORDERPRIORITY",
		"type": "STRING"
	},
	{
		"description": "order clerk",
		"mode": "REQUIRED",
		"name": "O_CLERK",
		"type": "STRING"
	},
	{
		"description": "order ship priority",
		"mode": "REQUIRED",
		"name": "O_SHIPPRIORITY",
		"type": "INTEGER"
	},
	{
		"description": "order comment",
		"mode": "REQUIRED",
		"name": "O_COMMENT",
		"type": "STRING"
	}
]
EOF
bq load --source_format=CSV --field_delimiter='|' ORDERTBL gs://mcg-tdc2/tpch/30gb/ordertbl* ./ordertbl.json
```

```bash
cat > lineitem.json <<EOF
[
	{
		"description": "order nbr",
		"mode": "REQUIRED",
		"name": "L_ORDERKEY",
		"type": "INTEGER"
	},
	{
		"description": "part nbr",
		"mode": "REQUIRED",
		"name": "L_PARTKEY",
		"type": "INTEGER"
	},
	{
		"description": "supplier nbr",
		"mode": "REQUIRED",
		"name": "L_SUPPKEY",
		"type": "INTEGER"
	},
	{
		"description": "linenmber",
		"mode": "REQUIRED",
		"name": "L_LINENUMBER",
		"type": "INTEGER"
	},
	{
		"description": "quantity",
		"mode": "REQUIRED",
		"name": "L_QUANTITY",
		"type": "NUMERIC"
	},
	{
		"description": "extended price",
		"mode": "REQUIRED",
		"name": "L_EXTENDEDPRICE",
		"type": "NUMERIC"
	},
	{
		"description": "discount",
		"mode": "REQUIRED",
		"name": "L_DISCOUNT",
		"type": "NUMERIC"
	},
	{
		"description": "tax",
		"mode": "REQUIRED",
		"name": "L_TAX",
		"type": "NUMERIC"
	},
	{
		"description": "returnflag",
		"mode": "REQUIRED",
		"name": "L_RETURNFLAG",
		"type": "STRING"
	},
	{
		"description": "status",
		"mode": "REQUIRED",
		"name": "L_LINESTATUS",
		"type": "STRING"
	},
	{
		"description": "ship date",
		"mode": "REQUIRED",
		"name": "L_SHIPDATE",
		"type": "DATE"
	},
	{
		"description": "commit date",
		"mode": "REQUIRED",
		"name": "L_COMMITDATE",
		"type": "DATE"
	},
	{
		"description": "receipt date",
		"mode": "REQUIRED",
		"name": "L_RECEIPTDATE",
		"type": "DATE"
	},
	{
		"description": "sship instructions",
		"mode": "REQUIRED",
		"name": "L_SHIPINSTRUCT",
		"type": "STRING"
	},
	{
		"description": "ship mode",
		"mode": "REQUIRED",
		"name": "L_SHIPMODE",
		"type": "STRING"
	},
	{
		"description": "lineitem comment",
		"mode": "REQUIRED",
		"name": "L_COMMENT",
		"type": "STRING"
	}
]
EOF
bq load --source_format=CSV --field_delimiter='|' LINEITEM gs://mcg-tdc2/tpch/30gb/lineitem* ./lineitem.json
```

```sql
select 'customer' entity, count(*) from tdalpha.CUSTOMER union all
select 'lineitem' entity, count(*) from tdalpha.LINEITEM union all
select 'nation' entity, count(*) from tdalpha.NATION union all
select 'ordertbl' entity, count(*) from tdalpha.ORDERTBL union all
select 'parttbl' entity, count(*) from tdalpha.PARTTBL union all
select 'partsupp' entity, count(*) from tdalpha.PARTSUPP union all
select 'region' entity, count(*) from tdalpha.REGION union all
select 'supplier' entity, count(*) from tdalpha.SUPPLIER order by 2;
```

</details>

### Running Queries (TPC-H 30GB Stream)

<details>

```sql

SELECT /* tdb=TPCH_Q06 */
        round(cast(SUM(L_EXTENDEDPRICE*L_DISCOUNT) as NUMERIC ),2) AS REVENUE
FROM 
        tdalpha.LINEITEM
WHERE
        L_SHIPDATE >= CAST('1993-01-01' AS DATE)
        AND L_SHIPDATE <  DATE_ADD(CAST('1993-01-01' AS DATE) , INTERVAL 1 YEAR)
        AND L_DISCOUNT BETWEEN 0.02 - 0.01 AND 0.02 + 0.01
        AND L_QUANTITY < 25;


SELECT /* tdb=TPCH_Q19 */
        ROUND(CAST(SUM(L_EXTENDEDPRICE* (1 - L_DISCOUNT)) AS NUMERIC), 2) AS REVENUE
 FROM
        tdalpha.LINEITEM,
        tdalpha.PARTTBL
WHERE
    (
		P_PARTKEY = L_PARTKEY
		AND P_BRAND = 'BRAND#11'
		AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		AND L_QUANTITY >= 4 AND L_QUANTITY <= 4 + 10
		AND P_SIZE BETWEEN 1 AND 5
		AND L_SHIPMODE IN ('AIR', 'AIR REG')
		AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
	)
	OR
	(
		P_PARTKEY = L_PARTKEY
		AND P_BRAND = 'BRAND#33'
		AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		AND L_QUANTITY >= 19 AND L_QUANTITY <= 19 + 10
		AND P_SIZE BETWEEN 1 AND 10
		AND L_SHIPMODE IN ('AIR', 'AIR REG')
		AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
	)
	OR
	(
		P_PARTKEY = L_PARTKEY
		AND P_BRAND = 'BRAND#35'
		AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		AND L_QUANTITY >= 25 AND L_QUANTITY <= 25 + 10
		AND P_SIZE BETWEEN 1 AND 15
		AND L_SHIPMODE IN ('AIR', 'AIR REG')
		AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
	);


SELECT /* tdb=TPCH_Q13 */
        C_COUNT, COUNT(*) AS CUSTDIST
FROM   (
        SELECT
                C_CUSTKEY AS C_CUSTKEY,
                COUNT(O_ORDERKEY) AS C_COUNT
        FROM
                tdalpha.CUSTOMER LEFT OUTER JOIN tdalpha.ORDERTBL ON
                        C_CUSTKEY = O_CUSTKEY
                        AND O_COMMENT NOT LIKE '%unusual%accounts%'
        GROUP BY
                C_CUSTKEY
        ) AS C_ORDERS
GROUP BY
        C_COUNT
ORDER BY
        CUSTDIST DESC,
        C_COUNT DESC;


SELECT /* tdb=TPCH_Q21 */
        S_NAME,
        COUNT(*) AS NUMWAIT
FROM
        tdalpha.SUPPLIER,
        tdalpha.LINEITEM L1,
        tdalpha.ORDERTBL,
        tdalpha.NATION
WHERE
        S_SUPPKEY = L1.L_SUPPKEY
        AND O_ORDERKEY = L1.L_ORDERKEY
        AND O_ORDERSTATUS='F'
        AND L1.L_RECEIPTDATE > L1.L_COMMITDATE
        AND EXISTS (
                SELECT
                        *
                FROM 
                        tdalpha.LINEITEM L2
                WHERE
                        L2.L_ORDERKEY = L1.L_ORDERKEY
                        AND L2.L_SUPPKEY <> L1.L_SUPPKEY
        )
        AND NOT EXISTS (
                SELECT
                        *
                FROM 
                        tdalpha.LINEITEM L3
                WHERE
                        L3.L_ORDERKEY = L1.L_ORDERKEY
                        AND L3.L_SUPPKEY <> L1.L_SUPPKEY
                        AND L3.L_RECEIPTDATE > L3.L_COMMITDATE
        )
        AND S_NATIONKEY = N_NATIONKEY
        AND N_NAME = 'IRAN'
GROUP BY
        S_NAME
ORDER BY
        NUMWAIT DESC,
        S_NAME
LIMIT 100;


SELECT /* tdb=TPCH_Q14 */
        100.00*cast(SUM(CASE 
                WHEN P_TYPE LIKE 'PROMO%'
                THEN L_EXTENDEDPRICE*(1-L_DISCOUNT) 
                ELSE 0 
        END)as NUMERIC) /round(cast( SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) as NUMERIC),2)  AS PROMO_REVENUE
FROM 
        tdalpha.LINEITEM,
        tdalpha.PARTTBL
WHERE
        L_PARTKEY = P_PARTKEY
        AND L_SHIPDATE >= CAST('1995-11-01' AS DATE)
        AND L_SHIPDATE < DATE_ADD (CAST('1995-11-01' AS DATE) , INTERVAL 1 MONTH);


SELECT /* tdb=TPCH_Q03 */
        L_ORDERKEY,
        ROUND(CAST(SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS NUMERIC), 2) AS REVENUE,
        O_ORDERDATE,
        O_SHIPPRIORITY
FROM  
        tdalpha.CUSTOMER,
        tdalpha.ORDERTBL,
        tdalpha.LINEITEM
WHERE
        C_MKTSEGMENT  = 'FURNITURE'
        AND C_CUSTKEY    = O_CUSTKEY
        AND L_ORDERKEY   = O_ORDERKEY
        AND O_ORDERDATE  < cast('1995-03-13' as date)
        AND L_SHIPDATE   > cast('1995-03-13' as date)
GROUP BY
        L_ORDERKEY,
        O_ORDERDATE,
        O_SHIPPRIORITY
ORDER BY
        REVENUE DESC,
        O_ORDERDATE
LIMIT 10;


SELECT /* tdb=TPCH_Q20 */
        S_NAME,
        S_ADDRESS
FROM
        tdalpha.SUPPLIER,
        tdalpha.NATION
WHERE
        S_SUPPKEY IN (
                SELECT
                        PS_SUPPKEY
                FROM
                        tdalpha.PARTSUPP
                WHERE
                        PS_PARTKEY IN (
                                SELECT
                                        P_PARTKEY
                                FROM
                                        tdalpha.PARTTBL
                                WHERE
                                        P_NAME LIKE 'blush%'
                        )
                AND  PS_AVAILQTY > (
                        SELECT
                                0.5 * SUM(L_QUANTITY)
                        FROM
                                tdalpha.LINEITEM
                        WHERE
                                L_PARTKEY = PS_PARTKEY
                                AND  L_SUPPKEY = PS_SUPPKEY
                                AND  L_SHIPDATE >= CAST('1995-01-01' AS DATE)
                                AND  L_SHIPDATE <  DATE_ADD( CAST('1995-01-01' AS DATE) , INTERVAL 1 YEAR)
                )
        )
        AND S_NATIONKEY = N_NATIONKEY
        AND N_NAME = 'KENYA'
ORDER BY
        S_NAME;


SELECT /* tdb=TPCH_Q02 */
        S_ACCTBAL,
        S_NAME,
        N_NAME,
        P_PARTKEY,
        P_MFGR,
        S_ADDRESS,
        S_PHONE,
        S_COMMENT
FROM 
        tdalpha.PARTTBL,
        tdalpha.SUPPLIER,
        tdalpha.PARTSUPP,
        tdalpha.NATION,
        tdalpha.REGION
WHERE
        P_PARTKEY = PS_PARTKEY
        AND S_SUPPKEY = PS_SUPPKEY
        AND P_SIZE = 32
        AND P_TYPE LIKE '%NICKEL'
        AND S_NATIONKEY = N_NATIONKEY
        AND N_REGIONKEY = R_REGIONKEY
        AND R_NAME = 'ASIA'
        AND PS_SUPPLYCOST = (
                SELECT 
                        MIN(PS_SUPPLYCOST)
                FROM 
 
        tdalpha.SUPPLIER,
        tdalpha.PARTSUPP,
        tdalpha.NATION,
        tdalpha.REGION
                WHERE
                        P_PARTKEY = PS_PARTKEY
                        AND S_SUPPKEY = PS_SUPPKEY
                        AND S_NATIONKEY = N_NATIONKEY
                        AND N_REGIONKEY = R_REGIONKEY
                        AND R_NAME = 'ASIA'
        )
ORDER BY 
        S_ACCTBAL DESC,
        N_NAME,
        S_NAME,
        P_PARTKEY
LIMIT 100;


SELECT /* tdb=TPCH_Q12 */
        L_SHIPMODE,
        SUM(CASE
                WHEN O_ORDERPRIORITY = '1-URGENT'
                        OR O_ORDERPRIORITY = '2-HIGH'
                THEN 1 
                ELSE 0 
        END) AS HIGH_LINE_COUNT,
        SUM(CASE 
                WHEN O_ORDERPRIORITY <> '1-URGENT'
                        AND O_ORDERPRIORITY <> '2-HIGH'
                THEN 1 
                ELSE 0 
        END) AS LOW_LINE_COUNT
FROM 
       tdalpha.ORDERTBL,
       tdalpha.LINEITEM
WHERE 
        O_ORDERKEY = L_ORDERKEY
        AND L_SHIPMODE IN ('TRUCK', 'AIR')
        AND L_COMMITDATE < L_RECEIPTDATE
        AND L_SHIPDATE < L_COMMITDATE
        AND L_RECEIPTDATE >= CAST('1994-01-01' AS DATE)
        AND L_RECEIPTDATE < DATE_ADD( CAST('1994-01-01' AS DATE) , INTERVAL 1 YEAR)
GROUP BY 
        L_SHIPMODE
ORDER BY 
        L_SHIPMODE;


SELECT /* tdb=TPCH_Q01 */
        L_RETURNFLAG, 
        L_LINESTATUS,
        ROUND(CAST(SUM(L_QUANTITY) AS NUMERIC), 2) AS SUM_QTY,
        ROUND(CAST(SUM(L_EXTENDEDPRICE) AS NUMERIC), 2) AS SUM_BASE_PRICE,
        ROUND(CAST(SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS NUMERIC), 2) AS SUM_DISC_PRICE,
		ROUND(CAST(SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)*(1+L_TAX)) AS NUMERIC), 2) AS SUM_CHARGE,
        ROUND(CAST(AVG(L_QUANTITY) AS NUMERIC), 2) AS AVG_QTY,
        ROUND(CAST(AVG(L_EXTENDEDPRICE) AS NUMERIC), 2) AS AVG_PRICE,
        ROUND(CAST(AVG(L_DISCOUNT) AS NUMERIC), 2) AS AVG_DISC,
        ROUND(CAST(COUNT(*) AS NUMERIC), 0) AS COUNT_ORDER
FROM 
        tdalpha.LINEITEM
WHERE
        L_SHIPDATE <= DATE_SUB(DATE '1998-12-01', INTERVAL 84 DAY)
GROUP BY
        L_RETURNFLAG,
        L_LINESTATUS
ORDER BY
        L_RETURNFLAG,
        L_LINESTATUS;


SELECT /* tdb=TPCH_Q10 */
        C_CUSTKEY,
        C_NAME,
        ROUND(CAST(SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS NUMERIC),2) AS REVENUE,
        C_ACCTBAL,
        N_NAME,
        C_ADDRESS,
        C_PHONE, 
        C_COMMENT
FROM 
        tdalpha.CUSTOMER,
        tdalpha.ORDERTBL,
        tdalpha.LINEITEM,
        tdalpha.NATION
WHERE
        C_CUSTKEY = O_CUSTKEY
        AND L_ORDERKEY = O_ORDERKEY
        AND O_ORDERDATE >= CAST('1994-03-01' AS DATE)
        AND O_ORDERDATE < DATE_ADD( CAST('1994-03-01' AS DATE) , INTERVAL 3 MONTH)
        AND L_RETURNFLAG = 'R'
        AND C_NATIONKEY = N_NATIONKEY
GROUP BY 
        C_CUSTKEY,
        C_NAME,
        C_ACCTBAL,
        C_PHONE,
        N_NAME,
        C_ADDRESS,
        C_COMMENT
ORDER BY 
        REVENUE DESC
LIMIT 20;


SELECT /* tdb=TPCH_Q05 */
        N_NAME,
        ROUND(CAST(SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS NUMERIC), 2) AS REVENUE
FROM 
        tdalpha.CUSTOMER,
        tdalpha.ORDERTBL,
        tdalpha.LINEITEM,
        tdalpha.SUPPLIER,
        tdalpha.NATION,
        tdalpha.REGION
WHERE
        C_CUSTKEY = O_CUSTKEY
        AND L_ORDERKEY = O_ORDERKEY
        AND L_SUPPKEY = S_SUPPKEY
        AND C_NATIONKEY = S_NATIONKEY
        AND S_NATIONKEY = N_NATIONKEY
        AND N_REGIONKEY = R_REGIONKEY
        AND R_NAME = 'AFRICA'
        AND O_ORDERDATE >= CAST('1993-01-01' AS DATE)
        AND O_ORDERDATE < DATE_ADD(CAST('1993-01-01' AS DATE), INTERVAL 1 YEAR)
GROUP BY
        N_NAME
ORDER BY
        REVENUE DESC;


SELECT /* tdb=TPCH_Q22 */
        CNTRYCODE,
        COUNT(*) AS NUMCUST,
        SUM(C_ACCTBAL) AS TOTACCTBAL
FROM    (
        SELECT
                SUBSTR (C_PHONE,1,2) AS CNTRYCODE,
                C_ACCTBAL
        FROM
                tdalpha.CUSTOMER
        WHERE
                SUBSTR (C_PHONE,1 , 2) IN
                       ('24', '22', '34', '21', '32', '25', '30')
                AND C_ACCTBAL > (
                        SELECT
                                AVG(C_ACCTBAL)
                        FROM
                                tdalpha.CUSTOMER
                        WHERE
                                C_ACCTBAL > 0.00
                                AND SUBSTR (C_PHONE , 1 , 2) IN
                                          ('24', '22', '34', '21', '32', '25', '30')
                )
                AND NOT EXISTS (
                        SELECT
                                *
                        FROM
                                tdalpha.ORDERTBL
                        WHERE
                                O_CUSTKEY=C_CUSTKEY
                )
        ) AS CUSTSALE
GROUP BY
        CNTRYCODE
ORDER BY
        CNTRYCODE;


SELECT /* tdb=TPCH_Q04 */
        O_ORDERPRIORITY,
        COUNT(*) AS ORDER_COUNT
FROM
        tdalpha.ORDERTBL
WHERE
        O_ORDERDATE >=  CAST('1994-12-01' AS DATE)
        AND O_ORDERDATE < DATE_ADD(CAST('1994-12-01' AS DATE) , INTERVAL 3 MONTH)
        AND EXISTS (
                SELECT
                        *
                FROM 
                        tdalpha.LINEITEM
                WHERE
                        L_ORDERKEY = O_ORDERKEY
                        AND L_COMMITDATE < L_RECEIPTDATE
        )
GROUP BY
        O_ORDERPRIORITY
ORDER BY
        O_ORDERPRIORITY;


SELECT /* tdb=TPCH_Q11 */
        PS_PARTKEY,
        ROUND(CAST(SUM(PS_SUPPLYCOST * PS_AVAILQTY) AS NUMERIC), 2) AS VALUE
FROM 
        tdalpha.PARTSUPP,
        tdalpha.SUPPLIER,
        tdalpha.NATION
WHERE
        PS_SUPPKEY = S_SUPPKEY
        AND S_NATIONKEY = N_NATIONKEY
        AND N_NAME ='CHINA'
GROUP BY
        PS_PARTKEY HAVING 
                SUM(PS_SUPPLYCOST * PS_AVAILQTY) > (
                        SELECT
                                SUM(PS_SUPPLYCOST * PS_AVAILQTY) * 0.000003333333333
                        FROM
                                tdalpha.PARTSUPP,
                                tdalpha.SUPPLIER,
                                tdalpha.NATION
                        WHERE
                                PS_SUPPKEY = S_SUPPKEY
                                AND S_NATIONKEY = N_NATIONKEY
                                AND N_NAME = 'CHINA'
                )
ORDER BY
        VALUE DESC;


SELECT /* tdb=TPCH_Q16 */
        P_BRAND,
        P_TYPE,
        P_SIZE,
        COUNT(DISTINCT PS_SUPPKEY) AS SUPPLIER_CNT
FROM 
       tdalpha.PARTSUPP,
       tdalpha.PARTTBL
WHERE
        P_PARTKEY = PS_PARTKEY
        AND P_BRAND <> 'Brand#25'
        AND P_TYPE NOT LIKE 'MEDIUM POLISHED%'
        AND P_SIZE IN (2, 7, 24, 30, 27, 50, 23, 45)
        AND PS_SUPPKEY NOT IN (
                SELECT 
                        S_SUPPKEY 
                FROM 
                       tdalpha.SUPPLIER
                WHERE 
                        S_COMMENT LIKE '%Customer%Complaints%'
        )
GROUP BY
        P_BRAND,
        P_TYPE,
        P_SIZE
ORDER BY
        SUPPLIER_CNT DESC,
        P_BRAND,
        P_TYPE,
        P_SIZE;


SELECT /* tdb=TPCH_Q07 */
        N1.N_NAME  AS SUPP_NATION,
        N2.N_NAME  AS CUST_NATION,
        EXTRACT(YEAR FROM L_SHIPDATE) AS YEAR,
		ROUND(CAST(SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS NUMERIC), 2) AS REVENUE
FROM 
        tdalpha.SUPPLIER,
        tdalpha.LINEITEM,
        tdalpha.ORDERTBL,
        tdalpha.CUSTOMER,
        tdalpha.NATION N1,
        tdalpha.NATION N2
WHERE
        S_SUPPKEY  = L_SUPPKEY
        AND O_ORDERKEY = L_ORDERKEY
        AND C_CUSTKEY = O_CUSTKEY
        AND S_NATIONKEY = N1.N_NATIONKEY
        AND C_NATIONKEY = N2.N_NATIONKEY
        AND (
                (N1.N_NAME = 'ARGENTINA' AND N2.N_NAME = 'PERU')
                OR (N1.N_NAME = 'PERU' AND N2.N_NAME = 'ARGENTINA')
        )
        AND  L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
GROUP BY
        SUPP_NATION,
        CUST_NATION,
        YEAR
ORDER BY
        SUPP_NATION,
        CUST_NATION,
        YEAR;


SELECT /* tdb=TPCH_Q18 */
        C_NAME,
        C_CUSTKEY,
        O_ORDERKEY,
        O_ORDERDATE,
        O_TOTALPRICE,
        round(cast(SUM(L_QUANTITY) as  NUMERIC),2) AS SUM_QTY
FROM
       tdalpha.CUSTOMER,
       tdalpha.ORDERTBL,
       tdalpha.LINEITEM
WHERE
        O_ORDERKEY IN (
                SELECT
                        L_ORDERKEY
                FROM
                       tdalpha.LINEITEM
                GROUP BY
                        L_ORDERKEY HAVING
                                SUM(L_QUANTITY) > 313
        )
        AND C_CUSTKEY = O_CUSTKEY
        AND O_ORDERKEY = L_ORDERKEY
GROUP BY
        C_NAME,
        C_CUSTKEY,
        O_ORDERKEY,
        O_ORDERDATE,
        O_TOTALPRICE
ORDER BY
        O_TOTALPRICE DESC,
        O_ORDERDATE
LIMIT 100;


SELECT /* tdb=TPCH_Q08 */
        EXTRACT(YEAR FROM O_ORDERDATE) AS YEAR,
        SUM(CASE
                WHEN N2.N_NAME = 'PERU' 
                THEN cast (L_EXTENDEDPRICE*(1-L_DISCOUNT) as  NUMERIC)
                ELSE 0
        END) / round(cast(SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) as  NUMERIC),2)  AS MKT_SHARE
FROM 
        tdalpha.PARTTBL,
        tdalpha.SUPPLIER,
        tdalpha.LINEITEM,
        tdalpha.ORDERTBL,
        tdalpha.CUSTOMER,
        tdalpha.NATION as N1,
        tdalpha.NATION as N2,
        tdalpha.REGION
WHERE
        P_PARTKEY = L_PARTKEY
        AND S_SUPPKEY = L_SUPPKEY
        AND L_ORDERKEY = O_ORDERKEY
        AND O_CUSTKEY = C_CUSTKEY
        AND C_NATIONKEY = N1.N_NATIONKEY
        AND N1.N_REGIONKEY = R_REGIONKEY
        AND R_NAME = 'AMERICA'
        AND S_NATIONKEY = N2.N_NATIONKEY
        AND O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
        AND P_TYPE = 'SMALL BRUSHED BRASS'
GROUP BY
        YEAR
ORDER BY
        YEAR;


SELECT /* tdb=TPCH_Q09 */
        N_NAME AS NATION,
        EXTRACT(YEAR FROM O_ORDERDATE) AS YEAR,
        ROUND(CAST(SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)-PS_SUPPLYCOST*L_QUANTITY) AS NUMERIC), 2) AS SUM_PROFIT
FROM
       tdalpha.PARTTBL,
       tdalpha.SUPPLIER,
       tdalpha.LINEITEM,
       tdalpha.PARTSUPP,
       tdalpha.ORDERTBL,
       tdalpha.NATION
WHERE
        S_SUPPKEY = L_SUPPKEY
        AND PS_SUPPKEY = L_SUPPKEY
        AND PS_PARTKEY = L_PARTKEY
        AND P_PARTKEY = L_PARTKEY
        AND O_ORDERKEY = L_ORDERKEY
        AND S_NATIONKEY = N_NATIONKEY
        AND P_NAME LIKE '%frosted%'
GROUP BY
        NATION,
        YEAR
ORDER BY
        NATION,
        YEAR DESC;


SELECT /* tdb=TPCH_Q17 */
        SUM(L_EXTENDEDPRICE) / 7.0 AS AVG_YEARLY
FROM
        tdalpha.LINEITEM,
        tdalpha.PARTTBL
WHERE
        P_PARTKEY = L_PARTKEY
        AND P_BRAND = 'Brand#53'
        AND P_CONTAINER = 'SM BOX'
        AND L_QUANTITY < (
                SELECT
                        0.2 * AVG(L_QUANTITY)
                FROM
                        tdalpha.LINEITEM
                WHERE
                        L_PARTKEY = P_PARTKEY
        );


WITH /* tdb=TPCH_Q15 */ REVENUE0 AS 
(
	SELECT
			L_SUPPKEY AS SUPPLIER_NO,
			ROUND(CAST(SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS NUMERIC), 2) AS TOTAL_REVENUE
	FROM
			tdalpha.LINEITEM
	WHERE
			L_SHIPDATE >= CAST('1994-02-01' AS DATE)
			AND L_SHIPDATE < DATE_ADD(CAST('1994-02-01' AS DATE), INTERVAL 3 MONTH)
	GROUP BY
			L_SUPPKEY
)
SELECT
        S_SUPPKEY,
        S_NAME,
        S_ADDRESS,
        S_PHONE,
        TOTAL_REVENUE
FROM 
        tdalpha.SUPPLIER,
        REVENUE0
WHERE
        S_SUPPKEY = SUPPLIER_NO
        AND TOTAL_REVENUE = (
                SELECT 
                        MAX(TOTAL_REVENUE)
                FROM 
                        REVENUE0
        )
ORDER BY 
        S_SUPPKEY;
```

</details>

### Query History

<details>

```sql
bq show --format prettyjson -j [job_id]

bq show --format prettyjson -j [job_id] | jq -r '.configuration.query.query,.statistics.reservation_id,.statistics.query.biEngineStatistics,.statistics.finalExecutionDurationMs,.statistics.query.totalSlotMs,.statistics.query.totalBytesProcessed'
```

</details>

## Performance Features

### Designing Tables (Clustering and Partitioning)

<details>

```sql
create or replace table tdalpha_c.NATION copy tdalpha.NATION;
create or replace table tdalpha_c.REGION copy tdalpha.REGION;
create or replace table tdalpha_c.SUPPLIER copy tdalpha.SUPPLIER;
create or replace table tdalpha_c.PARTTBL copy tdalpha.PARTTBL;
create or replace table tdalpha_c.PARTSUPP copy tdalpha.PARTSUPP;
create or replace table tdalpha_c.CUSTOMER copy tdalpha.CUSTOMER;
create or replace table tdalpha_c.ORDERTBL cluster by (o_orderdate) as select * from tdalpha.ORDERTBL;
create or replace table tdalpha_c.LINEITEM cluster by (l_shipdate) as select * from tdalpha.LINEITEM;


create or replace table tdalpha_p.NATION copy tdalpha.NATION;
create or replace table tdalpha_p.REGION copy tdalpha.REGION;
create or replace table tdalpha_p.SUPPLIER copy tdalpha.SUPPLIER;
create or replace table tdalpha_p.PARTTBL copy tdalpha.PARTTBL;
create or replace table tdalpha_p.PARTSUPP copy tdalpha.PARTSUPP;
create or replace table tdalpha_p.CUSTOMER copy tdalpha.CUSTOMER;
create or replace table tdalpha_p.ORDERTBL partition by (o_orderdate) as select * from tdalpha.ORDERTBL;
create or replace table tdalpha_p.LINEITEM partition by (l_shipdate) as select * from tdalpha.LINEITEM;
```

</details>

### Results Cache

<details>

<code>"jdbc:bigquery://https://www.googleapis.com/bigquery/v2;ProjectId=striking-domain-287814;OAuthType=0;OAuthServiceAcctEmail=mcg-tdc@striking-domain-287814.iam.gserviceaccount.com;OAuthPvtKeyPath=auth.json;Timeout=5400;<b>useQueryCache=0</b>;DefaultDataset=tdalpha;"</code>

</details>

### Transparent Materialized Views

<details>

```sql
create materialized view mv_q18
as
select
   l_orderkey, 
   sum(l_quantity) sum_lq,
   count(l_quantity) count_lq,
   count(*) count_grp
from
   lineitem
group by
   l_orderkey;

/* TPC_H  Query 18 - Large Volume Customer */
select /* tdb=TPCH_Q18 */
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
from
    customer,
    ordertbl,
    lineitem
where
    o_orderkey in (
        select
            l_orderkey
        from
            lineitem
        group by
            l_orderkey having
                sum(l_quantity) > 300
    )
    and c_custkey = o_custkey
    and o_orderkey = l_orderkey
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
order by
    o_totalprice desc,
    o_orderdate
limit 100;
```

</details>

### BI Engine

<details>

```sql


```

</details>

## Usability Features

### External Tables

<details>

```sql
create or replace schema ext;

create or replace external table ext.customer
     (
      c_custkey integer as (value:c1::int),
      c_name varchar(25) as (value:c2::varchar),
      c_address varchar(40) as (value:c3::varchar),
      c_nationkey integer as (value:c4::int),
      c_phone varchar(15) as (value:c5::varchar),
      c_acctbal decimal(15,2) as (value:c6::number),
      c_mktsegment char(10) as (value:c7::varchar),
      c_comment varchar(117) as (value:c8::varchar)
     )
 with 
    location = @public.abench_s3_stage
    file_format = public.abench_filefmt
    pattern = '.*customer.*';
 
 select * from tdalpha.PUBLIC.customer;
 select * from tdalpha.EXT.customer;
 ```

</details>

### Dynamic Data Masking

<details>

```sql
create or replace masking policy name_mask as (val string) returns string ->
  case
    when current_role() in ('SECURITYADMIN') then val
    else '*********'
  end;
  
  alter table customer modify column c_name set masking policy name_mask;
  
  select * from tdalpha.CUSTOMER limit 100;
  
  alter masking policy name_mask set body ->
  case
    when current_role() in ('SYSADMIN') then val
    else '*********'
  end;

select * from tdalpha.CUSTOMER limit 100;
```

</details>

### Time Travel

<details>

```sql
show tables like 'ordertbl';

select count(*) from tdalpha.ORDERTBL;

delete from tdalpha.ORDERTBL where o_orderkey < 1000;

select count(*) from tdalpha.ORDERTBL;

select count(*) from tdalpha.ORDERTBL at(offset => -60*5);

create table restored_ordertbl clone ordertbl
  at(offset => -60*5);

select count(*) from tdalpha.RESTORED_ORDERTBL;

drop table ordertbl;

alter table restored_ordertbl rename to ordertbl;

select count(*) from tdalpha.ORDERTBL;
```

</details>

### Snowsight Dashboards

<details>

```sql
/* TPC_H  Query 3 - Shipping Priority */
select /* tdb=TPCH_Q03 */
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from
    customer,
    ordertbl,
    lineitem
where
    trim(c_mktsegment) = 'BUILDING'
    and c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate < '1995-03-15'
    and l_shipdate > '1995-03-15'
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate
limit 100;

/* TPC_H  Query 18 - Large Volume Customer */
select /* tdb=TPCH_Q18 */
    c_name,
    sum(o_totalprice),
    sum(l_quantity)
from
    customer,
    ordertbl,
    lineitem
where
    o_orderkey in (
        select
            l_orderkey
        from
            lineitem
        group by
            l_orderkey having
                sum(l_quantity) > 300
    )
    and c_custkey = o_custkey
    and o_orderkey = l_orderkey
group by
    c_name
order by
    sum(o_totalprice) desc
limit 100;

/* TPC_H  Query 9 - Product Type Profit Measure */
select /* tdb=TPCH_Q09 */
    nation,
    o_year,
    sum(amount) as sum_profit
from (
    select
        n_name as nation,
        extract(year from tdalpha.O_ORDERDATE) as o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    from
        parttbl,
        supplier,
        lineitem,
        partsupp,
        ordertbl,
        nation
    where
        s_suppkey = l_suppkey
        and ps_suppkey = l_suppkey
        and ps_partkey = l_partkey
        and p_partkey = l_partkey
        and o_orderkey = l_orderkey
        and s_nationkey = n_nationkey
        and p_name like '%green%'
        and n_nationkey < 5
    ) as profit
where
    o_year < 1998
group by
    nation,
    o_year
order by
    nation,
    o_year desc;
```

</details>

### Snowpark

<details>

```bash
wget https://repo.anaconda.com/archive/Anaconda3-2022.10-Linux-x86_64.sh
bash Anaconda3-2022.10-Linux-x86_64.sh
conda create --name py38_env --override-channels -c https://repo.anaconda.com/pkgs/snowflake python=3.8 numpy pandas
conda activate py38_env
pip install notebook
pip install snowflake-snowpark-python
jupyter notebook
```
```python
from tdalpha.SNOWFLAKE.snowpark import Session
connection_parameters = {
    "account": "of53892.us-east-1",
    "user": "tdalpha",
    "password": "17095ViaDelCampo",
    "role": "sysadmin",
    "warehouse": "tdalpha",
    "database": "tdalpha",
    "schema": "public",
  }  

session = Session.builder.configs(connection_parameters).create()  

session.sql("select count(*) from tdalpha.CUSTOMER").collect()
```

</details>
