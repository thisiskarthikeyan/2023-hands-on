# Competitive Hands-On Training - 2023 Q1
# Synapse

## Login
https://portal.azure.com/

Username
```
tdalpha@jakeadolezalgmail.onmicrosoft.com
```

Password
```
17095ViaDelCampo!
```



## Basic Functionality

### Serverless Data Exploration

<details>

```sql
select
    top 100
    *
from
    openrowset(
        bulk 'https://tdalpha.blob.core.windows.net/tpch/tpch/30gb_unload/customer_*.csv',
        format='csv',
        parser_version='2.0',
        fieldterminator='|'
    )
    with (
        c_custkey bigint,
        c_name varchar(25),
        c_address varchar(40),
        c_nationkey integer,
        c_phone char(15),
        c_acctbal decimal(15,2),
        c_mktsegment char(10),
        c_comment varchar(117)
    ) as customer;
```

</details>

### Create Warehouse, Database, and Tables

<details>

```sql
create table NATION (
	n_nationkey integer not null,
	n_name char(25) not null,
	n_regionkey integer not null,
	n_comment varchar(152)
) with (distribution = replicate)
;
create table REGION (
	r_regionkey integer not null,
	r_name char(25) not null,
	r_comment varchar(152)
) with (distribution = replicate)
;
create table SUPPLIER (
	s_suppkey integer not null,
	s_name char(25) not null,
	s_address varchar(40) not null,
	s_nationkey integer not null,
	s_phone char(15) not null,
	s_acctbal decimal(15,2) not null,
	s_comment varchar(101) not null
) with (distribution = replicate)
;
create table PARTSUPP (
	ps_partkey integer not null,
	ps_suppkey integer not null,
	ps_availqty integer not null,
	ps_supplycost decimal(15,2) not null,
	ps_comment varchar(199) not null
) with (distribution = hash(ps_partkey)
     ,clustered columnstore index order (ps_partkey, ps_suppkey))
;
create table PARTTBL (
	p_partkey integer not null,
	p_name varchar(55) not null,
	p_mfgr char(25) not null,
	p_brand char(10) not null,
	p_type varchar(25) not null,
	p_size integer not null,
	p_container char(10) not null,
	p_retailprice decimal(15,2) not null,
	p_comment varchar(23) not null
) with (distribution = hash(p_partkey)
     ,clustered columnstore index order (p_partkey))
;
create table CUSTOMER (
	c_custkey integer not null,
	c_name varchar(25) not null,
	c_address varchar(40) not null,
	c_nationkey integer not null,
	c_phone char(15) not null,
	c_acctbal decimal(15,2) not null,
	c_mktsegment char(10) not null,
	c_comment varchar(117) not null
) with (distribution = hash(c_custkey)
     ,clustered columnstore index order (c_custkey))
;
create table ORDERTBL (
	o_orderkey bigint not null,
	o_custkey integer not null,
	o_orderstatus char(1) not null,
	o_totalprice decimal(15,2) not null,
	o_orderdate date not null,
	o_orderpriority char(15) not null, 
	o_clerk char(15) not null, 
	o_shippriority integer not null,
	o_comment varchar(79) not null
) with (distribution = hash(o_orderkey)
     ,clustered columnstore index order (o_orderkey, o_orderdate))
;
create table LINEITEM (
	l_orderkey bigint not null,
	l_partkey integer not null,
	l_suppkey integer not null,
	l_linenumber integer not null,
	l_quantity decimal(15,2) not null,
	l_extendedprice decimal(15,2) not null,
	l_discount decimal(15,2) not null,
	l_tax decimal(15,2) not null,
	l_returnflag char(1) not null,
	l_linestatus char(1) not null,
	l_shipdate date not null,
	l_commitdate date not null,
	l_receiptdate date not null,
	l_shipinstruct char(25) not null,
	l_shipmode char(10) not null,
	l_comment varchar(44) not null
) with (distribution = hash(l_orderkey)
     ,clustered columnstore index order (l_orderkey, l_shipdate))
;
```

</details>

### Loading Data (Directly from Blob Storage)

<details>

```sql
copy into NATION from 'https://tdalpha.blob.core.windows.net/tpch/tpch/30gb_unload/nation_*.csv'
with (fieldterminator='|');

copy into REGION from 'https://tdalpha.blob.core.windows.net/tpch/tpch/30gb_unload/region_*.csv'
with (fieldterminator='|');

copy into SUPPLIER from 'https://tdalpha.blob.core.windows.net/tpch/tpch/30gb_unload/supplier_*.csv'
with (fieldterminator='|');

copy into PARTSUPP from 'https://tdalpha.blob.core.windows.net/tpch/tpch/30gb_unload/partsupp_*.csv'
with (fieldterminator='|');

copy into PARTTBL from 'https://tdalpha.blob.core.windows.net/tpch/tpch/30gb_unload/parttbl_*.csv'
with (fieldterminator='|');

copy into CUSTOMER from 'https://tdalpha.blob.core.windows.net/tpch/tpch/30gb_unload/customer_*.csv'
with (fieldterminator='|');

copy into ORDERTBL from 'https://tdalpha.blob.core.windows.net/tpch/tpch/30gb_unload/ordertbl_*.csv'
with (fieldterminator='|');

copy into LINEITEM from 'https://tdalpha.blob.core.windows.net/tpch/tpch/30gb_unload/lineitem_*.csv'
with (fieldterminator='|');

select 'customer' entity, count(*) from CUSTOMER union all
select 'lineitem' entity, count(*) from LINEITEM union all
select 'nation' entity, count(*) from NATION union all
select 'ordertbl' entity, count(*) from ORDERTBL union all
select 'parttbl' entity, count(*) from PARTTBL union all
select 'partsupp' entity, count(*) from PARTSUPP union all
select 'region' entity, count(*) from REGION union all
select 'supplier' entity, count(*) from SUPPLIER order by 2;
```

</details>

### Running Queries (TPC-H 30GB Stream)

<details>

```sql
SELECT /* tdb=TPCH_Q06 */
  SUM(L_EXTENDEDPRICE*L_DISCOUNT) AS REVENUE
FROM LINEITEM
WHERE L_SHIPDATE >= '1993-01-01' AND L_SHIPDATE < DATEADD(YY, 1, CAST('1993-01-01' AS DATETIME))
AND L_DISCOUNT BETWEEN .02 - 0.01 AND .02 + 0.01 AND L_QUANTITY < 25
;

SELECT /* tdb=TPCH_Q19 */
        SUM(L_EXTENDEDPRICE* (1 - L_DISCOUNT)) AS REVENUE
FROM LINEITEM, PARTTBL
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
	)
;

SELECT /* tdb=TPCH_Q13 */
C_COUNT, COUNT(*) AS CUSTDIST
FROM (SELECT C_CUSTKEY, COUNT(O_ORDERKEY)
FROM CUSTOMER LEFT OUTER JOIN ORDERTBL ON C_CUSTKEY = O_CUSTKEY
AND O_COMMENT NOT LIKE'%%unusual%%accounts%%'
GROUP BY C_CUSTKEY) AS C_ORDERTBL (C_CUSTKEY, C_COUNT)
GROUP BY C_COUNT
ORDER BY CUSTDIST DESC, C_COUNT DESC
;

SELECT /* tdb=TPCH_Q21 */
TOP 100 S_NAME, COUNT(*) AS NUMWAIT
FROM SUPPLIER, LINEITEM L1, ORDERTBL, NATION WHERE S_SUPPKEY = L1.L_SUPPKEY AND
O_ORDERKEY = L1.L_ORDERKEY AND O_ORDERSTATUS = 'F' AND L1.L_RECEIPTDATE> L1.L_COMMITDATE
AND EXISTS (SELECT * FROM LINEITEM L2 WHERE L2.L_ORDERKEY = L1.L_ORDERKEY
AND L2.L_SUPPKEY <> L1.L_SUPPKEY) AND
NOT EXISTS (SELECT * FROM LINEITEM L3 WHERE L3.L_ORDERKEY = L1.L_ORDERKEY AND
L3.L_SUPPKEY <> L1.L_SUPPKEY AND L3.L_RECEIPTDATE > L3.L_COMMITDATE) AND
S_NATIONKEY = N_NATIONKEY AND N_NAME = 'IRAN'
GROUP BY S_NAME
ORDER BY NUMWAIT DESC, S_NAME
;

SELECT /* tdb=TPCH_Q14 */
100.00* SUM(CASE WHEN P_TYPE LIKE 'PROMO%%' THEN L_EXTENDEDPRICE*(1-L_DISCOUNT)
ELSE 0 END) / SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS PROMO_REVENUE
FROM LINEITEM, PARTTBL
WHERE L_PARTKEY = P_PARTKEY AND L_SHIPDATE >= '1995-11-01' AND L_SHIPDATE < DATEADD(MM, 1, '1995-11-01')
;

SELECT /* tdb=TPCH_Q03 */
TOP 10 L_ORDERKEY, SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS REVENUE, O_ORDERDATE, O_SHIPPRIORITY
FROM CUSTOMER, ORDERTBL, LINEITEM
WHERE C_MKTSEGMENT = 'FURNITURE' AND C_CUSTKEY = O_CUSTKEY AND L_ORDERKEY = O_ORDERKEY AND
O_ORDERDATE < '1995-03-13' AND L_SHIPDATE > '1995-03-13'
GROUP BY L_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY
ORDER BY REVENUE DESC, O_ORDERDATE
;

SELECT /* tdb=TPCH_Q20 */
S_NAME, S_ADDRESS FROM SUPPLIER, NATION
WHERE S_SUPPKEY IN (SELECT PS_SUPPKEY FROM PARTSUPP
WHERE PS_PARTKEY in (SELECT P_PARTKEY FROM PARTTBL WHERE P_NAME LIKE 'blush%%') AND
PS_AVAILQTY > (SELECT 0.5*SUM(L_QUANTITY) FROM LINEITEM WHERE L_PARTKEY = PS_PARTKEY AND
L_SUPPKEY = PS_SUPPKEY AND L_SHIPDATE >= '1995-01-01' AND
L_SHIPDATE < DATEADD(YY, 1,'1995-01-01'))) AND S_NATIONKEY = N_NATIONKEY AND N_NAME = 'KENYA'
ORDER BY S_NAME
;

SELECT /* tdb=TPCH_Q02 */
TOP 100 S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT
FROM PARTTBL, SUPPLIER, PARTSUPP, NATION, REGION
WHERE P_PARTKEY = PS_PARTKEY AND S_SUPPKEY = PS_SUPPKEY AND P_SIZE = 32 AND
P_TYPE LIKE '%%NICKEL' AND S_NATIONKEY = N_NATIONKEY AND N_REGIONKEY = R_REGIONKEY AND
R_NAME = 'ASIA' AND
PS_SUPPLYCOST = (SELECT MIN(PS_SUPPLYCOST) FROM PARTSUPP, SUPPLIER, NATION, REGION
WHERE P_PARTKEY = PS_PARTKEY AND S_SUPPKEY = PS_SUPPKEY
AND S_NATIONKEY = N_NATIONKEY AND N_REGIONKEY = R_REGIONKEY AND R_NAME = 'ASIA')
ORDER BY S_ACCTBAL DESC, N_NAME, S_NAME, P_PARTKEY
;

SELECT /* tdb=TPCH_Q12 */
L_SHIPMODE,
SUM(CASE WHEN O_ORDERPRIORITY = '1-URGENT' OR O_ORDERPRIORITY = '2-HIGH' THEN 1 ELSE 0 END) AS HIGH_LINE_COUNT,
SUM(CASE WHEN O_ORDERPRIORITY <> '1-URGENT' AND O_ORDERPRIORITY <> '2-HIGH' THEN 1 ELSE 0 END ) AS LOW_LINE_COUNT
FROM ORDERTBL, LINEITEM
WHERE O_ORDERKEY = L_ORDERKEY AND L_SHIPMODE IN ('TRUCK', 'AIR')
AND L_COMMITDATE < L_RECEIPTDATE AND L_SHIPDATE < L_COMMITDATE AND L_RECEIPTDATE >= '1994-01-01'
AND L_RECEIPTDATE < DATEADD(MM, 1, CAST('1994-01-01' AS DATETIME))
GROUP BY L_SHIPMODE
ORDER BY L_SHIPMODE
;

SELECT /* tdb=TPCH_Q01 */
    L_RETURNFLAG, 
    L_LINESTATUS, 
    SUM(CAST(L_QUANTITY AS BIGINT)) AS SUM_QTY,
    SUM(L_EXTENDEDPRICE) AS SUM_BASE_PRICE, 
    SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS SUM_DISC_PRICE,
    SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)*(1+L_TAX)) AS SUM_CHARGE, 
    AVG(L_QUANTITY) AS AVG_QTY,
    AVG(L_EXTENDEDPRICE) AS AVG_PRICE, 
    AVG(L_DISCOUNT) AS AVG_DISC, 
    COUNT_BIG(*) AS COUNT_ORDER
FROM LINEITEM
WHERE L_SHIPDATE <= DATEADD(DD, -90, CAST('1998-12-01' AS DATETIME))
GROUP BY L_RETURNFLAG, L_LINESTATUS
ORDER BY L_RETURNFLAG,L_LINESTATUS
;

SELECT /* tdb=TPCH_Q10 */
TOP 20 C_CUSTKEY, C_NAME, SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS REVENUE, C_ACCTBAL,
N_NAME, C_ADDRESS, C_PHONE, C_COMMENT
FROM CUSTOMER, ORDERTBL, LINEITEM, NATION
WHERE C_CUSTKEY = O_CUSTKEY AND L_ORDERKEY = O_ORDERKEY AND O_ORDERDATE>= '1994-03-01' AND
O_ORDERDATE < DATEADD(MM, 3, CAST('1994-03-01' AS DATETIME)) AND
L_RETURNFLAG = 'R' AND C_NATIONKEY = N_NATIONKEY
GROUP BY C_CUSTKEY, C_NAME, C_ACCTBAL, C_PHONE, N_NAME, C_ADDRESS, C_COMMENT
ORDER BY REVENUE DESC
;

SELECT /* tdb=TPCH_Q05 */
N_NAME, SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS REVENUE
FROM CUSTOMER, ORDERTBL, LINEITEM, SUPPLIER, NATION, REGION
WHERE C_CUSTKEY = O_CUSTKEY AND L_ORDERKEY = O_ORDERKEY AND L_SUPPKEY = S_SUPPKEY
AND C_NATIONKEY = S_NATIONKEY AND S_NATIONKEY = N_NATIONKEY AND N_REGIONKEY = R_REGIONKEY
AND R_NAME = 'AFRICA' AND O_ORDERDATE >= '1993-01-01'
AND O_ORDERDATE < DATEADD(YY, 1, cast('1993-01-01' as datetime))
GROUP BY N_NAME
ORDER BY REVENUE DESC
;

SELECT /* tdb=TPCH_Q22 */
CNTRYCODE, COUNT(*) AS NUMCUST, SUM(C_ACCTBAL) AS TOTACCTBAL
FROM (SELECT SUBSTRING(C_PHONE,1,2) AS CNTRYCODE, C_ACCTBAL
FROM CUSTOMER WHERE SUBSTRING(C_PHONE,1,2) IN ('24', '22', '34', '21', '32', '25', '30') AND
C_ACCTBAL > (SELECT AVG(C_ACCTBAL) FROM CUSTOMER WHERE C_ACCTBAL > 0.00 AND
SUBSTRING(C_PHONE,1,2) IN ('24', '22', '34', '21', '32', '25', '30')) AND
NOT EXISTS ( SELECT * FROM ORDERTBL WHERE O_CUSTKEY = C_CUSTKEY)) AS CUSTSALE
GROUP BY CNTRYCODE
ORDER BY CNTRYCODE
;

SELECT /* tdb=TPCH_Q04 */
O_ORDERPRIORITY, COUNT(*) AS ORDER_COUNT FROM ORDERTBL
WHERE O_ORDERDATE >= '1994-12-01' AND O_ORDERDATE < DATEADD(MM,3, CAST('1994-12-01' AS DATETIME))
AND EXISTS (SELECT * FROM LINEITEM WHERE L_ORDERKEY = O_ORDERKEY AND L_COMMITDATE < L_RECEIPTDATE)
GROUP BY O_ORDERPRIORITY
ORDER BY O_ORDERPRIORITY
;

SELECT /* tdb=TPCH_Q11 */
PS_PARTKEY, SUM(PS_SUPPLYCOST*PS_AVAILQTY) AS VALUE
FROM PARTSUPP, SUPPLIER, NATION
WHERE PS_SUPPKEY = S_SUPPKEY AND S_NATIONKEY = N_NATIONKEY AND N_NAME = 'CHINA'
GROUP BY PS_PARTKEY
HAVING SUM(PS_SUPPLYCOST*PS_AVAILQTY) > (SELECT SUM(PS_SUPPLYCOST*PS_AVAILQTY) * 0.000003333333333
FROM PARTSUPP, SUPPLIER, NATION
WHERE PS_SUPPKEY = S_SUPPKEY AND S_NATIONKEY = N_NATIONKEY AND N_NAME = 'CHINA')
ORDER BY VALUE DESC
;

SELECT /* tdb=TPCH_Q16 */
P_BRAND, P_TYPE, P_SIZE, COUNT(DISTINCT PS_SUPPKEY) AS SUPPLIER_CNT
FROM PARTSUPP, PARTTBL
WHERE P_PARTKEY = PS_PARTKEY AND P_BRAND <> 'Brand#25' AND P_TYPE NOT LIKE 'MEDIUM POLISHED%%'
AND P_SIZE IN (2, 7, 24, 30, 27, 50, 23, 45) AND PS_SUPPKEY NOT IN (SELECT S_SUPPKEY FROM SUPPLIER
WHERE S_COMMENT LIKE '%%Customer%%Complaints%%')
GROUP BY P_BRAND, P_TYPE, P_SIZE
ORDER BY SUPPLIER_CNT DESC, P_BRAND, P_TYPE, P_SIZE
;

SELECT /* tdb=TPCH_Q07 */
SUPP_NATION, CUST_NATION, L_YEAR, SUM(VOLUME) AS REVENUE
FROM ( SELECT N1.N_NAME AS SUPP_NATION, N2.N_NAME AS CUST_NATION, datepart(yy, L_SHIPDATE) AS L_YEAR,
L_EXTENDEDPRICE*(1-L_DISCOUNT) AS VOLUME
FROM SUPPLIER, LINEITEM, ORDERTBL, CUSTOMER, NATION N1, NATION N2
WHERE S_SUPPKEY = L_SUPPKEY AND O_ORDERKEY = L_ORDERKEY AND C_CUSTKEY = O_CUSTKEY
AND S_NATIONKEY = N1.N_NATIONKEY AND C_NATIONKEY = N2.N_NATIONKEY AND  ((N1.N_NAME = 'ARGENTINA' AND N2.N_NAME = 'PERU') OR
(N1.N_NAME = 'PERU' AND N2.N_NAME = 'ARGENTINA')) AND
L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31' ) AS SHIPPING
GROUP BY SUPP_NATION, CUST_NATION, L_YEAR
ORDER BY SUPP_NATION, CUST_NATION, L_YEAR
;

SELECT /* tdb=TPCH_Q18 */
TOP 100 C_NAME, C_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE, SUM(L_QUANTITY)
FROM CUSTOMER, ORDERTBL, LINEITEM
WHERE O_ORDERKEY IN (SELECT L_ORDERKEY FROM LINEITEM GROUP BY L_ORDERKEY HAVING
SUM(L_QUANTITY) > 313) AND C_CUSTKEY = O_CUSTKEY AND O_ORDERKEY = L_ORDERKEY
GROUP BY C_NAME, C_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE
ORDER BY O_TOTALPRICE DESC, O_ORDERDATE
;

SELECT /* tdb=TPCH_Q08 */
O_YEAR, SUM(CASE WHEN NATION = 'PERU' THEN VOLUME ELSE 0 END)/SUM(VOLUME) AS MKT_SHARE
FROM (SELECT datepart(yy,O_ORDERDATE) AS O_YEAR, L_EXTENDEDPRICE*(1-L_DISCOUNT) AS VOLUME, N2.N_NAME AS NATION
FROM PARTTBL, SUPPLIER, LINEITEM, ORDERTBL, CUSTOMER, NATION N1, NATION N2, REGION
WHERE P_PARTKEY = L_PARTKEY AND S_SUPPKEY = L_SUPPKEY AND L_ORDERKEY = O_ORDERKEY
AND O_CUSTKEY = C_CUSTKEY AND C_NATIONKEY = N1.N_NATIONKEY AND
N1.N_REGIONKEY = R_REGIONKEY AND R_NAME = 'AMERICA' AND S_NATIONKEY = N2.N_NATIONKEY
AND O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31' AND P_TYPE= 'SMALL BRUSHED BRASS') AS ALL_NATIONS
GROUP BY O_YEAR
ORDER BY O_YEAR
;

SELECT /* tdb=TPCH_Q09 */
NATION, O_YEAR, SUM(AMOUNT) AS SUM_PROFIT
FROM (SELECT N_NAME AS NATION, datepart(yy, O_ORDERDATE) AS O_YEAR,
L_EXTENDEDPRICE*(1-L_DISCOUNT)-PS_SUPPLYCOST*L_QUANTITY AS AMOUNT
FROM PARTTBL, SUPPLIER, LINEITEM, PARTSUPP, ORDERTBL, NATION
WHERE S_SUPPKEY = L_SUPPKEY AND PS_SUPPKEY= L_SUPPKEY AND PS_PARTKEY = L_PARTKEY AND
P_PARTKEY= L_PARTKEY AND O_ORDERKEY = L_ORDERKEY AND S_NATIONKEY = N_NATIONKEY AND
P_NAME LIKE '%%frosted%%') AS PROFIT
GROUP BY NATION, O_YEAR
ORDER BY NATION, O_YEAR DESC
;

SELECT /* tdb=TPCH_Q17 */
SUM(L_EXTENDEDPRICE)/7.0 AS AVG_YEARLY FROM LINEITEM, PARTTBL
WHERE P_PARTKEY = L_PARTKEY AND P_BRAND = 'Brand#53' AND P_CONTAINER = 'SM BOX'
AND L_QUANTITY < (SELECT 0.2*AVG(L_QUANTITY) FROM LINEITEM WHERE L_PARTKEY = P_PARTKEY)
;

WITH /* tdb=TPCH_Q15 */
REVENUE0 AS (
SELECT 
  L_SUPPKEY AS SUPPLIER_NO, 
  SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS TOTAL_REVENUE
FROM LINEITEM
WHERE L_SHIPDATE >= '1994-02-01' AND L_SHIPDATE < DATEADD(MM, 3, CAST('1994-02-01' AS DATETIME))
GROUP BY L_SUPPKEY
)
SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE, TOTAL_REVENUE
FROM SUPPLIER, REVENUE0
WHERE S_SUPPKEY = SUPPLIER_NO AND TOTAL_REVENUE = (SELECT MAX(TOTAL_REVENUE) FROM REVENUE0)
ORDER BY S_SUPPKEY
;
```

</details>

### Query History

<details>

```sql
SELECT  r.[request_id]                           
,       r.[status]                               
,       r.resource_class                         
,       r.command
,       sum(bytes_processed) AS bytes_processed
,       sum(rows_processed) AS rows_processed
FROM    sys.dm_pdw_exec_requests r
              JOIN sys.dm_pdw_dms_workers w
                     ON r.[request_id] = w.request_id
WHERE session_id <> session_id() and type = 'WRITER'
GROUP BY r.[request_id]                           
,       r.[status]                               
,       r.resource_class                         
,       r.command;
```

</details>

### Query Profiles

<details>

Requires PSQL CLI

Works with Provisioned Clusters only

```bash
export PGDATABASE=tpch
export PGHOST=tdalpha.c3r0s0svrewi.us-east-1.redshift.amazonaws.com
export PGPORT=5439
export PGUSER=tdalpha
export PGPASSWORD=17095ViaDelCampo
psql -v qid=<query ID> -f rs-sqlmon.sql > <outfile>.html
```

rs-sqlmon.sql

```sql
\H

\echo '<html>'
\echo '<head>'
\echo '<meta http-equiv="Content-Type" content="text/html; charset=US-ASCII">'
\echo '<meta name="generator" content="SQL*Plus 12.2.0">'
\echo '<style type='text/css'>  body {  font:10pt Arial,Helvetica,sans-serif;  color:blue; background:white; }  '
\echo ' p {  font:8pt Arial,sans-serif;  color:grey; background:white; }  table,tr,td {  font:10pt Arial,Helvetica,sans-serif;  white-space:pre;  color:Black; background:white;  padding:0px 0px 0px 0px; margin:0px 0px 0px 0px; } '
\echo ' th {  font:bold 10pt Arial,Helvetica,sans-serif;  color:#336699;  background:#cccc99;  padding:0px 0px 0px 0px;} ' 
\echo ' h1 {  font:16pt Arial,Helvetica,Geneva,sans-serif;  color:#336699;  background-color:White;  border-bottom:1px solid #cccc99;  margin-top:0pt; margin-bottom:0pt; padding:0px 0px 0px 0px;} '
\echo ' h2 {  font:bold 10pt Arial,Helvetica,Geneva,sans-serif;  color:#336699;  background-color:White;  margin-top:4pt; margin-bottom:0pt;} '
\echo ' a {  font:9pt Arial,Helvetica,sans-serif;  color:#663300;  background:#ffffff;  margin-top:0pt; margin-bottom:0pt; vertical-align:top;}  .threshold-critical {  font:bold 10pt Arial,Helvetica,sans-serif;  color:red; } '
\echo ' .threshold-warning {  font:bold 10pt Arial,Helvetica,sans-serif;  color:orange; }  .threshold-ok {  font:bold 10pt Arial,Helvetica,sans-serif;  color:green; }  </style>'
\echo '<title>Redshift SQL Monitor Report</title>'
\echo '</head>'
\echo '<body>'

--Query 1
SELECT text "Query"
FROM stl_querytext
  --where workload='&&1'
  --and label='&&2'
WHERE query=:qid
ORDER BY sequence;


--Query 2
SELECT ROUND(WallTime,2) "Wall Time (sec)",
  slices "Slices",
  DbTimeQry "DB Time (sec)",
  CpuQry "CPU Time (sec)",
  OtherTime "Other Time (sec)",
  DbTimeLongestSeg "DB Time (sec) Longest Seg",
  CpuLongestSeg "CPU Time (sec) Longest Seg"
FROM
  /* Wall time is calculated by summing the max elapsed times per stream.  Redshift executes different parts
  of the query in segments and steps and processes a stream at a time.
  See http://docs.aws.amazon.com/redshift/latest/dg/reviewing-query-plan-steps.html */
  --  (SELECT 86400*(endtime-starttime) WallTime
  (
  SELECT extract(millisecond FROM (endtime-starttime))/1000 WallTime,
    query
  FROM stl_query
    --      WHERE workload='&&1'
    --      AND label     ='&&2'
  WHERE query=:qid
  ) a ,
  (
  /* The elapsed and cpu times for the sum of what each slice is responsible for doing is
  in stl_metrics.  See http://docs.aws.amazon.com/redshift/latest/dg/r_STL_QUERY_METRICS.html */
  SELECT slices,
    ROUND(cpu_time    /1000000.0,2) CpuQry,
    ROUND(max_cpu_time/1000000.0,2) CpuLongestSeg,
    ROUND(run_time    /1000000.0,2) DbTimeQry,
    ROUND(max_run_time/1000000.0,2) DbTimeLongestSeg,
    ROUND((run_time   /1000000.0 - cpu_time/1000000.0),2) OtherTime,
    query
  FROM stl_query_metrics
  WHERE --workload='&&1'
    --AND
    --seg = -1
    segment= -1
  AND step = -1
    --AND label     = '&&2'
  AND query=:qid
  ) b
  --WHERE a.query=b.query(+)
  ;
  
  
--Query 3
with total_ela as
(select date_diff('microseconds'::text, min(start_time),max(end_time)) total_ela from svl_query_report where query=:qid
)
select e.nodeid "Plan Line",
  e.parentid "Parent Line",
  rtrim(e.plannode, ' ') "Operation",
  round(date_diff('microseconds'::text,min_start_time,max_end_time)/1000000.0,2) "Elapsed Time(s)",
  rpad(' ',(date_diff('microseconds'::text,min_start_time,max_end_time)/(total_ela.total_ela/20))::INTEGER ,'*') "Activity" ,
  qstats.slices,
  qstats.rows "Rows (est)",
  qstats.srows "Rows (act)",
  qstats.max_rows "Rows/slice (Max)",
  qstats.min_rows "Rows/slice (Min)",
  qstats.arows "Rows/slice (Avg)",
  qstats.skew_rows "Row/slice Skew",
  round(qstats.max_ela/1000000.0,2) "Elps/slice (Max)",
  round(qstats.min_ela/1000000.0,2) "Elps/slice (Min)",
  round(qstats.avg_ela/1000000.0,2) "Elps/slice (Avg)",
  qstats.skew_ela "Elps/slice Skew",
  rtrim(e.info, ' ') "Plan Line Info"
from
(
SELECT pi.userid, 
      pi.nodeid,
      pi.query,
--      qr.start_time,
--      qr.end_time,
      qr.slice,
      qr.step,
      qr.rows,
      qr.elapsed_time,
      qr.label,
      count(slice) over (partition BY pi.nodeid,qr.segment,qr.step order by qr.segment desc, qr.step desc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) slices,
      row_number() over (partition BY pi.nodeid order by qr.segment desc, qr.step desc) rn,
      sum(qr.rows) over (partition BY pi.nodeid,qr.segment,qr.step order by qr.segment desc, qr.step desc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ) srows,
      avg(qr.rows) over (partition BY pi.nodeid,qr.segment,qr.step order by qr.segment desc, qr.step desc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ) arows,
      min(qr.rows) over (partition BY pi.nodeid,qr.segment,qr.step order by qr.segment desc, qr.step desc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ) min_rows,
      max(qr.rows) over (partition BY pi.nodeid,qr.segment,qr.step order by qr.segment desc, qr.step desc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ) max_rows,
      round(stddev(qr.rows) over (partition BY pi.nodeid,qr.segment,qr.step order by qr.segment desc, qr.step desc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ),2) skew_rows,
      min(qr.elapsed_time) over (partition BY pi.nodeid,qr.segment,qr.step order by qr.segment desc, qr.step desc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ) min_ela,
      max(qr.elapsed_time) over (partition BY pi.nodeid,qr.segment,qr.step order by qr.segment desc, qr.step desc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ) max_ela,
      avg(qr.elapsed_time) over (partition BY pi.nodeid,qr.segment,qr.step order by qr.segment desc, qr.step desc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ) avg_ela,
      round(stddev(qr.elapsed_time) over (partition BY pi.nodeid,qr.segment,qr.step order by qr.segment desc, qr.step desc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ),2) skew_ela,
      min(qr.start_time) over (partition BY pi.nodeid order by qr.start_time rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ) min_start_time,
      max(qr.end_time) over (partition BY pi.nodeid order by qr.start_time rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ) max_end_time
    FROM svl_query_report qr,
      stl_plan_info pi
    WHERE
      pi.userid  = qr.userid
    AND pi.query = qr.query
    AND pi.segment = qr.segment
    AND pi.step    = qr.step
    AND pi.query=:qid
    and pi.nodeid!=1
    union all
SELECT pi.userid, 
      pi.nodeid,
      pi.query,
--      qr.start_time,
--      qr.end_time,
      qr.slice,
      qr.step,
      qr.rows,
      qr.elapsed_time,
      qr.label,
      count(slice) over (partition BY pi.nodeid,qr.segment,qr.step order by qr.segment desc, qr.step desc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) slices,
      row_number() over (partition BY pi.nodeid order by qr.segment desc, qr.step desc) rn,
      sum(qr.rows) over (partition BY pi.nodeid,qr.segment,qr.step order by qr.segment desc, qr.step desc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ) srows,
      avg(qr.rows) over (partition BY pi.nodeid,qr.segment,qr.step order by qr.segment desc, qr.step desc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ) arows,
      min(qr.rows) over (partition BY pi.nodeid,qr.segment,qr.step order by qr.segment desc, qr.step desc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ) min_rows,
      max(qr.rows) over (partition BY pi.nodeid,qr.segment,qr.step order by qr.segment desc, qr.step desc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ) max_rows,
      round(stddev(qr.rows) over (partition BY pi.nodeid,qr.segment,qr.step order by qr.segment desc, qr.step desc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ),2) skew_rows,
      min(qr.elapsed_time) over (partition BY pi.nodeid,qr.segment,qr.step order by qr.segment desc, qr.step desc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ) min_ela,
      max(qr.elapsed_time) over (partition BY pi.nodeid,qr.segment,qr.step order by qr.segment desc, qr.step desc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ) max_ela,
      avg(qr.elapsed_time) over (partition BY pi.nodeid,qr.segment,qr.step order by qr.segment desc, qr.step desc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ) avg_ela,
      round(stddev(qr.elapsed_time) over (partition BY pi.nodeid,qr.segment,qr.step order by qr.segment desc, qr.step desc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ),2) skew_ela,
      min(qr.start_time) over (partition BY pi.nodeid order by qr.start_time rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ) min_start_time,
      max(qr.end_time) over (partition BY pi.nodeid order by qr.start_time rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ) max_end_time
    FROM svl_query_report qr,
      (select distinct nodeid,userid, query, segment from stl_plan_info where nodeid=1 and query=:qid) pi
    WHERE
      pi.userid  = qr.userid
    AND pi.query = qr.query
    AND pi.segment = qr.segment
--    AND pi.nodeid    = 1
--    AND pi.query=:qid
) qstats, stl_explain e,total_ela
where qstats.userid=e.userid and
qstats.query=e.query and
qstats.nodeid=e.nodeid and
qstats.query=e.query and
qstats.rn=1 
 ORDER BY qstats.nodeid
 ;


--Query 4
/* Redshift executes steps in parallel in streams.  This query shows which streams were used in which lines of the plan. */
WITH qstms AS
  (SELECT nodeid,
    procpernode,
    '{ '
    ||listagg(stm,',') within GROUP (
  ORDER BY stm)
    ||' }' stms
  FROM
    ( SELECT DISTINCT nodeid,
      stm,
      MAX(procpersliceperstm * slicespernode) over (partition BY nodeid) procpernode
    FROM
      (SELECT nodeid,
        slice,
        segment,
        stm,
        MAX(procpersliceperstm) over (partition BY nodeid,stm) procpersliceperstm,
        MAX(slicespernode) over (partition BY nodeid ) slicespernode
      FROM
        (SELECT pi.nodeid,
          qr.slice,
          qr.segment,
          qs.stm,
          --        COUNT(Distinct qr.segment) over (partition BY pi.nodeid,qs.stm) procpersliceperstm,
          --        COUNT(Distinct qr.slice) over (partition BY pi.nodeid) slicespernode
          dense_rank() over (partition BY pi.nodeid,qs.stm order by qr.segment) procpersliceperstm,
          dense_rank() over (partition BY pi.nodeid order by qr.slice) slicespernode
        FROM stl_plan_info pi,
          svl_query_report qr,
          svl_query_summary qs
        WHERE --pi.workload = qr.workload
          --AND
          pi.userid  = qr.userid
        AND pi.query = qr.query
          --      AND pi.label      = qr.label
        AND pi.segment = qr.segment
        AND pi.step    = qr.step
          --      AND pi.workload   = qs.workload
        AND pi.userid = qs.userid
        AND pi.query  = qs.query
          --      AND pi.label      = qs.label
        AND pi.segment = qs.seg
        AND pi.step    = qs.step
          --AND pi.workload   ='&&1'
          --AND pi.label      = '&&2'
        AND pi.query=:qid
        ORDER BY pi.nodeid,
          qr.slice,
          qr.segment,
          qs.stm
        )
      )
    )
  GROUP BY nodeid,
    procpernode
  ORDER BY nodeid
  )
SELECT qplan.nodeid "Plan Line",
  rtrim(qplan.plannode, ' ') "Operation",
  qstms.stms Streams,
  qstms.procpernode "Processes Across All Slices"
FROM qstms ,
  stl_explain qplan
WHERE qplan.nodeid = qstms.nodeid
  --AND qplan.workload ='&&1'
  --AND qplan.label    = '&&2'
AND qplan.query=:qid
ORDER BY 1;

--Query 5
SELECT *
FROM svl_query_report
  --where workload='&&1'
  --and label='&&2'
WHERE query=:qid
ORDER BY segment, step, elapsed_time, rows;

--Query 6
SELECT *
FROM svl_query_summary
WHERE query=:qid
ORDER BY stm, seg, step;

--Query 7
SELECT *
FROM stl_plan_info
WHERE query=:qid
ORDER BY nodeid desc;

--Query 8 (check data skew)
select query, segment, step, max(rows), min(rows),
case when sum(rows) > 0
then ((cast(max(rows) -min(rows) as float)*count(rows))/sum(rows))
else 0 end
from svl_query_report
where query=:qid
group by query, segment, step
order by segment, step;

\echo '</body>'
\echo '</html>'
```

</details>

### Results Cache

<details>

```sql
set enable_result_cache_for_session to off;
alter user benchmark set enable_result_cache_for_session to off;
```

</details>

## Performance Features

### Designing Tables (Sort and Distribution Keys)

<details>

```sql
alter table ordertbl alter sortkey auto;
alter table ordertbl alter diststyle auto;

alter table lineitem alter sortkey auto;
alter table lineitem alter diststyle auto;

create schema keys;
set search_path to keys;

create table customer as select * from public.CUSTOMER;
create table lineitem as select * from public.LINEITEM;
create table nation as select * from public.NATION;
create table ordertbl as select * from public.ORDERTBL;
create table parttbl as select * from public.PARTTBL;
create table partsupp as select * from public.PARTSUPP;
create table region as select * from public.REGION;
create table supplier as select * from public.SUPPLIER;

alter table ordertbl alter sortkey (o_orderdate);
alter table lineitem alter sortkey (l_shipdate);
alter table ordertbl alter diststyle key distkey o_orderkey;
alter table lineitem alter diststyle key distkey l_orderkey;
```

</details>

### Concurrency Scaling

<details>

```sql
select
    split_part(split_part(querytxt,'tdb=', 2), ' ', 1) as qrynum, 
    concurrency_scaling_status, 
    count(*) 
from STL_QUERY 
where 
    starttime > '2022-12-29 19:09:35' 
    and querytxt like '%tdb=%' 
group by 
    qrynum, 
    concurrency_scaling_status;
```

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

## Usability Features

### Redshift Spectrum (External Tables)

<details>

```sql
create external schema ext 
from data catalog 
database 'extdb' 
iam_role 'arn:aws:iam::791221762878:role/mySpectrumRole' 
create external database if not exists;

unload ('select * from public.customer')
to 's3://mcg-tdc2/tpch/30gb/spectrum/customer' 
iam_role 'arn:aws:iam::791221762878:role/mySpectrumRole'
csv;

create external table ext.customer (
	c_custkey bigint,
	c_name varchar(25),
	c_address varchar(40),
	c_nationkey integer,
	c_phone char(15),
	c_acctbal decimal(15,2),
	c_mktsegment char(10),
	c_comment varchar(117)
) 
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
    'separatorChar' = ',',
    'quoteChar' = '\"',
    'escapeChar' = '\\'
)
stored as textfile
location 's3://mcg-tdc2/tpch/30gb/spectrum/customer';
 
select * from public.customer;
select * from ext.customer;
```

</details>

### AutoMV

<details>

```sql
select 
    split_part(split_part(querytxt,'tdb=', 2), ' ', 1) as qrynum, 
    plannode,
    info    
from 
    STL_QUERY q,
    STL_EXPLAIN e
where 
    q.query = e.query
    and starttime > '2022-12-29 19:13:00' 
    and querytxt like '%tdb=%' 
    and (
        plannode like '%auto_mv%'
        or info like '%auto_mv%'
    );
```

</details>

### Dynamic Data Masking

<details>

```sql
create masking policy name_mask
with (c_name varchar(25))
using ('********');

attach masking policy name_mask
on customer(c_name)
to public;

select * from customer limit 100;
```

</details>

### Python UDF

<details>

```sql
select
    o_custkey,
    sum(o_totalprice) as totalspend
from ordertbl
group by
    o_custkey;

create function f_py_humanreadable (num float)
  returns varchar
stable
as $$
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    return '%.1f%s' % (num, ['', 'K', 'M', 'B', 'T'][magnitude])
$$ language plpythonu;

select
    o_custkey,
    f_py_humanreadable(sum(o_totalprice)::float) as totalspend
from ordertbl
group by
    o_custkey;
```

</details>
