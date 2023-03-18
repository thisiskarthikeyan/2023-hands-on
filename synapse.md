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
17095ViaDelCampo
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
select  r.[request_id]                           
,       r.[status]                               
,       r.resource_class                         
,       r.command
,       sum(bytes_processed) as bytes_processed
,       sum(rows_processed) as rows_processed
from    sys.dm_pdw_exec_requests r
              join sys.dm_pdw_dms_workers w
                     on r.[request_id] = w.request_id
where session_id <> session_id() and type = 'WRITER'
group by r.[request_id]                           
,       r.[status]                               
,       r.resource_class                         
,       r.command;
```

</details>

## Performance Features

### Workload Management

<details>

```sql
--use master
create login benchmark with password='17095ViaDelCampo';
```

```sql
--use sql pool database
create user benchmark for login benchmark;

exec sp_addrolemember 'db_datareader', 'benchmark';
```

</details>

### Results Cache

<details>

OFF by default!

```sql
use master;

alter database <your database> set result_set_caching off;
```

</details>

### Transparent Materialized Views

<details>

```sql
create materialized view mv_q18
with (distribution = hash(l_orderkey))
as
select
   l_orderkey, 
   sum(l_quantity) sum_lq,
   count(l_quantity) count_lq,
   count(*) count_grp
from
   dbo.lineitem
group by
   l_orderkey;
```

```sql
SELECT /* tdb=TPCH_Q18 */
TOP 100 C_NAME, C_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE, SUM(L_QUANTITY)
FROM CUSTOMER, ORDERTBL, LINEITEM
WHERE O_ORDERKEY IN (SELECT L_ORDERKEY FROM LINEITEM GROUP BY L_ORDERKEY HAVING
SUM(L_QUANTITY) > 313) AND C_CUSTKEY = O_CUSTKEY AND O_ORDERKEY = L_ORDERKEY
GROUP BY C_NAME, C_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE
ORDER BY O_TOTALPRICE DESC, O_ORDERDATE
;

```

</details>

## Usability Features

### Dynamic Data Masking

<details>

```sql
alter table CUSTOMER alter column c_name add masked with (function = 'default()');

EXECUTE AS USER='benchmark';
select top 10 * from CUSTOMER;

alter table CUSTOMER alter column c_name drop masked;
```

</details>

### External Data Sources

<details>

```sql
create master key encryption by password = '17095ViaDelCampo';
open master key decryption by password = '17095ViaDelCampo';

create database scoped credential AzureStorageCredential
with
    identity = 'mcg',
    secret = 'GE+EHbRey/07RrZjO7eEoSQVZsozTqnKC02ZlBTpqiNUYClkN6dDa35D3aQfkaFj/grs85XRtOFG+AStL6qxmg==';

create external data source mcgtpch
with (
    type = hadoop,
    location = 'wasbs://tpch@tdalpha.blob.core.windows.net',
    credential = AzureStorageCredential
);
create external file format CSVPipe
with (
    format_type = DelimitedText,
    format_options (field_terminator = '|')
);
create external file format CompressedPipe
with (
    format_type = DelimitedText,
    format_options (field_terminator = '|'),
    data_compression = 'org.apache.hadoop.io.compress.GzipCodec'
);

create external table CUSTOMER_EXT (
	c_custkey integer not null,
	c_name varchar(25) not null,
	c_address varchar(40) not null,
	c_nationkey integer not null,
	c_phone char(15) not null,
	c_acctbal decimal(15,2) not null,
	c_mktsegment char(10) not null,
	c_comment varchar(117) not null
) with (
   location='/tpch/customer/'
  ,data_source=mcgtpch
  ,file_format=CSVPipe
  ,reject_type=value
  ,reject_value=0)
;

select count(*) from CUSTOMER_EXT;
```

</details>

### Studio Charts

<details>

```sql
SELECT /* tdb=TPCH_Q18 */
TOP 100 C_NAME, SUM(O_TOTALPRICE), SUM(L_QUANTITY)
FROM CUSTOMER, ORDERTBL, LINEITEM
WHERE O_ORDERKEY IN (SELECT L_ORDERKEY FROM LINEITEM GROUP BY L_ORDERKEY HAVING
SUM(L_QUANTITY) > 313) AND C_CUSTKEY = O_CUSTKEY AND O_ORDERKEY = L_ORDERKEY
GROUP BY C_NAME
ORDER BY SUM(O_TOTALPRICE) DESC
;
```

</details>

