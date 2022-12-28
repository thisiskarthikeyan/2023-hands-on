# Competitive Hands-On Training - 2023 Q1
# Redshift

## Login
https://791221762878.signin.aws.amazon.com/console

## Basic Functionality

### Create Warehouse, Database, and Tables

<details>

```sql
create table NATION (
	n_nationkey integer not null,
	n_name char(25) not null,
	n_regionkey integer not null,
	n_comment varchar(152)
);
create table REGION (
	r_regionkey integer not null,
	r_name char(25) not null,
	r_comment varchar(152)
);
create table SUPPLIER (
	s_suppkey bigint not null,
	s_name char(25) not null,
	s_address varchar(40) not null,
	s_nationkey integer not null,
	s_phone char(15) not null,
	s_acctbal decimal(15,2) not null,
	s_comment varchar(101) not null
);
create table PARTSUPP (
	ps_partkey bigint not null,
	ps_suppkey bigint not null,
	ps_availqty integer not null,
	ps_supplycost decimal(15,2) not null,
	ps_comment varchar(199) not null
);
create table PARTTBL (
	p_partkey bigint not null,
	p_name varchar(55) not null,
	p_mfgr char(25) not null,
	p_brand char(10) not null,
	p_type varchar(25) not null,
	p_size integer not null,
	p_container char(10) not null,
	p_retailprice decimal(15,2) not null,
	p_comment varchar(23) not null
);
create table CUSTOMER (
	c_custkey bigint not null,
	c_name varchar(25) not null,
	c_address varchar(40) not null,
	c_nationkey integer not null,
	c_phone char(15) not null,
	c_acctbal decimal(15,2) not null,
	c_mktsegment char(10) not null,
	c_comment varchar(117) not null
);
create table ORDERTBL (
	o_orderkey bigint not null,
	o_custkey bigint not null,
	o_orderstatus char(1) not null,
	o_totalprice decimal(15,2) not null,
	o_orderdate date not null,
	o_orderpriority char(15) not null, 
	o_clerk char(15) not null, 
	o_shippriority integer not null,
	o_comment varchar(79) not null
);
create table LINEITEM (
	l_orderkey bigint not null,
	l_partkey bigint not null,
	l_suppkey bigint not null,
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
);

create view REVENUE0 (supplier_no, total_revenue) as
select
    l_suppkey,
    sum(l_extendedprice * (1 - l_discount))
from
    lineitem
where
    l_shipdate >= '1996-01-01' and l_shipdate < dateadd(month, 3, '1996-01-01')
group by
    l_suppkey;
```

</details>

### Loading Data (Directly from S3)

<details>

```sql
copy nation from 's3://mcg-tdc2/tpch/30gb/nation' 
iam_role 'arn:aws:iam::791221762878:role/mySpectrumRole'
format as csv
delimiter as '|';

copy region from 's3://mcg-tdc2/tpch/30gb/region' 
iam_role 'arn:aws:iam::791221762878:role/mySpectrumRole'
format as csv
delimiter as '|';

copy supplier from 's3://mcg-tdc2/tpch/30gb/supplier' 
iam_role 'arn:aws:iam::791221762878:role/mySpectrumRole'
format as csv
delimiter as '|';

copy partsupp from 's3://mcg-tdc2/tpch/30gb/partsupp' 
iam_role 'arn:aws:iam::791221762878:role/mySpectrumRole' 
format as csv 
delimiter as '|';

copy parttbl from 's3://mcg-tdc2/tpch/30gb/part.tbl' 
iam_role 'arn:aws:iam::791221762878:role/mySpectrumRole' 
format as csv 
delimiter as '|';

copy customer from 's3://mcg-tdc2/tpch/30gb/customer' 
iam_role 'arn:aws:iam::791221762878:role/mySpectrumRole' 
format as csv 
delimiter as '|';

copy ordertbl from 's3://mcg-tdc2/tpch/30gb/orders.tbl' 
iam_role 'arn:aws:iam::791221762878:role/mySpectrumRole' 
format as csv 
delimiter as '|';

copy lineitem from 's3://mcg-tdc2/tpch/30gb/lineitem' 
iam_role 'arn:aws:iam::791221762878:role/mySpectrumRole' 
format as csv 
delimiter as '|' ;

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
select 
/* tdb=TPCH_Q06 */
sum(l_extendedprice*l_discount) as revenue
from lineitem
where l_shipdate >= '1993-01-01' and
 l_shipdate < cast (date '1993-01-01' + interval '1 year' as date)  and
 l_discount between 0.02 - 0.01 and 0.02 + 0.01 and
 l_quantity < 25;


select  
/* tdb=TPCH_Q19 */
sum(l_extendedprice* (1 - l_discount)) as revenue
from lineitem,
 parttbl
where 	
    (
		p_partkey = l_partkey
		and p_brand = 'BRAND#11'
		and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		and l_quantity >= 4 and l_quantity <= 4 + 10
		and p_size between 1 and 5
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = 'BRAND#33'
		and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		and l_quantity >= 19 and l_quantity <= 19 + 10
		and p_size between 1 and 10
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = 'BRAND#35'
		and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		and l_quantity >= 25 and l_quantity <= 25 + 10
		and p_size between 1 and 15
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	);


select 
/* tdb=TPCH_Q13 */
c_count,
 count(*) as custdist
from ( select c_custkey,
   count(o_orderkey)
  from customer left outer join ordertbl on
   c_custkey = o_custkey  and
   o_comment not like '%unusual%accounts%'
  group by c_custkey
 ) as c_ordertbl (c_custkey, c_count)
group by c_count
order by custdist desc,
  c_count  desc;


select 
/* tdb=TPCH_Q21 */
s_name, count(*) as numwait
from supplier, lineitem l1, ordertbl, nation
where s_suppkey= l1.l_suppkey and
 o_orderkey= l1.l_orderkey and
 o_orderstatus= 'F' and
 l1.l_receiptdate > l1.l_commitdate and
 exists (select * from lineitem l2
  where l2.l_orderkey = l1.l_orderkey and 
  l2.l_suppkey <> l1.l_suppkey
  ) and
 not exists (select * from lineitem l3
   where l3.l_orderkey = l1.l_orderkey and
   l3.l_suppkey <> l1.l_suppkey and
   l3.l_receiptdate > l3.l_commitdate
   ) and s_nationkey = n_nationkey and n_name = 'IRAN'
group by s_name
order by numwait desc, s_name
limit 100;


select 
/* tdb=TPCH_Q14 */
100.00 * sum ( case when p_type like 'PROMO%'
     then l_extendedprice*(1-l_discount)
     else 0
    end) / sum(l_extendedprice*(1-l_discount)) as promo_revenue
from lineitem,
 parttbl
where l_partkey = p_partkey and
 l_shipdate >= '1995-11-01'  and
        l_shipdate < cast (date '1995-11-01' + interval '1 month' as date);


select 
/* tdb=tpch_q03 */
 l_orderkey,
 sum(l_extendedprice*(1-l_discount)) as revenue,
 o_orderdate, o_shippriority
from customer,
 ordertbl,
 lineitem
where c_mktsegment = 'FURNITURE' and
 c_custkey = o_custkey and
 l_orderkey = o_orderkey and
 o_orderdate < '1995-03-13' and
 l_shipdate > '1995-03-13'
group by l_orderkey,
  o_orderdate,
  o_shippriority
order by revenue desc,
  o_orderdate
limit 10;


select 
/* tdb=TPCH_Q20 */
s_name,
 s_address
from supplier,
 nation
where s_suppkey in (select ps_suppkey
   from partsupp
   where ps_partkey in 
   (select p_partkey
    from parttbl
    where p_name like 'blush%'
    ) and
   ps_availqty > ( select 0.5 * sum(l_quantity)
     from lineitem
      where l_partkey = ps_partkey and
      l_suppkey  = ps_suppkey and
      l_shipdate >= '1995-01-01'  and
      l_shipdate < cast (date '1995-01-01' + interval '1 year' as date)
     )
   ) and
 s_nationkey = n_nationkey and
 n_name = 'KENYA'
order by s_name;


select 
/* tdb=TPCH_Q02 */
 s_acctbal,
 s_name,
 n_name,
 p_partkey,
 p_mfgr,
 s_address,
 s_phone,
 s_comment
from parttbl,
 supplier,
 partsupp,
 nation,
 region
where p_partkey = ps_partkey and
 s_suppkey = ps_suppkey and
 p_size  = 32 and
 p_type  like '%NICKEL' and
 s_nationkey = n_nationkey and
 n_regionkey = r_regionkey and
 r_name  = 'ASIA' and
 ps_supplycost = ( select min(ps_supplycost)
    from partsupp,
     supplier,
     nation,
     region
    where p_partkey = ps_partkey and
     s_suppkey = ps_suppkey and
     s_nationkey = n_nationkey and
     n_regionkey = r_regionkey and
     r_name  = 'ASIA'
     )
order by s_acctbal desc,
  n_name,
  s_name,
  p_partkey
limit 100;


select 
/* tdb=TPCH_Q12 */
l_shipmode,
 sum( case when o_orderpriority  = '1-URGENT' or
        o_orderpriority  = '2-HIGH'
   then 1
   else 0
  end) as high_line_count,
 sum( case when o_orderpriority <> '1-URGENT' and
        o_orderpriority <> '2-HIGH'
   then 1
   else 0
  end) as low_line_count
from ordertbl,
 lineitem
where o_orderkey = l_orderkey  and
 l_shipmode in ('TRUCK', 'AIR')  and
 l_commitdate < l_receiptdate  and
 l_shipdate < l_commitdate  and
 l_receiptdate >= '1994-01-01'   and
 l_receiptdate < cast (date '1994-01-01' + interval '1 year' as date)
group by l_shipmode
order by l_shipmode;


select 
/* tdb=TPCH_Q01 */
l_returnflag,
 l_linestatus,
 sum(l_quantity)     as sum_qty,
 sum(l_extendedprice)    as sum_base_price,
 sum(l_extendedprice*(1-l_discount))  as sum_disc_price,
 sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
 avg(l_quantity)     as avg_qty,
 avg(l_extendedprice)    as avg_price,
 avg(l_discount)     as avg_disc,
 count(*)     as count_order
from lineitem
where l_shipdate <= cast ( date '1998-12-01' - interval '90 days' as date )
group by l_returnflag,
  l_linestatus
order by l_returnflag,
  l_linestatus; 


select 
/* tdb=TPCH_Q10 */
 c_custkey,
 c_name,
 sum(l_extendedprice*(1-l_discount)) as revenue,
 c_acctbal,
 n_name,
 c_address,
 c_phone,
 c_comment
from customer,
 ordertbl,
 lineitem,
 nation
where c_custkey = o_custkey  and
 l_orderkey = o_orderkey  and
 o_orderdate >= '1994-03-01'   and
 o_orderdate < cast (date '1994-03-01' + interval '3 months' as date) and
 l_returnflag = 'R'   and
 c_nationkey = n_nationkey
group by c_custkey,
  c_name,
  c_acctbal,
  c_phone,
  n_name,
  c_address,
  c_comment
order by revenue desc
limit 20;


select 
/* tdb=TPCH_Q05 */
n_name,
 sum(l_extendedprice*(1-l_discount)) as revenue
from customer,
 ordertbl,
 lineitem,
 supplier,
 nation,
 region
where c_custkey = o_custkey and
 l_orderkey = o_orderkey and
 l_suppkey = s_suppkey and
 c_nationkey = s_nationkey and
 s_nationkey = n_nationkey and
 n_regionkey = r_regionkey and
 r_name  = 'AFRICA' and
        o_orderdate >= date '1993-01-01' and 
     o_orderdate < cast (date '1993-01-01' + interval '1 year' as date)
group by n_name
order by revenue desc;


select 
/* tdb=TPCH_Q22 */
cntrycode,
 count(*) as numcust,
 sum(c_acctbal) as totacctbal
from (select substring(c_phone,1,2) as cntrycode, c_acctbal
  from customer
  where substring(c_phone,1,2) in ('24', '22', '34', '21', '32', '25', '30') and
   c_acctbal > (select avg(c_acctbal)
     from customer
     where c_acctbal > 0.00 and
     substring(c_phone,1,2) in ('24', '22', '34', '21', '32', '25', '30')
     ) and
   not exists (select *
     from ordertbl
     where o_custkey = c_custkey
     )
 ) as custsale
group by cntrycode
order by cntrycode;


select 
/* tdb=TPCH_Q04 */
o_orderpriority,
 count(*)  as order_count
from ordertbl
where o_orderdate >= '1994-12-01' and
 o_orderdate < cast (date '1994-12-01' + interval '3 months' as date) and
 exists  ( select *
    from lineitem
    where l_orderkey = o_orderkey and
     l_commitdate < l_receiptdate
   )
group by o_orderpriority
order by o_orderpriority;


select 
/* tdb=TPCH_q11 */
ps_partkey,
 sum(ps_supplycost*ps_availqty) as value
from partsupp,
 supplier,
 nation
where ps_suppkey = s_suppkey and
 s_nationkey = n_nationkey and
 n_name  = 'CHINA'
group by ps_partkey
having sum(ps_supplycost*ps_availqty) >
  ( select sum(ps_supplycost*ps_availqty) * 0.000003333333333
   from partsupp,
    supplier,
    nation
   where ps_suppkey = s_suppkey and
    s_nationkey = n_nationkey and
    n_name  = 'CHINA'
  )
order by value desc;


select 
/* tdb=TPCH_Q16 */
p_brand,
 p_type,
 p_size,
 count(distinct ps_suppkey) as supplier_cnt
from partsupp,
 parttbl
where p_partkey = ps_partkey and
 p_brand <> 'Brand#25' and
 p_type not like 'MEDIUM POLISHED%' and
 p_size in (2, 7, 24, 30, 27, 50, 23, 45) and
 ps_suppkey not in ( select s_suppkey
     from supplier
     where s_comment like '%Customer%Complaints%'
    )
group by p_brand,
  p_type,
  p_size
order by supplier_cnt desc,
  p_brand,
  p_type,
  p_size;


select 
/* tdb=TPCH_Q07 */
supp_nation,
 cust_nation,
 l_year,
 sum(volume) as revenue
from ( select n1.n_name   as supp_nation,
   n2.n_name   as cust_nation,
   extract(year from l_shipdate) as l_year,
   l_extendedprice*(1-l_discount) as volume
  from supplier,
   lineitem,
   ordertbl,
   customer,
   nation n1,
   nation n2
  where s_suppkey = l_suppkey and
   o_orderkey = l_orderkey and
   c_custkey = o_custkey and
   s_nationkey = n1.n_nationkey and
   c_nationkey = n2.n_nationkey and
   ( (n1.n_name = 'ARGENTINA' and n2.n_name = 'PERU')
    or
    (n1.n_name = 'PERU' and n2.n_name = 'ARGENTINA')
   ) and
   l_shipdate between '1995-01-01' and '1996-12-31'
 ) as shipping
group by supp_nation,
  cust_nation,
  l_year
order by supp_nation,
  cust_nation,
  l_year;


select 
/* tdb=TPCH_Q18 */
 c_name,
 c_custkey,
 o_orderkey,
 o_orderdate,
 o_totalprice,
 sum(l_quantity)
from customer,
 ordertbl,
 lineitem
where o_orderkey in (select l_orderkey
    from lineitem
    group by l_orderkey having sum(l_quantity) > 313
    ) and
 c_custkey = o_custkey and
 o_orderkey = l_orderkey
group by c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice
order by o_totalprice desc,
  o_orderdate
limit 100;


select 
/* tdb=TPCH_Q08 */
o_year,
 sum(case when nation = 'PERU'
   then volume
   else 0
   end) / sum(volume) as mkt_share
from ( select 
                   extract(year from o_orderdate) as o_year,
   l_extendedprice * (1-l_discount) as volume,
   n2.n_name    as nation
  from parttbl,
   supplier,
   lineitem,
   ordertbl,
   customer,
   nation n1,
   nation n2,
   region
  where p_partkey = l_partkey and
   s_suppkey = l_suppkey and
   l_orderkey = o_orderkey and
   o_custkey = c_custkey and
   c_nationkey = n1.n_nationkey and
   n1.n_regionkey = r_regionkey and
   r_name = 'AMERICA' and
   s_nationkey = n2.n_nationkey and
   o_orderdate between '1995-01-01' and '1996-12-31' and
   p_type  = 'SMALL BRUSHED BRASS'
 ) as all_nations
group by o_year
order by o_year;


select 
/* tdb=TPCH_Q09 */
nation,
 o_year,
 sum(amount) as sum_profit
from ( select n_name       as nation,
   extract(year from o_orderdate) as o_year,
   l_extendedprice*(1-l_discount)-ps_supplycost*l_quantity as amount
  from parttbl,
   supplier,
   lineitem,
   partsupp,
   ordertbl,
   nation
  where s_suppkey = l_suppkey and
   ps_suppkey = l_suppkey and
   ps_partkey = l_partkey and
   p_partkey = l_partkey and
   o_orderkey = l_orderkey and
   s_nationkey = n_nationkey and
   p_name  like '%frosted%'
 ) as profit
group by nation,
  o_year
order by nation,
  o_year desc;


select 
/* tdb=TPCH_Q17 */
sum(l_extendedprice)/7.0 as avg_yearly
from lineitem,
 parttbl
where p_partkey = l_partkey and
 p_brand  = 'Brand#53' and
 p_container = 'SM BOX' and
 l_quantity < (select 0.2 * avg(l_quantity)
    from lineitem
    where l_partkey = p_partkey
    );


with /* tdb=TPCH_Q15 */ revenue0 AS (
    select
        l_suppkey as supplier_no,
        sum(l_extendedprice * (1 - l_discount)) as total_revenue
    from
        lineitem
    where
        l_shipdate >= '1994-02-01' and
        l_shipdate < cast (date '1994-02-01' + interval '3 months' as date)
    group by
        l_suppkey)
select 
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    supplier,
    revenue0
where
    s_suppkey = supplier_no
    and total_revenue = (
        select
            max(total_revenue)
        from
            revenue0
    )
order by
    s_suppkey;
```

</details>

### Query History

<details>

```sql
select * from SYS_QUERY_HISTORY;
```

</details>

### Query Profiles

<details>

Requires PSQL CLI

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

## Performance Features

### Results Cache

<details>

```sql
set enable_result_cache_for_session to off;
alter user benchmark set enable_result_cache_for_session to off;
```

</details>

### Multi-clusters

<details>

```sql
alter warehouse TDALPHA set max_concurrency_level = 32;

alter warehouse TDALPHA set min_cluster_count = 1 max_cluster_count = 3;

alter warehouse TDALPHA set scaling_policy = 'ECONOMY';

select 
  cluster_number,
  sum(execution_time/1000),
  sum(queued_overload_time/1000)
from table(information_schema.query_history_by_user(user_name => 'TDALPHA', result_limit => 10000)) 
where 
  end_time between 
    to_timestamp_tz('2022-12-24 20:11:16 -0000') 
    and to_timestamp_tz('2022-12-24 20:15:04 -0000') 
group by 1
order by 1;
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

### Search Optimization Service

<details>

```sql
select approx_count_distinct(o_clerk) from ordertbl;

select o_clerk, count(*) cnt from ordertbl group by o_clerk order by cnt desc;

select * from ordertbl where o_clerk in ('Clerk#000007320','Clerk#000024529','Clerk#000007341');
select * from ordertbl where o_clerk like 'Clerk#000007%';

alter table ordertbl add search optimization on equality(o_clerk);
show tables like 'ordertbl';

alter table ordertbl drop search optimization;
alter table ordertbl add search optimization on equality(o_clerk), substring(o_clerk);
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
 
 select * from public.customer;
 select * from ext.customer;
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
  
  select * from customer limit 100;
  
  alter masking policy name_mask set body ->
  case
    when current_role() in ('SYSADMIN') then val
    else '*********'
  end;

select * from customer limit 100;
```

</details>

### Time Travel

<details>

```sql
show tables like 'ordertbl';

select count(*) from ordertbl;

delete from ordertbl where o_orderkey < 1000;

select count(*) from ordertbl;

select count(*) from ordertbl at(offset => -60*5);

create table restored_ordertbl clone ordertbl
  at(offset => -60*5);

select count(*) from restored_ordertbl;

drop table ordertbl;

alter table restored_ordertbl rename to ordertbl;

select count(*) from ordertbl;
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
        extract(year from o_orderdate) as o_year,
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
from snowflake.snowpark import Session
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

session.sql("select count(*) from customer").collect()
```

</details>
