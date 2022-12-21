# Competitive Hands-On Training - 2023 Q1
# Snowflake

## Setup
https://of53892.us-east-1.snowflakecomputing.com/

## Databases and DDL

### Create Warehouse, Database, and Tables

<details><p>

```sql
set MYNAME='NAME_FOR_YOUR_STUFF';

create or replace warehouse identifier($MYNAME) 
    warehouse_size=xsmall
    initially_suspended=true
    min_cluster_count=1
    max_cluster_count=1
    auto_suspend=300;

create or replace database identifier($MYNAME);
use identifier($MYNAME);

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

--revenue0 view
create or replace view revenue0 (supplier_no, total_revenue) as
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

</p></details>

## Loading Data

### Directly from S3

<details><p>

```sql
create or replace file format abench_filefmt
	type = 'CSV'
	field_delimiter = '|'
    field_optionally_enclosed_by = '"'
    date_format = 'YYYY-MM-DD'
    error_on_column_count_mismatch=false
    encoding = 'iso-8859-1';

create or replace stage abench_s3_stage
	file_format = abench_filefmt
	credentials = (
	    aws_key_id='AKIA3QOD57M7H2SLFMGA'
	    aws_secret_key='UrFZFfjaHJJdB6QATUizxG+JeRQqauAoVrT8u0Y9')
	url = 's3://mcg-tdc2/tpch/30gb';

copy into nation from @abench_s3_stage pattern = '.*nation.*';
copy into region from @abench_s3_stage pattern = '.*region.*';
copy into supplier from @abench_s3_stage pattern = '.*supplier.*';
copy into parttbl from @abench_s3_stage pattern = '.*part\.tbl.*'; 
copy into partsupp from @abench_s3_stage pattern = '.*partsupp.*';
copy into customer from @abench_s3_stage pattern = '.*customer.*';
copy into ordertbl from @abench_s3_stage pattern = '.*orders.*';
copy into lineitem from @abench_s3_stage pattern = '.*lineitem.*';

select 'customer' entity, count(*) from customer union all
select 'lineitem' entity, count(*) from lineitem union all
select 'nation' entity, count(*) from nation union all
select 'ordertbl' entity, count(*) from ordertbl union all
select 'parttbl' entity, count(*) from parttbl union all
select 'partsupp' entity, count(*) from partsupp union all
select 'region' entity, count(*) from region union all
select 'supplier' entity, count(*) from supplier order by 2;
```

</p></details>

## Running Queries

### TPC-H 30GB Stream

<details><p>

```sql
/* TPC_H  Query 6 - Forecasting Revenue Change */
select /* tdb=TPCH_Q06 */
    sum(l_extendedprice*l_discount) as revenue
from
    lineitem
where
    l_shipdate >= '1994-01-01'
    and l_shipdate < dateadd(year, 1, to_date('1994-01-01'))
    and l_discount between 0.06 - 0.01 and 0.06 + 0.01
    and l_quantity < 24;

/* TPC_H  Query 19 - Discounted Revenue */
select /* tdb=TPCH_Q19 */
    sum(l_extendedprice * (1 - l_discount) ) as revenue
from
    lineitem,
    parttbl
where
    (
        p_partkey = l_partkey
        and trim(p_brand) = 'Brand#12'
        and trim(p_container) in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        and l_quantity >= 1 and l_quantity <= 1 + 10
        and p_size between 1 and 5
        and trim(l_shipmode) in ('AIR', 'AIR REG')
        and trim(l_shipinstruct) = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and trim(p_brand) = 'Brand#23'
        and trim(p_container) in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        and l_quantity >= 10 and l_quantity <= 10 + 10
        and p_size between 1 and 10
        and trim(l_shipmode) in ('AIR', 'AIR REG')
        and trim(l_shipinstruct) = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and trim(p_brand) = 'Brand#34'
        and trim(p_container) in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        and l_quantity >= 20 and l_quantity <= 20 + 10
        and p_size between 1 and 15
        and trim(l_shipmode) in ('AIR', 'AIR REG')
        and trim(l_shipinstruct) = 'DELIVER IN PERSON'
    );

/* TPC_H  Query 13 - Customer Distribution */
select /* tdb=TPCH_Q13 */
    c_count, count(*) as custdist
from (
    select
        c_custkey,
        count(o_orderkey) as c_count
    from
        customer left outer join ordertbl on
            c_custkey = o_custkey
            and o_comment not like '%special%requests%'
    group by
        c_custkey
    )as c_orders
group by
    c_count
order by
    custdist desc,
    c_count desc;

/* TPC_H  Query 21 - Suppliers Who Kept Orders Waiting */
select /* tdb=TPCH_Q21 */
    s_name,
    count(*) as numwait
from
    supplier,
    lineitem l1,
    ordertbl,
    nation
where
    s_suppkey = l1.l_suppkey
    and o_orderkey = l1.l_orderkey
    and o_orderstatus = 'F'
    and l1.l_receiptdate > l1.l_commitdate
    and exists (
        select
            *
        from
            lineitem l2
        where
            l2.l_orderkey = l1.l_orderkey
            and l2.l_suppkey <> l1.l_suppkey
    )
    and not exists (
        select
            *
        from
            lineitem l3
        where
            l3.l_orderkey = l1.l_orderkey
            and l3.l_suppkey <> l1.l_suppkey
            and l3.l_receiptdate > l3.l_commitdate
    )
    and s_nationkey = n_nationkey
    and trim(n_name) = 'SAUDI ARABIA'
group by
    s_name
order by
    numwait desc,
    s_name
limit 100;

/* TPC_H  Query 14 - Promotion Effect */
select /* tdb=TPCH_Q14 */
    100.00 * sum(case
        when p_type like 'PROMO%'
        then l_extendedprice*(1-l_discount)
        else 0
    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
    lineitem,
    parttbl
where
    l_partkey = p_partkey
    and l_shipdate >= '1995-09-01'
    and l_shipdate < dateadd(month, 1, to_date('1995-09-01'));

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
limit 10;

/* TPC_H  Query 20 - Potential Part Promotion */
select /* tdb=TPCH_Q20 */
    s_name,
    s_address
from
    supplier, nation
where
    s_suppkey in (
        select
            ps_suppkey
        from
            partsupp
        where
            ps_partkey in (
                select
                    p_partkey
                from
                    parttbl
                where
                    p_name like 'forest%'
            )
            and ps_availqty > (
                select
                    0.5 * sum(l_quantity)
                from
                    lineitem
                where
                    l_partkey = ps_partkey
                    and l_suppkey = ps_suppkey
                    and l_shipdate >= '1994-01-01'
                    and l_shipdate < dateadd(year, 1, to_date('1994-01-01'))
            )
    )
    and s_nationkey = n_nationkey
    and trim(n_name) = 'CANADA'
order by
    s_name;

/* TPC_H  Query 2 - Minimum Cost Supplier */
select /* tdb=TPCH_Q02 */
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from
    parttbl,
    supplier,
    partsupp,
    nation,
    region
where
    p_partkey = ps_partkey
    and s_suppkey = ps_suppkey
    and p_size = 15
    and p_type like '%BRASS'
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and trim(r_name) = 'EUROPE'
    and ps_supplycost = (
        select
            min(ps_supplycost)
        from
            partsupp,
            supplier,
            nation,
            region
        where
            p_partkey = ps_partkey
            and s_suppkey = ps_suppkey
            and s_nationkey = n_nationkey
            and n_regionkey = r_regionkey
            and trim(r_name) = 'EUROPE'
    )
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
limit 100;

/* TPC_H  Query 12 - Shipping Modes and Order Priority */
select /* tdb=TPCH_Q12 */
    l_shipmode,
    sum(case
        when trim(o_orderpriority) ='1-URGENT'
            or trim(o_orderpriority) ='2-HIGH'
        then 1
        else 0
    end) as high_line_count,
    sum(case
        when trim(o_orderpriority) <> '1-URGENT'
            and trim(o_orderpriority) <> '2-HIGH'
        then 1
        else 0
    end) as low_line_count
from
    ordertbl,
    lineitem
where
    o_orderkey = l_orderkey
    and trim(l_shipmode) in ('MAIL', 'SHIP')
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and l_receiptdate >= '1994-01-01'
    and l_receiptdate < dateadd(year, 1, to_date('1994-01-01'))
group by
    l_shipmode
order by
    l_shipmode;

/* TPC_H  Query 1 - Pricing Summary Report */
select /* tdb=TPCH_Q01 */
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= dateadd(day, -90, to_date('1998-12-01'))
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;

/* TPC_H  Query 10 - Returned Item Reporting */
select /* tdb=TPCH_Q10 */
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    customer,
    ordertbl,
    lineitem,
    nation
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate >= '1993-10-01'
    and o_orderdate < dateadd(month, 3, to_date('1993-10-01'))
    and l_returnflag = 'R'
    and c_nationkey = n_nationkey
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc
limit 20;

/* TPC_H  Query 5 - Local Supplier Volume */
select /* tdb=TPCH_Q05 */
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    customer,
    ordertbl,
    lineitem,
    supplier,
    nation,
    region
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and l_suppkey = s_suppkey
    and c_nationkey = s_nationkey
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and trim(r_name) = 'ASIA'
    and o_orderdate >= '1994-01-01'
    and o_orderdate < dateadd(year, 1, to_date('1994-01-01'))
group by
    n_name
order by
    revenue desc;

/* TPC_H  Query 22 - Global Sales Opportunity */
select /* tdb=TPCH_Q22 */
    cntrycode,
    count(*) as numcust,
    sum(c_acctbal) as totacctbal
from (
    select
        substr(c_phone,1,2) as cntrycode,
        c_acctbal
    from
        customer
    where
        substr(c_phone,1,2) in
            ('13','31','23','29','30','18','17')
        and c_acctbal > (
            select
                avg(c_acctbal)
            from
                customer
            where
                c_acctbal > 0.00
                and substr (c_phone,1,2) in
                    ('13','31','23','29','30','18','17')
        )
        and not exists (
            select
                *
            from
                ordertbl
            where
                o_custkey = c_custkey
        )
    ) as custsale
group by
    cntrycode
order by
    cntrycode;

/* TPC_H  Query 4 - Order Priority Checking */
select /* tdb=TPCH_Q04 */
    o_orderpriority,
    count(*) as order_count
    from
    ordertbl
where
    o_orderdate >= '1993-07-01'
    and o_orderdate < dateadd(month, 3, to_date('1993-07-01'))
    and exists (
        select
            *
        from
            lineitem
        where
            l_orderkey = o_orderkey
            and l_commitdate < l_receiptdate
    )
group by
    o_orderpriority
order by
    o_orderpriority;

/* TPC_H  Query 11 - Important Stock Indentification */
select /* tdb=TPCH_Q11 */
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    partsupp,
    supplier,
    nation
where
    ps_suppkey = s_suppkey
    and s_nationkey = n_nationkey
    and trim(n_name) = 'GERMANY'
group by
    ps_partkey having
        sum(ps_supplycost * ps_availqty) > (
            select
                sum(ps_supplycost * ps_availqty) * 0.000003333333333
            from
                partsupp,
                supplier,
                nation
            where
                ps_suppkey = s_suppkey
                and s_nationkey = n_nationkey
                and trim(n_name) = 'GERMANY'
        )
order by
    value desc;

/* TPC_H  Query 16 - Parts/Supplier Relationship */
select /* tdb=TPCH_Q16 */
    p_brand,
    p_type,
    p_size,
    count(distinct ps_suppkey) as supplier_cnt
from
    partsupp,
    parttbl
where
    p_partkey = ps_partkey
    and trim(p_brand) <> 'Brand#45'
    and p_type not like 'MEDIUM POLISHED%'
    and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
    and ps_suppkey not in (
        select
            s_suppkey
        from
            supplier
        where
            s_comment like '%Customer%Complaints%'
    )
group by
    p_brand,
    p_type,
    p_size
order by
    supplier_cnt desc,
    p_brand,
    p_type,
    p_size;

/* TPC_H  Query 7 - Volume Shipping */
select /* tdb=TPCH_Q07 */
    supp_nation,
    cust_nation,
    l_year, sum(volume) as revenue
from (
    select
        n1.n_name as supp_nation,
        n2.n_name as cust_nation,
        extract(year from l_shipdate) as l_year,
        l_extendedprice * (1 - l_discount) as volume
    from
        supplier,
        lineitem,
        ordertbl,
        customer,
        nation n1,
        nation n2
    where
        s_suppkey = l_suppkey
        and o_orderkey = l_orderkey
        and c_custkey = o_custkey
        and s_nationkey = n1.n_nationkey
        and c_nationkey = n2.n_nationkey
        and (
            (trim(n1.n_name) = 'FRANCE' and trim(n2.n_name) = 'GERMANY')
            or (trim(n1.n_name) = 'GERMANY' and trim(n2.n_name) = 'FRANCE')
        )
        and l_shipdate between '1995-01-01' and '1996-12-31'
    ) as shipping
group by
    supp_nation,
    cust_nation,
    l_year
order by
    supp_nation,
    cust_nation,
    l_year;

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

/* TPC_H  Query 8 - National Market Share */
select /* tdb=TPCH_Q08 */
    o_year,
    sum(case
        when nation = 'BRAZIL'
        then volume
        else 0
    end) / sum(volume) as mkt_share
from (
    select
        extract(year from o_orderdate) as o_year,
        l_extendedprice * (1-l_discount) as volume,
        trim(n2.n_name) as nation
    from
        parttbl,
        supplier,
        lineitem,
        ordertbl,
        customer,
        nation n1,
        nation n2,
        region
    where
        p_partkey = l_partkey
        and s_suppkey = l_suppkey
        and l_orderkey = o_orderkey
        and o_custkey = c_custkey
        and c_nationkey = n1.n_nationkey
        and n1.n_regionkey = r_regionkey
        and trim(r_name) = 'AMERICA'
        and s_nationkey = n2.n_nationkey
        and o_orderdate between '1995-01-01' and '1996-12-31'
        and p_type = 'ECONOMY ANODIZED STEEL'
    ) as all_nations
group by
    o_year
order by
    o_year;

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
    ) as profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc;

/* TPC_H  Query 17 - Small-Quantity-Order Revenue */
select /* tdb=TPCH_Q17 */
    sum(l_extendedprice) / 7.0 as avg_yearly
from
    lineitem,
    parttbl
where
    p_partkey = l_partkey
    and trim(p_brand) = 'Brand#23'
    and trim(p_container) = 'MED BOX'
    and l_quantity < (
        select
            0.2 * avg(l_quantity)
        from
            lineitem
        where
            l_partkey = p_partkey
    );

/* TPC_H  Query 15 - Top Supplier */
select /* tdb=TPCH_Q15 */
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

</p></details>

### Clustering

<details><p>

```sql
create or replace schema clustered clone public;
use tdalpha.clustered;

alter table lineitem cluster by (l_shipdate);
alter table ordertbl cluster by (o_orderdate);

show tables like 'lineitem';
show tables like 'ordertbl';

select system$clustering_depth('lineitem') union all
select system$clustering_depth('ordertbl');

select system$clustering_information('lineitem') union all
select system$clustering_information('ordertbl');

select * from public.lineitem where l_shipdate = '1998-01-01';
select * from clustered.lineitem where l_shipdate = '1998-01-01';
```

</p></details>

