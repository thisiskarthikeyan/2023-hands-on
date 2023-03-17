# Competitive Hands-On Training - 2023 Q1
# Databricks

## Login
https://accounts.cloud.databricks.com/

Username
```
tdalpha2023@gmail.com
```
or another username provided to you.

Password
```
17095ViaDelCampo!
```

AWS Account ID
```
791221762878
```

## Basic Functionality

### Token-based access

<details>

#### Databricks Personal Access Token
```bash
export DBSQLCLI_ACCESS_TOKEN="dapi6a1e919655c0970830c0871fa4741897"
```

#### AWS Session Token
```bash
aws iam create-access-key --user-name tdalpha2023@gmail.com
```
```json
{
    "AccessKey": {
        "UserName": "tdalpha2023@gmail.com",
        "Status": "Active",
        "CreateDate": "2023-01-21T16:27:36Z",
        "SecretAccessKey": "lY8rg/SJacOgG9TrANj0dP14gWJVXvr7OwTLmMBD",
        "AccessKeyId": "AKIA3QOD57M7CU37FFUC"
    }
}
```
```bash
aws sts assume-role --role-arn arn:aws:iam::791221762878:role/databricks-tpch-access-data-buckets --role-session-name "copyinto" --duration-seconds 43200 --profile tdalpha2023@gmail.com
```
```json
{
    "AssumedRoleUser": {
        "AssumedRoleId": "AROA3QOD57M7CXPYC4NLC:copyinto",
        "Arn": "arn:aws:sts::791221762878:assumed-role/databricks-tpch-access-data-buckets/copyinto"
    },
    "Credentials": {
        "SecretAccessKey": "+Rq3Cr4aQWedYJ5XcZAL519cjIsTGiFMBWDXFg/c",
        "SessionToken": "FwoGZXIvYXdzEHMaDPHU/5Y8QfPecKXC+yKtAf9O04ACd2SMJxJx7oG6yaI1HvRErjqSv02ux/LyfSnc1vKjbmTwrI2bZXwLd7HBs87JPgUb7cR3ZgnkgTuTJpWvXTD7EDZvU+CCCovrOff642bs65ZmcdGqrioyAwc9Hq12ISabeVPaKfeDMwLP7mEaayIXKGM2E8qi8qikgPEKYRWgjtWjwPF4Gzli3UlYTKQRL4NN7M9bxPF++mICEMol6PIt/ScnK8SvRwC6KIPx754GMi2+kTXEXlZW60T907lfubLwNgVXpCRxbvJW4QF7B0RM1TLGhjkF7Yb9d/rTwOs=",
        "Expiration": "2023-01-22T04:34:30Z",
        "AccessKeyId": "ASIA3QOD57M7BSU7U5E3"
    }
}
```

</details>

### Create Tables

<details>

```sql
create schema <your schema>;
use schema <your schema>;

-- nation
CREATE OR REPLACE TABLE nation (
	n_nationkey int,
	n_name varchar(25),
	n_regionkey int,
	n_comment varchar(152)
);

-- region
CREATE OR REPLACE TABLE region (
	r_regionkey int,
	r_name varchar(25),
	r_comment varchar(152)
);

-- supplier
CREATE OR REPLACE TABLE supplier (
	s_suppkey int,
	s_name varchar(25),
	s_address varchar(40),
	s_nationkey int,
	s_phone varchar(15),
	s_acctbal decimal(12,2),
	s_comment varchar(101)
);

-- partsupp
CREATE OR REPLACE TABLE partsupp (
	ps_partkey int,
	ps_suppkey int,
	ps_availqty int,
	ps_supplycost decimal(12,2),
	ps_comment varchar(199)
);

-- part
CREATE OR REPLACE TABLE parttbl (
	p_partkey int,
	p_name varchar(55),
	p_mfgr varchar(25),
	p_brand varchar(10),
	p_type varchar(25),
	p_size int,
	p_container varchar(10),
	p_retailprice decimal(12,2),
	p_comment varchar(23)
);

-- customer
CREATE OR REPLACE TABLE customer (
	c_custkey int,
	c_name varchar(25),
	c_address varchar(40),
	c_nationkey int,
	c_phone varchar(15),
	c_acctbal decimal(12,2),
	c_mktsegment varchar(10),
	c_comment varchar(117)
);

-- orders
CREATE OR REPLACE TABLE ordertbl (
	o_orderkey bigint,
	o_custkey int,
	o_orderstatus varchar(1),
	o_totalprice decimal(12,2),
	o_orderdate date,
	o_orderpriority varchar(15),
	o_clerk varchar(15),
	o_shippriority int,
	o_comment varchar(79)
) PARTITIONED BY(o_orderdate);

-- lineitem
CREATE OR REPLACE TABLE lineitem (
	l_orderkey bigint,
	l_partkey int,
	l_suppkey int,
	l_linenumber int,
	l_quantity decimal(12,2),
	l_extendedprice decimal(12,2),
	l_discount decimal(12,2),
	l_tax decimal(12,2),
	l_returnflag varchar(1),
	l_linestatus varchar(1),
	l_shipdate date,
	l_commitdate date,
	l_receiptdate date,
	l_shipinstruct varchar(25),
	l_shipmode varchar(10),
	l_comment varchar(44)
) PARTITIONED BY(l_shipdate);
```

</details>

### Loading Delta Lake (Directly from S3)

<details>

```sql
COPY INTO nation
FROM (
  SELECT 
    CAST(_c0 AS int) AS n_nationkey, 
    _c1 AS n_name, 
    CAST(_c2 AS int) AS n_regionkey, 
    _c3 AS n_comment 
  FROM 
    's3://mcg-tdc2/tpch/30gb/'
WITH (
  CREDENTIAL (
    AWS_ACCESS_KEY = 'ASIA3QOD57M7BSU7U5E3',
    AWS_SECRET_KEY = '+Rq3Cr4aQWedYJ5XcZAL519cjIsTGiFMBWDXFg/c',
    AWS_SESSION_TOKEN = 'FwoGZXIvYXdzEHMaDPHU/5Y8QfPecKXC+yKtAf9O04ACd2SMJxJx7oG6yaI1HvRErjqSv02ux/LyfSnc1vKjbmTwrI2bZXwLd7HBs87JPgUb7cR3ZgnkgTuTJpWvXTD7EDZvU+CCCovrOff642bs65ZmcdGqrioyAwc9Hq12ISabeVPaKfeDMwLP7mEaayIXKGM2E8qi8qikgPEKYRWgjtWjwPF4Gzli3UlYTKQRL4NN7M9bxPF++mICEMol6PIt/ScnK8SvRwC6KIPx754GMi2+kTXEXlZW60T907lfubLwNgVXpCRxbvJW4QF7B0RM1TLGhjkF7Yb9d/rTwOs='
    )
  )
)
FILEFORMAT = CSV
PATTERN = 'nation.*'
FORMAT_OPTIONS (
  'delimiter' = '|'
)
COPY_OPTIONS (
  'force' = 'true'
);

COPY INTO region
FROM (
  SELECT 
    CAST(_c0 AS int) AS r_regionkey, 
    _c1 AS r_name, 
    _c2 AS r_comment 
  FROM 
    's3://mcg-tdc2/tpch/30gb/'
WITH (
  CREDENTIAL (
    AWS_ACCESS_KEY = 'ASIA3QOD57M7BSU7U5E3',
    AWS_SECRET_KEY = '+Rq3Cr4aQWedYJ5XcZAL519cjIsTGiFMBWDXFg/c',
    AWS_SESSION_TOKEN = 'FwoGZXIvYXdzEHMaDPHU/5Y8QfPecKXC+yKtAf9O04ACd2SMJxJx7oG6yaI1HvRErjqSv02ux/LyfSnc1vKjbmTwrI2bZXwLd7HBs87JPgUb7cR3ZgnkgTuTJpWvXTD7EDZvU+CCCovrOff642bs65ZmcdGqrioyAwc9Hq12ISabeVPaKfeDMwLP7mEaayIXKGM2E8qi8qikgPEKYRWgjtWjwPF4Gzli3UlYTKQRL4NN7M9bxPF++mICEMol6PIt/ScnK8SvRwC6KIPx754GMi2+kTXEXlZW60T907lfubLwNgVXpCRxbvJW4QF7B0RM1TLGhjkF7Yb9d/rTwOs='
    )
  )
)
FILEFORMAT = CSV
PATTERN = 'region.*'
FORMAT_OPTIONS (
  'delimiter' = '|'
)
COPY_OPTIONS (
  'force' = 'true'
);

COPY INTO supplier
FROM (
  SELECT 
    CAST(_c0 AS int) AS s_suppkey, 
    _c1 AS s_name,
    _c2 AS s_address,
    CAST(_c3 AS int) AS s_nationkey,
    _c4 AS s_phone,
    CAST(_c5 AS decimal(12,2)) AS s_acctbal,    
    _c6 AS s_comment 
  FROM 
    's3://mcg-tdc2/tpch/30gb/'
WITH (
  CREDENTIAL (
    AWS_ACCESS_KEY = 'ASIA3QOD57M7BSU7U5E3',
    AWS_SECRET_KEY = '+Rq3Cr4aQWedYJ5XcZAL519cjIsTGiFMBWDXFg/c',
    AWS_SESSION_TOKEN = 'FwoGZXIvYXdzEHMaDPHU/5Y8QfPecKXC+yKtAf9O04ACd2SMJxJx7oG6yaI1HvRErjqSv02ux/LyfSnc1vKjbmTwrI2bZXwLd7HBs87JPgUb7cR3ZgnkgTuTJpWvXTD7EDZvU+CCCovrOff642bs65ZmcdGqrioyAwc9Hq12ISabeVPaKfeDMwLP7mEaayIXKGM2E8qi8qikgPEKYRWgjtWjwPF4Gzli3UlYTKQRL4NN7M9bxPF++mICEMol6PIt/ScnK8SvRwC6KIPx754GMi2+kTXEXlZW60T907lfubLwNgVXpCRxbvJW4QF7B0RM1TLGhjkF7Yb9d/rTwOs='
    )
  )
)
FILEFORMAT = CSV
PATTERN = 'supplier.*'
FORMAT_OPTIONS (
  'delimiter' = '|'
)
COPY_OPTIONS (
  'force' = 'true'
);

COPY INTO partsupp
FROM (
  SELECT 
    CAST(_c0 AS int) AS ps_partkey, 
    CAST(_c1 AS int) AS ps_suppkey, 
    CAST(_c2 AS int) AS ps_availqty,
    CAST(_c3 AS decimal(12,2)) AS ps_supplycost,    
    _c4 AS ps_comment 
  FROM 
    's3://mcg-tdc2/tpch/30gb/'
WITH (
  CREDENTIAL (
    AWS_ACCESS_KEY = 'ASIA3QOD57M7BSU7U5E3',
    AWS_SECRET_KEY = '+Rq3Cr4aQWedYJ5XcZAL519cjIsTGiFMBWDXFg/c',
    AWS_SESSION_TOKEN = 'FwoGZXIvYXdzEHMaDPHU/5Y8QfPecKXC+yKtAf9O04ACd2SMJxJx7oG6yaI1HvRErjqSv02ux/LyfSnc1vKjbmTwrI2bZXwLd7HBs87JPgUb7cR3ZgnkgTuTJpWvXTD7EDZvU+CCCovrOff642bs65ZmcdGqrioyAwc9Hq12ISabeVPaKfeDMwLP7mEaayIXKGM2E8qi8qikgPEKYRWgjtWjwPF4Gzli3UlYTKQRL4NN7M9bxPF++mICEMol6PIt/ScnK8SvRwC6KIPx754GMi2+kTXEXlZW60T907lfubLwNgVXpCRxbvJW4QF7B0RM1TLGhjkF7Yb9d/rTwOs='
    )
  )
)
FILEFORMAT = CSV
PATTERN = 'partsupp.*'
FORMAT_OPTIONS (
  'delimiter' = '|'
)
COPY_OPTIONS (
  'force' = 'true'
);

COPY INTO parttbl
FROM (
  SELECT 
    CAST(_c0 AS int) AS p_partkey,
    _c1 AS p_name,
    _c2 AS p_mfgr,
    _c3 AS p_brand,
    _c4 AS p_type,
    CAST(_c5 AS int) AS p_size, 
    _c6 AS p_container,
    CAST(_c7 AS decimal(12,2)) AS p_retailprice,    
    _c8 AS p_comment 
  FROM 
    's3://mcg-tdc2/tpch/30gb/'
WITH (
  CREDENTIAL (
    AWS_ACCESS_KEY = 'ASIA3QOD57M7BSU7U5E3',
    AWS_SECRET_KEY = '+Rq3Cr4aQWedYJ5XcZAL519cjIsTGiFMBWDXFg/c',
    AWS_SESSION_TOKEN = 'FwoGZXIvYXdzEHMaDPHU/5Y8QfPecKXC+yKtAf9O04ACd2SMJxJx7oG6yaI1HvRErjqSv02ux/LyfSnc1vKjbmTwrI2bZXwLd7HBs87JPgUb7cR3ZgnkgTuTJpWvXTD7EDZvU+CCCovrOff642bs65ZmcdGqrioyAwc9Hq12ISabeVPaKfeDMwLP7mEaayIXKGM2E8qi8qikgPEKYRWgjtWjwPF4Gzli3UlYTKQRL4NN7M9bxPF++mICEMol6PIt/ScnK8SvRwC6KIPx754GMi2+kTXEXlZW60T907lfubLwNgVXpCRxbvJW4QF7B0RM1TLGhjkF7Yb9d/rTwOs='
    )
  )
)
FILEFORMAT = CSV
PATTERN = 'part\.tbl.*'
FORMAT_OPTIONS (
  'delimiter' = '|'
)
COPY_OPTIONS (
  'force' = 'true'
);

COPY INTO customer
FROM (
  SELECT 
    CAST(_c0 AS int) AS c_custkey,
    _c1 AS c_name,
    _c2 AS c_address,
    CAST(_c3 AS int) AS c_nationkey,
    _c4 AS c_phone,
    CAST(_c7 AS decimal(12,2)) AS c_acctbal, 
    _c6 AS c_mktsegment,
    _c7 AS c_comment 
  FROM 
    's3://mcg-tdc2/tpch/30gb/'
WITH (
  CREDENTIAL (
    AWS_ACCESS_KEY = 'ASIA3QOD57M7BSU7U5E3',
    AWS_SECRET_KEY = '+Rq3Cr4aQWedYJ5XcZAL519cjIsTGiFMBWDXFg/c',
    AWS_SESSION_TOKEN = 'FwoGZXIvYXdzEHMaDPHU/5Y8QfPecKXC+yKtAf9O04ACd2SMJxJx7oG6yaI1HvRErjqSv02ux/LyfSnc1vKjbmTwrI2bZXwLd7HBs87JPgUb7cR3ZgnkgTuTJpWvXTD7EDZvU+CCCovrOff642bs65ZmcdGqrioyAwc9Hq12ISabeVPaKfeDMwLP7mEaayIXKGM2E8qi8qikgPEKYRWgjtWjwPF4Gzli3UlYTKQRL4NN7M9bxPF++mICEMol6PIt/ScnK8SvRwC6KIPx754GMi2+kTXEXlZW60T907lfubLwNgVXpCRxbvJW4QF7B0RM1TLGhjkF7Yb9d/rTwOs='
    )
  )
)
FILEFORMAT = CSV
PATTERN = 'customer.*'
FORMAT_OPTIONS (
  'delimiter' = '|'
)
COPY_OPTIONS (
  'force' = 'true'
);

COPY INTO ordertbl
FROM (
  SELECT 
    CAST(_c0 AS bigint) AS o_orderkey,
    CAST(_c1 AS int) AS o_custkey,
    _c2 AS o_orderstatus,
    CAST(_c3 AS decimal(12,2)) AS o_totalprice, 
    CAST(_c4 AS date) AS o_orderdate,
    _c5 AS o_orderpriority,
    _c6 AS o_clerk,
    CAST(_c7 AS int) AS o_shippriority,
    _c8 AS o_comment 
  FROM 
    's3://mcg-tdc2/tpch/30gb/'
WITH (
  CREDENTIAL (
    AWS_ACCESS_KEY = 'ASIA3QOD57M7BSU7U5E3',
    AWS_SECRET_KEY = '+Rq3Cr4aQWedYJ5XcZAL519cjIsTGiFMBWDXFg/c',
    AWS_SESSION_TOKEN = 'FwoGZXIvYXdzEHMaDPHU/5Y8QfPecKXC+yKtAf9O04ACd2SMJxJx7oG6yaI1HvRErjqSv02ux/LyfSnc1vKjbmTwrI2bZXwLd7HBs87JPgUb7cR3ZgnkgTuTJpWvXTD7EDZvU+CCCovrOff642bs65ZmcdGqrioyAwc9Hq12ISabeVPaKfeDMwLP7mEaayIXKGM2E8qi8qikgPEKYRWgjtWjwPF4Gzli3UlYTKQRL4NN7M9bxPF++mICEMol6PIt/ScnK8SvRwC6KIPx754GMi2+kTXEXlZW60T907lfubLwNgVXpCRxbvJW4QF7B0RM1TLGhjkF7Yb9d/rTwOs='
    )
  )
)
FILEFORMAT = CSV
PATTERN = 'orders.*'
FORMAT_OPTIONS (
  'delimiter' = '|'
)
COPY_OPTIONS (
  'force' = 'true'
);

COPY INTO lineitem
FROM (
  SELECT 
    CAST(_c0 AS bigint) AS l_orderkey,
    CAST(_c1 AS int) AS l_partkey,
    CAST(_c2 AS int) AS l_suppkey,
    CAST(_c3 AS int) AS l_linenumber,
    CAST(_c4 AS decimal(12,2)) AS l_quantity, 
    CAST(_c5 AS decimal(12,2)) AS l_extendedprice, 
    CAST(_c6 AS decimal(12,2)) AS l_discount, 
    CAST(_c7 AS decimal(12,2)) AS l_tax, 
    _c8 AS l_returnflag,
    _c9 AS l_linestatus,
    CAST(_c10 AS date) AS l_shipdate,
    CAST(_c11 AS date) AS l_commitdate,
    CAST(_c12 AS date) AS l_receiptdate,
    _c13 AS l_shipinstruct,
    _c14 AS l_shipmode,
    _c15 AS l_comment 
  FROM 
    's3://mcg-tdc2/tpch/30gb/'
WITH (
  CREDENTIAL (
    AWS_ACCESS_KEY = 'ASIA3QOD57M7BSU7U5E3',
    AWS_SECRET_KEY = '+Rq3Cr4aQWedYJ5XcZAL519cjIsTGiFMBWDXFg/c',
    AWS_SESSION_TOKEN = 'FwoGZXIvYXdzEHMaDPHU/5Y8QfPecKXC+yKtAf9O04ACd2SMJxJx7oG6yaI1HvRErjqSv02ux/LyfSnc1vKjbmTwrI2bZXwLd7HBs87JPgUb7cR3ZgnkgTuTJpWvXTD7EDZvU+CCCovrOff642bs65ZmcdGqrioyAwc9Hq12ISabeVPaKfeDMwLP7mEaayIXKGM2E8qi8qikgPEKYRWgjtWjwPF4Gzli3UlYTKQRL4NN7M9bxPF++mICEMol6PIt/ScnK8SvRwC6KIPx754GMi2+kTXEXlZW60T907lfubLwNgVXpCRxbvJW4QF7B0RM1TLGhjkF7Yb9d/rTwOs='
    )
  )
)
FILEFORMAT = CSV
PATTERN = 'lineitem.tbl.9*'
FORMAT_OPTIONS (
  'delimiter' = '|'
)
COPY_OPTIONS (
  'force' = 'true'
);

select 'customer' entity, count(*) from customer union all
select 'lineitem' entity, count(*) from lineitem union all
select 'nation' entity, count(*) from nation union all
select 'ordertbl' entity, count(*) from ordertbl union all
select 'parttbl' entity, count(*) from parttbl union all
select 'partsupp' entity, count(*) from partsupp union all
select 'region' entity, count(*) from region union all
select 'supplier' entity, count(*) from supplier order by 2;
```

</details>

### Running Queries (TPC-H 30GB Stream)

<details>

```sql
set use_cached_result = false;

select /* tdb=TPCH_Q06 */
    sum(l_extendedprice*l_discount) as revenue
from
    lineitem
where
    l_shipdate >= '1993-01-01'
    and l_shipdate < dateadd(year, 1, to_date('1993-01-01', 'yyyy-MM-dd'))
    and l_discount between 0.02 - 0.01 and 0.02 + 0.01
    and l_quantity < 25;

select /* tdb=TPCH_Q19 */
	sum(l_extendedprice* (1 - l_discount)) as revenue
from
	lineitem,
	parttbl
where
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#11'
		and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		and l_quantity >= 4 and l_quantity <= 4 + 10
		and p_size between 1 and 5
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#33'
		and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		and l_quantity >= 19 and l_quantity <= 19 + 10
		and p_size between 1 and 10
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#35'
		and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		and l_quantity >= 25 and l_quantity <= 25 + 10
		and p_size between 1 and 15
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	);

select /* tdb=TPCH_Q13 */
    c_count, count(*) as custdist
from (
    select
        c_custkey,
        count(o_orderkey) as c_count
    from
        customer left outer join ordertbl on
            c_custkey = o_custkey
            and o_comment not like '%unusual%accounts%'
    group by
        c_custkey
    )as c_orders
group by
    c_count
order by
    custdist desc,
    c_count desc;

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
    and trim(n_name) = 'IRAN'
group by
    s_name
order by
    numwait desc,
    s_name
limit 100;

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
    and l_shipdate >= '1995-11-01'
    and l_shipdate < dateadd(month, 1, to_date('1995-11-01', 'yyyy-MM-dd'));

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
    trim(c_mktsegment) = 'FURNITURE'
    and c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate < '1995-03-13'
    and l_shipdate > '1995-03-13'
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate
limit 10;

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
                    p_name like 'blush%'
            )
            and ps_availqty > (
                select
                    0.5 * sum(l_quantity)
                from
                    lineitem
                where
                    l_partkey = ps_partkey
                    and l_suppkey = ps_suppkey
                    and l_shipdate >= '1995-01-01'
                    and l_shipdate < dateadd(year, 1, to_date('1995-01-01', 'yyyy-MM-dd'))
            )
    )
    and s_nationkey = n_nationkey
    and trim(n_name) = 'KENYA'
order by
    s_name;

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
    and p_size = 32
    and p_type like '%NICKEL'
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and trim(r_name) = 'ASIA'
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
            and trim(r_name) = 'ASIA'
    )
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
limit 100;

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
    and trim(l_shipmode) in ('TRUCK', 'AIR')
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and l_receiptdate >= '1994-01-01'
    and l_receiptdate < dateadd(year, 1, to_date('1994-01-01', 'yyyy-MM-dd'))
group by
    l_shipmode
order by
    l_shipmode;

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
    l_shipdate <= dateadd(day, -90, to_date('1998-12-01', 'yyyy-MM-dd'))
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;

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
    and o_orderdate >= '1994-03-01'
    and o_orderdate < dateadd(month, 3, to_date('1994-03-01', 'yyyy-MM-dd'))
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
    and trim(r_name) = 'AFRICA'
    and o_orderdate >= '1993-01-01'
    and o_orderdate < dateadd(year, 1, to_date('1993-01-01', 'yyyy-MM-dd'))
group by
    n_name
order by
    revenue desc;

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
            ('24', '22', '34', '21', '32', '25', '30')
        and c_acctbal > (
            select
                avg(c_acctbal)
            from
                customer
            where
                c_acctbal > 0.00
                and substr (c_phone,1,2) in
                    ('24', '22', '34', '21', '32', '25', '30')
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

select /* tdb=TPCH_Q04 */
    o_orderpriority,
    count(*) as order_count
    from
    ordertbl
where
    o_orderdate >= '1994-12-01'
    and o_orderdate < dateadd(month, 3, to_date('1994-12-01', 'yyyy-MM-dd'))
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
    and trim(n_name) = 'CHINA'
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
                and trim(n_name) = 'CHINA'
        )
order by
    value desc;

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
    and trim(p_brand) <> 'Brand#25'
    and p_type not like 'MEDIUM POLISHED%'
    and p_size in (2, 7, 24, 30, 27, 50, 23, 45)
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
            (trim(n1.n_name) = 'ARGENTINA' and trim(n2.n_name) = 'PERU')
            or (trim(n1.n_name) = 'PERU' and trim(n2.n_name) = 'ARGENTINA')
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
                sum(l_quantity) > 313
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

select /* tdb=TPCH_Q08 */
    o_year,
    sum(case
        when nation = 'PERU'
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
        and p_type = 'SMALL BRUSHED BRASS'
    ) as all_nations
group by
    o_year
order by
    o_year;

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
        and p_name like '%frosted%'
    ) as profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc;

select /* tdb=TPCH_Q17 */
    sum(l_extendedprice) / 7.0 as avg_yearly
from
    lineitem,
    parttbl
where
    p_partkey = l_partkey
    and trim(p_brand) = 'Brand#53'
    and trim(p_container) = 'SM BOX'
    and l_quantity < (
        select
            0.2 * avg(l_quantity)
        from
            lineitem
        where
            l_partkey = p_partkey
    );

with /* tdb=TPCH_Q15 */ revenue0 as (
    select
        l_suppkey as supplier_no,
        sum(l_extendedprice * (1 - l_discount)) as total_revenue
    from
        lineitem
    where
        L_SHIPDATE >= '1994-02-01' AND
        L_SHIPDATE < dateadd(month, 3, to_date('1994-02-01', 'yyyy-MM-dd'))
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

## Performance Features

### Delta Lake

<details>

```sql
describe detail ordertbl;

describe history ordertbl;
```

</details>

### Performance Tuning

<details>

```sql
optimize partsupp zorder by ps_partkey;

select * from partsupp limit 1000;

describe history partsupp;
```

</details>

## Usability Features

### Filters, Parameters & Visualizations

<details>

Filter on NATION

```sql
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
        and p_name like '%frosted%'
    ) as profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc;
```

Parameter by P_CONTAINER

```sql
select /* tdb=TPCH_Q17 */
    sum(l_extendedprice) / 7.0 as avg_yearly
from
    lineitem,
    parttbl
where
    p_partkey = l_partkey
    and trim(p_brand) = 'Brand#53'
    and trim(p_container) = '{{ p_container }}'
    and l_quantity < (
        select
            0.2 * avg(l_quantity)
        from
            lineitem
        where
            l_partkey = p_partkey
    );
```

```
SM DRUM
SM CASE
JUMBO BAG
LG PKG
SM PACK
JUMBO DRUM
LG BOX
WRAP PKG
LG DRUM
```

</details>

