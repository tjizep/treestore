explain
select max(extract(year from o_orderdate)) as o_year, sum(l_extendedprice * (1 - l_discount)) as volume
from PART, SUPPLIER, LINEITEM, ORDERS, CUSTOMER, NATION n1, NATION n2, REGION 
where p_partkey = l_partkey and 
s_suppkey = l_suppkey and 
l_orderkey = o_orderkey and 
o_custkey = c_custkey and 
c_nationkey = n1.n_nationkey and 
n1.n_regionkey = r_regionkey and 
s_nationkey = n2.n_nationkey and 
r_name = 'AMERICA' and 
o_orderdate between date '1995-01-01' and date '1996-12-31' and p_type = 'PROMO POLISHED TIN'