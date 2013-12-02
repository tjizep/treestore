treestore
=========

TuReestore MySQL Storage Engine version 0.2 (beta)
--------------------------------------------------

TuReestore is a high performance ACID compliant storage engine for MySQL 5.6.13+

Features
--------

 1. Collumn oriented

 2. ACID Compliant through Multi version concurrency (READ COMMITTED isolation only)

 3. Global journal provides cross database consistency and durability

 4. Very small data size

 5. B+ TREE indexes AND tables

 6. variable row size, theres no minimum row size, unused fields in a row arent stored unlike table structures 

 7. portable collumns

 8. 2 Level CPU Cache Aware Predictive Hash Table for unique index optimization

 9. Simple and easy to change code base

 10. STL like internal data structures for index and collumn storage

 11. No NULL storage format i.e .no extra bit for NULL values

 12. Very Efficient sparse collumn storage due to 11 and differencial encoding

 13. High performance - always more than 200% performance on tpc-h (HammerDB) and more, much moar when running from cold cache

Configuration
-------------

The mysql system variable treestore_mem_use can be used to limit the treestore memory use

Building
--------

GCC and MSVC is supported under windows amd64 and linux amd64 only (no 32 bit)

Benchmarks
----------

Machine
+ Core i7 3770s 
+ 16 GB RAM
+ Western Digital Green 1.5 TB hardrive

Load
TPC-H Scale 1 all queries

Results (hot start 0.5 % variance)
InnoDB 76 secs 
Treestore 32 secs

Results (cold start +-30% variance)
Inno DB 660 secs
TuReestore 180 secs

TODO FOR GA 0.3
---------------

 1. Interpolation encoding on collumn id
 2. Replication
 3. Multi threaded writes
 
 TODO
 ----

 1. Integrate with MariaDB
 2. Expose Virtual col for fast table offsets
 3. Resolve some memory release issues
 4. Text index
 5. Spatial index
