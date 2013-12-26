treestore
=========

TuReestore MySQL Storage Engine version 0.26 (beta)
--------------------------------------------------

TuReestore is a high performance ACID compliant storage engine for MySQL 5.6.13+

Features
--------

+ Collumn oriented
+ ACID Compliant through Multi version concurrency (READ COMMITTED isolation only)
+ Global journal provides cross database consistency and durability
+ Low storage overhead and small datasize - combined with zlib or lz4 minimum of 3 times smaller data size
+ Good write performance under transactional and bulk loads
+ High read performance - always more than 200% performance on tpc-h (HammerDB) and more, much moar when running from cold cache

Technical
---------

+ B+ TREE indexes AND tables
+ variable row size, theres no minimum row size, unused fields in a row are not stored, unlike table structures 
+ portable collumns
+ 2 Level CPU Cache Aware Predictive Hash Table for unique index optimization
+ Simple and easy to change code base
+ STL like internal data structures for index and collumn storage
+ No NULL storage format i.e .no extra bit for NULL values
+ Very Efficient sparse collumn storage due to no null format and runlength + differencial encoding
+ Multi threaded bulk loads - bulk loads are threaded for further performance gains

Configuration
-------------

+ *treestore_mem_use*
The mysql system variable treestore_mem_use can be used to limit the treestore memory use
+ *treestore_journal_lower_max*
Used to set lower limit at which the journal is merged back to collum storages
+ *treestore_journal_upper_max*
If the journal reaches this limit then no new transactions are started and the journal is merged back
+ *treestore_efficient_text*
Defaults to FALSE. If set to TRUE treestore will use much less memory (usually half) at the expense of performance. 
Experiment your milage may vary

Building
--------

GCC and MSVC is supported under windows amd64 and linux amd64 only (no 32 bit)

Benchmarks
----------

Machine
+ Core i7 3770s 
+ 16 GB RAM
+ Western Digital Green 1.5 TB hardrive
+ Windows 8.1 x64

Software
+ Mysql 5.6.13
+ HammerDB 2.1.4
+ Treestore 0.2 beta

Load
+ TPC-H Scale 1 all queries

Results (hot start 0.5 % variance)
+ InnoDB 76 secs 
+ Treestore 32 secs

Results (cold start +-30% variance)
+ InnoDB 660 secs
+ TuReestore 180 secs

TODO FOR GA 0.3
---------------

 1. Replication
 2. DONE: Multi threaded writes
 
TODO
----

 1. Integrate with MariaDB
 2. Expose Virtual col for fast table offsets
 3. Resolve some memory release issues
 4. Text index
 5. Spatial index
