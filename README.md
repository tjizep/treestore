treestore
=========

TuReestore MySQL Storage Engine version 0.31 (beta)
--------------------------------------------------

TuReestore is a high performance ACID compliant storage engine for MySQL 5.6.20

Features
--------

+ Collumn oriented
+ ACID Compliant through Multi version concurrency (REPEATABLE READ isolation only)
+ Global journal provides cross database consistency and durability
+ Low storage overhead and small datasize - combined with zlib or lz4 minimum of 3 times smaller data size
+ Good write performance under transactional and bulk loads, similar or exceeding row databases
+ High read performance - average increase of 200% performance on tpc-h (HammerDB) from hot cache and more up to 1000% (10x)when running from cold cache
+ In memory compression without performance loss

Benchmarks
----------

Machine
+ Core i7 3770s 
+ 16 GB RAM
+ Western Digital Green 1.5 TB hardrive
+ Windows 8.1 x64

Software
+ Mysql 5.6.20
+ HammerDB 2.1.4
+ Treestore 0.31 beta

Load
+ TPC-H Scale 1 all queries

Results (hot start 0.5 % variance)
+ InnoDB 76 secs 
+ Treestore 25 secs

Results (cold start 30 % variance)
+ InnoDB 660 secs
+ TuReestore 59 secs

Linux Installation and Notes
----------------------------

+ find libtreestore.so under codeblocks/treestore/bin/Release
+ put it in your plugin directory
+ execute INSTALL PLUGIN TREESTORE SONAME 'libtreestore.so'
+ Treestore is **case** sensitive so a table with name **EMPLOYEES** does
  not equal **employees**

Microsoft Windows Installation
------------------------------

+ Download and install MySQL 5.6.17 x64 from [MySQL downloads](http://dev.mysql.com/downloads/mysql/)
+ Locate 'plugin' folder in MySQL installation directory
+ Copy treestore.dll from x64/Release to the previously located plugin folder
+ Install MSVC 11 x64 runtime dependencies from [MSVC Redistributable download](http://www.microsoft.com/en-za/download/details.aspx?id=30679)
+ Start MySQL
+ Using the client run the statement INSTALL PLUGIN TREESTORE SONAME 'treestore.dll'
+ use create table syntax with a suffix like engine='treestore'

Technical
---------

+ B+ TREE indexes AND tables
+ variable row size, theres no minimum row size, unused fields in a row are not stored, unlike table structures 
+ portable columns
+ 2 Level CPU Cache Aware Predictive Hash Table for unique index optimization
+ Simple and easy to change code base
+ STL like internal data structures for index and column storage
+ No NULL storage format i.e no extra bit for NULL values
+ Very Efficient sparse column storage due to no null format and runlength + differencial encoding
+ Multi threaded bulk loads - bulk loads are threaded for further performance gains
+ Simple entropy coding is used for in memory compression 
+ filter expressions are evaluated in the storage engine for better performance
+ Mini B-tree static page stores most frequently used keys in continous memory area (i.e. 'harmonic' keys in binary  search). 
  The CPU cache is usually an LRU (least recently used) type which throws away the harmonic keys or the middle key of each binary search iteration.
  This reduces the average CPU cache miss rate by about 50% while overall performance
  is improved by 20% on loads exceeding CPU cache size until there are no more fully associative or L3 CPU cache
  space available for mini pages.

Configuration
-------------

+ **treestore_mem_use**
The mysql system variable treestore_mem_use can be used to limit the treestore memory use
+ **treestore_journal_lower_max**
Used to set lower limit at which the journal is merged back to collum storages
+ **treestore_journal_upper_max**
If the journal reaches this limit then no new transactions are started and the journal is merged back
+ **treestore_efficient_text**
Defaults to false. If set to TRUE treestore will use much less memory (usually half) at the expense of performance. 
Experiment - your milage may vary
+ **treestore_predictive_hash**
Defaults to true. enables/disables the predictive hash can improve performance on some queries
+ **treestore_column_cache**
Defaults to true. enables/disables the column cache, should be left enabled unless ultra low memory use is required
+ **treestore_reduce_tree_use_on_unlock**
Defaults to false. releases shared tree cached pages on every unlock also releases readlocks every time. Its a good option for memory constrained environments 
+ **treestore_current_mem_use** can be queried to retrieve the current treestore memory use

Time Performance versus Space performance
-----------------------------------------

This table supplies configuration options for sacrificing performance for less memory use. try different options for your workload. 

| Performance+memuse   | treestore_predictive_hash           | treestore_column_cache  | treestore_reduce_tree_use_on_unlock  | treestore_reduce_index_tree_use_on_unlock |
| -------------------- | -----------------------------------:| ------------------------:|-------------------------------------:| -----------------------------------------:|
| Lowest               | false                               | false                    | true                                 | true                                      |
| Low                  | false                               | false                    | true                                 | false                                     |
| Medium               | false                               | true                     | true                                 | false                                     |
| High                 | false                               | true                     | false                                | false                                     |
| Highest              | true                                | true                     | false                                | false                                     |

+ The **Medium** option has the best performance versus memory use characteristics
+ The **Low** option will use less than double the space use on disk which is usually **very** low on compressible data
+ The **Lowest** option will only cache the encoded blocks on disk which is usually **extremely** small 

Changes
-------
0.31 beta

1. Performance Improvements 10% on average
2. Linux builds, added libtreestore under codeblocks/treestore/bin/Release folder

0.30 beta

1. Added b-tree CPU cache conscious optimization 

+ similar to optimization described here [Cache Oblivious B-Trees](http://erikdemaine.org/papers/FOCS2000b/paper.pdf)

Although the implementation in treestore is a lot simpler there is still a 20% random read rate improvement 
on loads exceeding CPU cache size. Loads fitting completely in L3 cache does not benefit.
This optimization also allows for larger page sizes which in turn will improve compression on disk. 
Transactional performance for small changes will not benefit from larger page sizes.

2. Made Predictive hash, lock free on reads

3. Added back test code

Fixes:

1. Fix B-tree erase issues
2. Fix B-tree bulk load sorting optimization


0.29 beta

1. Added conditional push downs to improve table scan performance by 3 to 5 times
2. Added primitive indexes for reduced memory use

0.28 beta

1. Added in memory column compression improving memory use by a lot
2. do not load columns which are not specified in a query
3. compression improvement 

0.27 beta

1. Changed decimal format for improved performance 10% - 100 % in some cases that involve only decimal fields
2. fixed decimal index order bug
3. Configuration additions

Building
--------

GCC and MSVC is supported under windows amd64 and linux amd64 only (no 32 bit)


TODO FOR GA 0.3x
----------------

 1. Replication
 2. DONE: Multi threaded writes
 
TODO
----

 1. Integrate with MariaDB
 2. Expose Virtual col for fast table offsets
 3. DONE: Resolve some memory release issues
 4. Text index
 5. Spatial index
 6. embedded interface to javascript and lua allowing manipulation and queries without a SQL interpreter
 7. internal mysql function to executed embedded lua and javascript interfaces
