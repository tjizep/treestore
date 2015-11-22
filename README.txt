treestore
=========

TuReestore MySQL Storage Engine version 0.5 (beta)
--------------------------------------------------

TuReestore is a high performance ACID compliant storage engine for MySQL 5.6.20

Features
--------

+ Collumn oriented
+ ACID Compliant through Multi version concurrency (REPEATABLE READ isolation only)
+ Global journal provides cross database consistency and durability
+ Low storage overhead and small datasize - combined with zlib or lz4 minimum of 4 times smaller data size
+ Good write performance under transactional and bulk loads, similar or exceeding row databases
+ High read performance - average increase of 200% performance on tpc-h (HammerDB) from hot cache and more up to 1000% (10x)when running from cold cache
+ Bytewise wise rotation encoding to expose low entropy to 
+ In memory compression without performance loss

Rednode (Experimental)
----------------------

+ Rednode is a multimaster node distribution architecture 
+ PAXOS based consistency protocol using timestamps
+ partitioning
+ Dataless Virtual masters can be elastically provisioned - 
  operates as a branch or as part of the 'trunk'
+ can be enabled in TuReeStore with simple config parameters

Benchmarks
----------

Machine
+ Core i7 3770s 
+ 16 GB RAM
+ Samsung SSD 850 Pro 256 GB
+ Western Digital Caviar Green 1.5 TB
+ Windows 10 x64

Software
+ Mysql 5.6.20
+ HammerDB 2.1.4
+ Treestore 0.5 beta
+ Facebook Linkbench

Load
+ Linkbench (44 000 000 rows in link table)

Results (cold start 30 % variance)
SSD
+ InnoDB 2020 ops/s
+ TuReestore 3100 ops/s
HDD
+ InnoDB 40 ops/s
+ TuReestore 1000 ops/s

Load
+ TPC-H Scale 1 all queries

Results (hot start 0.5 % variance)
+ InnoDB 76 secs 
+ Treestore 29 secs

Results (cold start 30 % variance)
+ InnoDB 75 secs
+ TuReestore 59 secs

+ note: 80% of the queries run in 12 secs on ts vs 36 secs on innodb (300% improvement)

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

+ B+ TREE indexes 
+ Hashed columns 
+ variable row size, theres no minimum row size, unused fields in a row are not stored, unlike table structures 
+ portable columns
+ 2 Level CPU Cache Aware Hash Table for unique index optimization
+ Simple and easy to change code base
+ STL like internal data structures for index and column storage
+ No NULL storage format i.e no extra bit for NULL values
+ Very Efficient sparse column storage due to no null format and runlength + differencial encoding
+ Multi threaded bulk loads - bulk loads are threaded for further performance gains
+ Simple entropy coding is used for in memory compression 
+ filter expressions are evaluated in the storage engine for better performance

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

Linux Build
-----------

These instructions are for ubuntu 14.04 in desktop mode

1. Download generic source archive for mysql 5.6.x (x=23 as of this writing)
2. Extract to folder accessible by current user i.e. /user/[current user]/Desktop/mysql-5.6.23
3. Install cmake,ccmake,g++,gcc including automake,bison
4. Install Codeblocks ide 
5. download source from [treestore clone ZIP](https://github.com/tjizep/treestore/archive/master.zip)
6. extract zip file to /home/[current user]/Desktop/treestore-master (root of zip contents)
7. create folder /home/[current user]/Desktop/mysql
8. mysql instructions:

	shell> cd ~/Desktop
	
	shell> tar zxvf mysql-VERSION.tar.gz
	
	shell> cd mysql-VERSION
	
	shell> mkdir bld
	
	shell> cd bld
	
	shell> ccmake ..  
	
	*NB!: ccmake ..*
	
	*NB!: set CMAKE_INSTALL_PREFIX=/home/[current user]/Desktop/mysql*
	
	*NB!: set CMAKE_DATADIR=/home/[current user]/Desktop/mysql/data*
	
	*NB!: press C for configure*
	
	*NB!: press g for generate and exit*
	
	shell> make 
	
	shell> make install
	
	shell> cd /home/[current user]/Desktop/mysql
	
	shell> scripts/mysql_install_db --user=[current user]
	
	shell> scripts/mysql_install_db --user=[current user]

9. This should give you a valid directory to install from
10. Open CodeBlocks ide
11. Open /home/[current user]/Desktop/treestore-master/codeblocks/treestore/treestore.cbp
12. Consult codeblocks manual and set custom variable MYSQL_REPO=/home/[current user]/Desktop/mysql-5.6.23 (right click treestore->[build options]->treestore <set variable> ok ok)
13. project->build
14. the libtreestore.so will be built to /home/[current user]/Desktop/treestore-master/treestore/codeblocks/treestore/bin/Release
15. copy libtreestore.so to /home/[current user]/Desktop/mysql/lib/plugin
16. open new terminal
17. 

	shell> cd ~/Desktop/mysql
	
	shell> gedit my.cnf
	
	paste to the bottom of the file
	
	treestore_max_mem_use = 8G
	
	treestore_efficient_text = false
	
	treestore_journal_lower_max = 12G
	
	treestore_journal_upper_max = 24G
	
	treestore_column_cache = false
	
	treestore_column_cache_factor = 0.7
	
	treestore_reduce_storage_use_on_unlock = false
	
	treestore_reduce_tree_use_on_unlock = false
	
	treestore_reduce_index_tree_use_on_unlock = false
	
	treestore_predictive_hash = false
	
	treestore_column_encoded = false
	
	treestore_use_primitive_indexes=true
	
	save
	
	exit
	
	shell> ./bin/mysql -u root
	
	mysql> install plugin treestore soname 'libtreestore.so'
	
	mysql> create database 'test_t'
	
	mysql> set default_storage_engine='treestore';
	
	mysql> use 'test_t';
	
	mysql> create table t1(c1 int);
	
	mysql> {etc...}


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
