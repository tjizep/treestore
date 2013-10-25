treestore
=========

treestore MySQL Storage Engine version 0.1

Treestore is a high performance ACID compliant storage engine for MySQL 5.6.13+

Features
--------

 1. Collumn oriented

 2. ACID Compliant through Multi version concurrency

 3. Global journal provides cross database consistency

 4. Very small data size

 5. B+ TREE indexes

 6. CPU Cache Aware Predictive Cache for join optimization

 7. Simple and easy to change code base

 8. stl like internal data structures for index and collumn storage

 9. No NULL storage 


Building
--------

Currently only MSVC 11 is a supported compiler. Future builds will be GCC 'compliant'

TODO
----

 1. Erase records not always transactionally consistent
 2. Simple Configuration
 3. Compile with GCC