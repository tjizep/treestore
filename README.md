treestore
=========

treestore MySQL Storage Engine version 0.2
------------------------------------------

Treestore is a high performance ACID compliant storage engine for MySQL 5.6.13+

Features
--------

 1. Collumn oriented

 2. ACID Compliant through Multi version concurrency (READ COMMITTED isolation only)

 3. Global journal provides cross database consistency and durability

 4. Very small data size

 5. B+ TREE indexes AND tables

 6. variable row size

 7. portable collumns

 8. 2 Level CPU Cache Aware Predictive Hash Table for unique index optimization

 9. Simple and easy to change code base

 10. stl like internal data structures for index and collumn storage

 11. No NULL storage format, no extra bit for NULL values


Building
--------

Currently only MSVC 11 is a supported compiler. Future builds will be GCC 'compliant'

TODO
----

 1. Simple Configuration
 2. Compile with GCC