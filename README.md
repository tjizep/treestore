treestore
=========

treestore MySQL Storage Engine version 0.1

Treestore is a high performance ACID compliant storage engine for MySQL 5.6.13+

Features
=========

Collumn oriented
ACID Compliant through Multi version concurrency
Global journal provides cross database consistency
Very small data size
B+ TREE indexes
CPU Cache Aware Predictive Cache for join optimization
Simple and easy to change code base
stl like internal data structures for index and collumn storage
No NULL storage 


Building
========

Currently only MSVC 11 is a supported compiler. Future builds will be GCC 'compliant'

TODO
====

Erase records
Simple Configuration
Compile with GCC