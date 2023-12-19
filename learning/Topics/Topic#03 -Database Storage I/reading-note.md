# Database Storage I

---

## 01 Overview of Physical Storage Media

Concerned attributes of physical storage medias are: `speed`, `cost` and `reliability`.

Primary Storage                                : `Cache` and `Main Memory`

Secondary Storage(Online Storage): `Flash Memory` and `Magnetic Disk`

Tertiary Storage(Offline Storage)    : `Optical Disk` and `Magnetic Tape`

---

## 02 File Organization

In database, a *file* is organized as a sequence of records. Each file is logically partitioned into fixed-length *blocks*, which are the units of both storage allocation and data transfer.

Three concered issues when storing records are: fix-length records, variable-length record and large object.

---

## 03 Organization of Records in Files

**Heap File Organization**: a record may be stored anywhere in the file corresponding to the relation. *free-space map* is the data structure to track which blocks have free space to store the new record.

**Sequential File Organization**: is designed for efficient processing of records in sorted order based on some search key.

**Multitable Clustering File Organization**: two or more relations are stored in a single file based on the specific key and thus improve the `join` query performance.

**Partitioning**: partition a relation into several small relations on the basis of an attribute value. This can reduce the overhead when inserting a record into a big relation and can allow the relation to be stored on the different storage devices.

---

## Summary

Storage devices can be divided into two categories which are *non-volatile storage* and *volatile storage*. Volatile storage is byte-addressable and has higher access rate while non-volatile storage is block/page addressable and has lower access rate.

There are three tools for DBMS to manage the database. *Buffer pool* manages the data movement back and forth between disk and memory. *Execution engine* takes the responsibility to execute query. *Storage manager* is responsible for managing a database's file.

One upper level goal of DBMS is similar to the Virtual Memory of operating system. But DBMS does not implement its function using and hence gives itself better control and performance.

The DBMS organizes the database across one or more files in fixed-size blocks of data as *pages*. Some database system requires the pages to be self-contained. The DBMS takes measures to ensure the write request for one page is atomic.

Different DBMSs organizes the pages in different ways. One page storage architecture is heap file organization. A *heap file* is an unordered collection of pages 
with tuples that are stored in random order. Two methods are token to locate one specific page which are linked list and page directory.

*Page layout* includes a header contains the meta information about the data stored on this page. There are two main approaches to laying out data in pages: slotted pages and log structured.

*Tuple layout* is essentially a sequence of bytes. For tuples, the tuple header, tuple data and tuple unique identifier are needed. There is another form of tuple layout which is denormalized tuple data.
