# Memory Management

---

## 01 Data-Directory Storage

**Data Directory** is the structure  in which the metadata about the database stores. The data directory data often be stored in the database as several tables and these tables are often read into memory when the DBMS starts up.

---

## 02 Database Buffer

A major goal of database system is to minimize the frequency of transferring data between disk and main memory.

### Buffer manager

**Buffer** is the part for the storage of the copies of the blocks in disk.**Buffer manager** is the subsystem responsible for the allocation of buffer space.

If the requested block is in the buffer, the buffer manager retures the memory address of the requested block instantly and it reads the block from the disk and return the memory address of it if the block is not in the buffer.

There might be no available space for reading the block from the disk into buffer. So *eviction* is necessary which writes out the chosen block based on certain *replacement strartegy* like LRU scheme and read the target block into buffer.

To make concurrent operations correct, the **pin/unpin** operations are needed to avoid/permit the specific block to be written/read. *Exclusive and shared* locks are taken in different situations in the buffer.

*Output of blocks* can happen when there is no space in buffer or there are still some space. *Force output* is used in some cases.

### Buffer-Replacement Strategies

Least Recently Used(LRU): If a block must be replaced, the least recently referenced block is replaced.

Most Recently Used(MRU): If a block must be replaced, the most recently referenced block is replaced.

Database systems may have information regarding at least the short-term future.

### Reordering of Writes and Recovery

To optimize the disk seek time, the DBMS might reoreder the writes. But this might incur some problems like data corruption. To solve that when recovering, the DBMS has a log system to ensure that the database can recovery in the event of crash, even in writes reordering.
