# Storage Models & Compression

---

## 01 Data Warehousing

### 01.1 Components of a Data Warehouse

**When and how to gather data:** 

* Source-driven architecture: the data sources transmit new information either periodically or continually.

* Destination-driven architecture: the data warehouse sends request for new data to the srouces.

* Synchronous replication is expensive so that many data warehouses do not use them.

**What schema to use:** Data warehouse performs schema integration and converts the data to the integrated schema before they are stored.

**Data transformation and cleansing:** Data cleasing is the process of correcting and preprocessing the data. Some cleasing operations are: fuzzy lookup, deduplication and householding.

**How to propagate updates:** The updates are straightforward if the relations at the warehouse are exactly the same as those at source.

**What to summarize:** Answer many queries by maintaining just summary data obtained by aggregation on a relation, rather than maintaining the entire relation.

The steps involved in getting data into a data warehouse are: extract, transform and load(ETL tasks). In current generation of data warehouse, the steps can also be ELT. 

### 0.1.2 Multidimensional Data and Warehouse Schemas

**Warehouse schema** can be typically classified as *fact tables* and *dimension tables*.

* Fact table: record information about individual events. The attributes of fact tables can be classified as either *measure attibute* and *dimension attribute*. Measure attributes store quantitative information. In contrast, dimension attributes are dimensions upon which measure attributes, and summaries of measure attributes, are grouped and viewed.

* Dimension table: dimension attributes are usually short identififiers that are foreign keys into other tables called dimension tables.

**Multidimensional data:** Data that can be modeled using dimension attributes and measure attributes.

**Star schema:** A schema, with a fact table, multiple dimension tables, and foreign keys from the fact table to the dimension tables.

**Snowflake schema:** Have multiple levels of dimension tables.

### 0.1.3 Database Support for Data Warehouses

Key difference between database designed for transaction-processing system and the one designed for data warehouse: 

* Transaction-processing database needs to support many small queries,which may involve updates in addition to reads. 

* Data warehouses typically need to process far fewer queries, but each query accesses a much larger amount of data. Most importantly, data warehouse does not need to pay for concurrence control.

---

## 02 Column-Oriented Storage

**Definition:** Each attribute of a relation is stored separately, with values of the attribute from successive tuples stored at successive positions in the file.

**Alias:** Columnar storage.

**Benifits:**

* Reduced I/O

* Improved CPU cache performance

* Improved compression

* Vector processing

**Drawbacks:**

* Cost of tuple reconstruction

* Cost of tuple update and delete

* Cost of decompression

---

## Note for class

Understand again the column oriented storage after learning compression.
