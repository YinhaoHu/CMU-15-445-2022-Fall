# Hash Tables

---

## 1 Basic Concepts

**Bucket:** A unit of storage that can store one or more records. For in-memory hash indices, it could be a linked list of index entries or more records. For disk-based indices, it would be a linked list of disk blocks. 

**Hash file organization:** Buckets stored the actual records instead of pointers.

**Overflow chaining:** Handle the case of multiple records in a bucket.

**Closed addressing:** Hash indices using overflow chaining.

**Bucket overflow:** Occur when there are no free space to store a new record. This issues is fixed by using *overflow buckets*.

**Skew**: A distribution where a few buckets store so many records while the rest buckets store only a few records.

**Static hashing/Dynamic hasing:** The number of buckets is(not) fixed when the index created.

---

## 2 Hash Indices

### 2.1 Hash Function

Hash function needs to be designed to have the two properties:

* The distribution is uniform.

* The distribution is random.

## 2.2 Bucket Overflow

Bucket overflow can occur for the two reasons:

* Insufficient buckets

* Skew

This problem can be solved by two hash structures: *open addressing* and *closed addressing* which are used in dynamic hashing and static hashing.

### 2.3 Static and dynamic hashing

**Static Hashing:** Allocate more buckets to store the new record or index when the bucket has no free space to store the new record or index.

**Dynamic Hashing:** Allow the hash function to be modified to accommadate the growth or shrinkage of the database.

**Extendiable hashing:** (See the related discussion in lecture slides and textbook. The animation of the related operations helps understanding)

Compared to static hashing, extendiable hashing has these advantages and disadvantages:

**Advantages:**

* Performance does not degrade as the file grows.

* Space overhead is minimul.

* No buckets are needed to be reserved for future growth.

**Disadvantages:**

* Lookup involves an additional level of indirection.

* The cost of doubling the size of bucket address table.
