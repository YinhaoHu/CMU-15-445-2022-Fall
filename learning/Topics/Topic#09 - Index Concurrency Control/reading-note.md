# Index Concurrency Control

---

**Operation serializability:** A concurrent execution of index operations on an index is said to be serializable if there is a serialization order of the operations that is consistent with the results that each index operation in the concurrent execution sees, as well as with the final state of the index after all the operations have been executed.

There are three concurrency control techniques mentioned in the textbook:

**crabbing protocol**: lock the next node before releasing  the current node using shared mutex.

**b-link tree**: store the pointer to the next sibling in the internel node and release the current node before locking the next one(parent or child).

**key-value locking:** lock the key and value in a leaf node individually to increase the concurrency.
