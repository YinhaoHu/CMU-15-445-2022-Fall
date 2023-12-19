# Indexing

---

## 01 Basic Concepts

Two basic kinds of indices:

* **Ordered Indices:** Based on a sorted ordering of the values.

* **Hash indices:** Based on a uniform distribution of values across a range of buckets. 

Factors to evaluate an index technique:

* Access types

* Access time

* Insertion time

* Deletion time

* Space overhead

**Search key:** An attribute or set of attributes used to look up records in a fifile.

---

## 02 Ordered Indices

Each index structure is associated with a search key. An *ordered index* stores the value of the search keys in sorted order and associates with each search key the records that contain it. 

**Clustering Index:** If the file containing the records is sequentially ordered, a clustering index is an index whose search key also defines the sequential order of the file.

**Secondary Index:** Indices whose search key specifies an order different from the sequential order of the file. It must be dense, with an index entry for every search-key value, and a pointer to every record in the file. *Nonunique search key* is the kind of key that has more than one records containing it. The drawbacks of this technique are:

* It may take more time to access a record because of the extra indirection level.

* It may waste a lot of space if there are very few records with some search key and a whole block was allocated to it. 

**Index Entry:** consists of a search-key value and pointers to one or more records with that value as their search-key value.

**Dense Index:** An index entry appears for every search-key value in the file.

**Sparse Index:** An index entry appears only for some of the search-key values in the file.

**Multilevel indices:** Indices with two or more levels.

**Composite Search Key:** The search key that contains more than one attributes.

---

## 03 B+-Tree Index Files

B+-Tree is a balance tree in which every path from the root to the leaf is same. To construct a B+-tree, the quality and order requirements needs to be satisfied.

**Quality requirement:**

* Each leaf node: number of values should be between ⌈(n − 1)∕2⌉ and n-1.

* Each nonleaf node: number of pointers should be between ⌈ n∕2 ⌉ and n.

* Root node: number of pointers should be between 2 and n.

**Order requirement:** The values in a leaf node are ordered.

---

## 04 B+-Tree Extensions

* B+-Tree File Organization

* Secondary Indices and Record Reloaction

* Indexing strings

* Bulk Loading of B+-Tree Indices

* B-Tree Index File

* Indexing on Flash Storage

* Indexing in Main Memory
