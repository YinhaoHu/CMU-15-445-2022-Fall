# Database Storage II

---

## 0 Log-Structured Merge Tree and Its Variants

The key idea of LSM tree is to replace random I/O operations during tree inserts, updates and deletes with a smaller number of sequential I/O operations.

### 0.1 Insertions on LSM tree

### 0.2 Rolling Emerges

A few pages of L[I]are merged into corresponding pages of L[i+1] at a time, and removed from L[i] . This is done whenever Li becomes close to its target size, and it results in L[i] shrinking a bit to return to its target size. 

### 0.3 Handling deletes and updates

Instead of directly fifinding an index entry and deleting it, deletion results in insertion of a new deletion entry that indicates which index entry is to be deleted.

### 0.4 The Stepped-Merge Index

The stepped-merge index has multiple trees at each level.

#### 0.4.1 Insertion Algorithm

#### 0.4.2 Lookup Operations Using Bloom Filters

### 0.5 LSM Trees For Flash Storage
