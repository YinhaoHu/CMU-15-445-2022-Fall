# 主题二：索引(Index)

---

当一个表有相当多的元组时，为了访问某一个值所在的元组而遍历整张表这一过程十分低效。为了高效地查找某一具体的元组，我们使用索引来辅助查找。

## 基本概念

*索引(Index)* 是用于辅助快速检索具有特定属性值的元组的数据结构。如果索引存储的键是有序的，那么这一索引被称为*有序索引(ordered indices)*。另一种索引类型是*哈希索引(hash indices)*。在这一部分中列出常见的关于有序索引的概念。

* 与索引顺序和建索引文件的顺序有关的
  
  * 聚簇索引(clustering index)：索引定义的搜索键的顺序也同时是建索引的文件的元组排序规则。
  
  * 辅助索引(secondary index)：索引定义的搜索键的顺序与文件的顺序不一样，通常是让索引项指向聚簇索引项而非直接指向元组。
  
  辅助索引项指向举措索引项的一个原因是：如果表的许多元组位置变更，例如B+树文件组织的叶节点发生分割。那么最坏情况下会导致每个位置变更的元组所在的辅助索引的叶节点都不一样，这会导致很多的随机I/O，从而产生很大的额外开销。通过让辅助索引项指向聚簇索引项可以解决该问题。

* 索引值与表中元组的对应关系
  
  * 稠密索引(dense index)：文件中每一个搜索键的值在索引中都有索引项。
  
  * 稀疏索引(sparse index)：文件中不是每一个搜索键的值在索引中都有索引项。

## 哈希索引(Hash Index)

使用哈希函数来建立的索引称为*哈希索引(Hash Index)* 。给定一个键，哈希函数通过计算后返回一个整数值。可以从两个指标评价哈希函数的好坏：1）哈希函数计算速率；2）哈希值的冲突率。当前最好的哈希函数是Facebook的XXHash3。

*静态哈希(Static Hash)* 的哈希表大小是固定的，当存储的值大于允许大小后，需要从新开始重建一个更大的哈希表，代价比较高。常见的静态哈希策略包括：线性探测哈希(Lear Probing Hashing)、杜鹃哈希(Cuckoo Hashing)和劫富济贫哈希(Robin Hood Hashing)。

*动态哈希(Dynamic Hash)* 的哈希表大小是不固定的，允许较低代价的更改哈希表的大小。常见的动态哈希策略而包括：可扩展哈希(Extendiable Hashing)，线性哈希(Linear Hashing)和链式哈希(Chained Hashing)。

哈希索引适用于只需要使用键的相等运算符的情况（例如键值存储系统）以及提供的键需要是完全的，也就是说不能只提供前缀来查找某一值。哈希索引在数据库系统内部广泛使用，例如内存池管理器的页表。

哈希索引的并发控制比较简单，有两种实现方式：1）槽闩锁(slot latches)，例如可扩展哈希每个桶有一个读写闩锁；2）页闩锁(page latches)。

## B+树索引(B+ Tree Index)

使用B+树数据结构来建立的索引称为*B+树索引(B+ Tree Index)* 。B+树是一个平衡树并且允许插入、删除和查找操作的时间复杂度在最坏和平均情况下都是O(LogN)。

B+树适用于：1）需要使用键的比较运算符的范围查找情况，例如，`SELECT * FROM students WHERE grade < 60`；2）需要使用键的前缀匹配的情况，例如，`SELECT * FROM students WHERE name LIKE 'H%'`。B+树索引是建立表索引的最佳选择。

基本的并发控制策略是：Basic Crabbing Locking。使用乐观锁策略可以得到更高的并发度。

## 索引数据结构的比较(Comparison In Index Data Structures)

在建立表索引时，选择B+树而非哈希表是因为哈希表提供的索引优化能力有限，仅仅对点查询能够起到优化效果。但在真实的关系型数据库系统负载中，范围查询并不少见，而B+树索引能够在范围查询中提供更高的性能，因为B+树存储的键值对均在叶结点中查找，而节点内部是有序的，底层的所有叶节点可以视为一个有序链表。

选择B+树而非B树的原因包括：

* B+树的高度更低，因为内部节点每个键值对的值都是指向子节点的指针，从而有更低的磁盘I/O操作数；

* B+树的叶节点成链表结构而B树的叶节点不成链表结构，这使得B+树范围查询更高效。

* B+树的更具有缓存亲和性。
