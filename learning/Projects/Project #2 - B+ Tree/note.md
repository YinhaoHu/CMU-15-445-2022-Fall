# Project #2 - B+ Tree

---

## 提示

* 使用Profiler做性能分析

* 采用防御性编程方式

---

## 任务 #1 - B+ Tree Pages

本任务是实现B+树所需要的页类。

**BPlusTreePage** 是`BPlusTreeInternalPage` 和 `BPlusTreeLeafPage` 的父类。包括两个派生类共同的必要数据成员和函数成员。包含的数据成员有：当前大小、最大大小、页类型、页ID、父节点页ID以及日志序列号。

**BPlusTreeInternalPage** 是B+树内部节点类。实现要点：

* 第一个键是无效键，搜索从第二个键开始。

* 每个对象都应该是半满的。

* 删除时，两个半满内部节点可以被合并。插入时，一个全满内部节点可以被分割。

**BPlusTreeLeafPage** 是B+树的叶节点类。它的键数和值数是相同的。下一节点的位置存储在Header中。

在实现过程中需要注意：

* 对象`BPlusTreeInternalPage` 和 `BPlusTreeLeafPage` 的构造均由`BufferPoolManager`的 `FetchPage` 所返回的`Page`对象`reinterpret_cast`而来。

* 对象`BPlusTreeInternalPage` 和 `BPlusTreeLeafPage`的`max_size_`并不相同，原因在于前者的value_type是32-bits的page_id_t，而后者是64-bits的RID.

* Fetch获得一个页并且完成了对该页的读写操作后，需要unpin。

---

## 任务 #2 - B+ Tree Data Structure

本任务主要是实现B+树数据结构与其查找、删除与插入算法。

就树性质而言，整棵树存储的是`唯一键` 。某一结点会变化的情况包括分割与合并。其中，分割的原因有两个：1) 在叶节点插入元素 <u>后</u>，当前大小等于节点允许的最大大小(也就是，查看的时候 <u>不能够</u> 看到叶节点的大小等于最大大小)；2)在内部节点插入元素 <u>前</u>，当前大小等于节点允许的最大大小（也就是，查看到的时候 <u>能够</u> 看到叶节点大小等于最大大小）。此外，根的ID可能变，调用`BPlusTree::UpdateRootPageId`来更新根的ID。

---

## 检查点 #1 - 小结

检查点1的难点在于怎么正确处理插入与删除时的分割与合并问题。参考教材的伪代码描述的算法可以严格定义插入与删除的语义。由于复杂的插入与删除涉及到的变量不少，比如在删除一个内部节点的键后导致两个节点合并，这个时候不仅需要更新父节点存储的键值对以及更新子节点的父节点页ID。所以，需要采用防御性编程技术以及更多的DEBUG日志信息便于出现BUG时快速定位到问题以及预防BUG的产生。

在完成后续任务时，充分记录DEBUG日志以及ASSERT。

---

## 任务 #3 - Index Iterator

> 开始日期：11月5日；结束日期：11月6日；日估用时：1.5h。

本任务是实现B+ Tree的迭代器，其被用于遍历树的每一个叶的每一个键值对。支持的操作见`src/include/storage/index/index_iterator.h` 。另外，需要实现`src/include/storage/index/b_plus_tree.h`中的begin和end函数。

基本的想法是，维护一个记录树的当前页的page_id、当前页的值索引 index以及BufferPoolManager的指针。定义如果当前页的下一页ID为INVALID_PAGE_ID ，那么Iterator::IsEnd()返回true。每次解引用时，均从BufferPoolManager中获取页并返回，这简单的实现保证了的指针的解引用语义。

可能会想到迭代器失效问题，比较std::vector和std::map为例说明该问题（此任务目前不考虑）。

对于std::vector，有两种操作会使得迭代器失效：

* 插入(insert): 
  
  * 如果插入不会改变vector的capacity大小，那么仅仅使得插入处及其之后的迭代器失效。
  
  * 如果插入会改变vector的capacity大小，从而使得vector的内存空间重分配，那么所有的迭代器都会失效。

* 擦除(erase): 
  
  * 所有迭代器均会失效。

对于std::map，由于其是树形结构，无需所有元素的内存位置是连续的，插入和擦除并不会影响操作前的所有迭代器。

---

## 任务 #4 - Concurrent Index

该任务要求使用transaction存储获得闩的页以及存储删除页的ID。使用Page提供的接口来处理上闩、开闩。另外，额外保护根ID。 

基本策略是采用课本上的Basic Crabbing Latching悲观锁策略。观察到Benchmark的数据，叶节点和内部节点的大小都是254，插入和删除的个数分别是400000，200000。这就造成了在Benchmark的过程中会产生很大的B+树但树的深度仅有2。在此情况下，采用乐观锁策略会更好。即使发生了不常见的叶节点不安全情况，重新进行悲观搜索的代价也很少。

---

## 总结

在实现本项目的正确性的过程中，通过观察B+树的情况、足够的日志信息以及断言，查明代码到底发生了什么，从而快速定位到问题并解决。在实现并发控制的时候，正确的上锁需要我们理解数据之间的依赖关系。例如，在实施乐观锁策略的过程中，先获得子节点的锁然后再释放当前节点的锁，这样才能避免目的子节点在当前节点锁释放到子节点加锁的时间窗中发生变化的情况。

在多线程日志记录中，可以参考THREAD_DEBUG_LOG宏的封装。保证多线程情况下每个线程的日志能够正确记录。

在优化过程中，可以记录下Benchmark的工作负载，然后利用Profiler针对性优化大多数情况。
