# 项目一：缓冲池管理器(Buffer Pool Manager)

---

## 任务一：可扩展哈希(Extendible Hash Table)

可扩展哈希维护着一个目录、全局深度、堆的数量以及每个堆（包括实际存储的键与局部深度）。该任务的核心问题在于如何正确的处理插入时引起的堆分裂情况。在处理该情况时，可能会在处理当需要分裂堆时，有多个目录项指向待分裂堆而堆分裂后相关信息更改不全。

---

## 任务二：LRU-K 替换策略(LRU-K Replacement Policy)

### 设计类

有关类的UML图在LRUKReplace.drawio中。

### 保证线程安全

LRU-K替换策略的读写可以分为是对`entryTable_` 整体的读写和对`entryTable_` 中某一项的读写。前者的具体情况包含对`entryTable_` 进行的*增加项*, *删除项* 以及*访问项* 三个操作，注意，对某一项的记录变更由*访问项* 这一操作来完成。后者的具体情况包含对某一项中<u>访问记录</u>的*变更* 与 *访问* 两个操作。

### 后期优化考虑

假定工作负载会导致Entry的历史记录容器大多数情况下会有K个记录。我们可以用大小为K的分配在堆上的动态循环数组来管理记录。这样做能够减少动态大小容器的内存分配开销。 

### 细节处理

* 时间戳：记录frame的访问时间选择std::chrono::high_resolution_clock而非time().

* 并发问题：`std::unique_lock`改为`std::shared_lock`后，会在并发测试用例(多个线程同时emplace，但能够保证多个线程的frame_id参数不会一致)中有可能出现`entry_table_[frame_id]->RecordAccess()` 访问非法地址的情况。分析发现，问题出在一个线程使`entry_table_`在rehash的过程中，另一个线程访问了`entry_table_`中正在rehash的key。

```cpp
auto LRUKReplacer::RecordAccess(frame_id_t frame_id) -> void {
  std::unique_lock lock(latch_);

  if (entry_table_.count(frame_id) == 0) {
    entry_table_.emplace(frame_id, std::make_shared<Entry>(k_, timer_));
  }
  entry_table_[frame_id]->RecordAccess();
}
```

**小结：** 需要加强对常见容器的实现原理的了解。

---

## 任务三：缓冲池管理器实例(Buffer Pool Manager Instance)

### 分析

该任务是使`BufferPool`, `BufferPoolManagerInstance` 和 `DiskManager` 这三个类能正确交互。需要实现的类是`BufferPoolManagerInstance` 。首先建立该类的模型（通过任务要求的接口即可），然后分析出该类主要需要`pages_` 和 `disk_manager_` 在元信息(`page_table_` ,`free_list_`)以及辅助成员`replacer_`的帮助下来实现数据的流动（参考Lecture#06的Lecture Note中Figure 2)。

### 设计类

任务要求所给的类设计已经大致足够，需要额外的读写锁即可。

### 保证线程安全

由于部分成员已经是线程安全的，我们只需要对部分非线程安全的成员（`pages_` 本身, `pages_`的每一个成员和 `free_list_`）加以保护。

### 细节处理

* `Page`对象： `Page` 对象是此任务中需要频繁处理的基本对象，在每一个成员函数实现中，需要小心处理的`Page` 对象涉及到的变化。

---

## 总结

该项目要求实现数据库系统中存储层部分的内存缓冲池模块。内存缓冲池是数据库系统与磁盘存储交互的中间模块。它通过维护着一个定长的帧区域、帧号与页号的映射关系以及一个处理页交换的交换器来实现获取页、新建页以及删除页等操作，抽离出数据库系统对页面的管理。

导致项目整体完成时间较长的原因类型以及实例和处理办法如下：

* 对概念的误解以及不透彻。
  
  * 实例：对ExtendiableHashTable的SplitBucket的理解不够到位（没有考虑到在Split时有多个目录索引指向同一个待分割桶）、对LRUKReplacer的Evict策略的误解、对BufferPoolManagerInstance的UnPinPg的理解不到位（假如之前已经有了标记为脏页，之后标记为不脏，这不应该改变页的dirty属性）
  
  * 处理：在实现之前应该尽可能全面的确保对概念的理解是正确的。为了确保概念理解正确，可以综合Slides、Andy授课内容、Lecture Note以及测试用例。在必要的排查问题时，可以参考Solution代码。以及FetchPage的理解。
  
  * 实例：在尝试提高ExtendiableHashTable的并行能力时，认为一个函数调用了一系列原子的操作，那么这个函数会得到并发正确的结果，花费了大量的时间去做锁的优化。
  
  * 处理：项目完成处理应该先保证逻辑正确，即使是加粗粒度锁。当确认并发能力是瓶颈时才做细粒度锁优化。另外，需要系统学习如何保证数据结构是线程安全的以及充分发挥其性能。

* 优化方面把控不准确。
  
  * 实例：此前我认为BufferPoolManagerInstance的性能瓶颈在并行性欠缺，最终与Solution代码进行比对发现是Evict的复杂度问题。
  
  * 处理：在不确定Benchmark的假定负载时，我们应该先充分优化算法的时间复杂度以及尽可能优化其中的常数部分。为了能更好把握代码的性能，还需要对STL的常见容器有一定了解，特别是时间复杂度方面。

* 急于实现。
  
  * 实例：在实现BufferPoolManagerInstance时，未能对概念足够熟悉便尝试写，这加长了Debug的时间。
  
  * 处理：在开始着手编码前务必熟悉需求、仔细设计。

结合上述出现的问题，在完成下一个Project时考虑遵循这样的流程：`仔细阅读任务要求->理出相关概念并熟悉它们—>整体设计组织代码,选择适当的数据结构与算法->实现出结构良好、可读性高的代码(此时无需考虑深度优化,如加细粒度锁,读写锁)->完成正确性测试->分析性能瓶颈并优化->总结项目中遇到的问题与收获` 另外，在完成项目时，写下文档记录过程中遇到的问题、思考的心路、总结和收获。
