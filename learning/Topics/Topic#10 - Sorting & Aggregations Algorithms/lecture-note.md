# Sorting & Aggregation Algorithms

---

## 1 Sorting

由于在关系型数据库中，数据是没有一定的顺序。所以，在执行`ORDER BY`这类显示要求排序或者`DISTINCT`, `GROUP BY`这类聚合操作潜在要求排序时，需要进行排序操作。当输入数据和排序结果都能同时放入内存缓冲区中，那么仅需要标准的排序算法即可。当涉及到与磁盘进行多次交换数据时，可以根据排序结果能否完全放在内存中可以把排序分为`Top-N Heap Sorting` 以及`External Merge Sorting`这两种。

**Top-N Heap Sorting:** 当涉及到`LIMIT N`这类的语句时，只需要维护一个大小为N的有序堆，一般来说这个堆能够在内存中。这样，就能把输出放在内存中而无需写入磁盘。

**External Merge Sorting:** 当涉及到对数据库相当大的数据集进行排序时，输入和输出都无法完全放在内存缓冲区中，通过将输入数据集分成多个`run` 然后逐次将若干个`run`排序成一个`run` ，如此循环最后得到仅有一个`run`的输出。其可分为`排序` 和 `合并` 两个阶段。有常见的四个技术：`Two-way Merge Sort`, `General Merge Sort`, `Double Buffering Optimization` , `Using B+ Tree`.

---

## 2 Aggregation

聚合操作把多个元组中的某一属性值合并为一个标量值。实现聚合的方法包括排序和哈希。

**排序实现：** 在这种实现中，应该达到先排序后聚合能够得到更好的效率。

**哈希实现：** 哈希实现一般比排序实现的效果更好，因为计算的成本相对较低。实现过程是扫描一次数据集并且在扫描的过程中维护一个哈希表，通过哈希函数将具有相同`GroupByKey`的`RunningValue`放在一起。
