# 主题三：查询处理与优化(Query Processing&Optimization)

---

## 查询处理(Query Processing)

查询是向数据库系统获得数据的请求。例如，SQL查询。解析器和翻译器首先将查询转换为关系代数表达式。然后，优化器结合统计数据将该表达式优化得到查询执行计划。最后，执行引擎对执行计划进行求值，得到查询结果。

### 算子(Operators)

**排序算子(Sort Operator)** 通常使用外部排序来实现，由排序和归并两个阶段组成。当排序的键有B+树索引时，可以直接使用B+树索引。

**聚合算子(Aggregation Operator)** 通常有哈希或者排序两种方法来实现。

**连接算子(Join Operator)** 有简单嵌套循环连接、块嵌套循环连接、索引嵌套循环连接、哈希连接、排序归并连接实现方式。一般来说，哈希连接是性能最好的连接算法。

### 处理模型(Processing Model)

**迭代器模型(Iterator Model)** 亦称火山模型或流水线模型，是最常用的查询处理模型。一个查询执行计划的运算过程是：根结点算子调用子结点算子的Next函数得到数据，子结点算子又继续向下调用Next函数得到数据，直到叶结点算子从数据库中获得数据。这样的运算过程使得查询执行是流水线化的。sort这样的算子必须要获得子结点的所有数据才能输出，这样的算子称为流水线打破者。

**物化模型(Materialization Model)** 是一种特殊的迭代器模型。查询计划中的一个结点立刻从所有的子结点中得到所有数据并处理，之后将所有结果返回。该模型适合于OLTP负载。

**向量化模型(Vectorization Model)** 是一种特殊的迭代器模型。查询计划中的一个节点从子结点中一次性得到一批数据并处理，之后将该批数据产生的结果输出。一批数据的大小受到查询的性质和硬件的影响(SIMD指令)。该模型适合于OLAP负载。

处理模型的处理方向可以是自顶向下或者自底向上。

## 并行化(Parallelism)

在并行化数据库系统中，并行处理模型包括：一个线程一个工作者、一个进程一个工作者。

### 查询间并行

> 见主题六：分布式系统

### 查询内并行

**算子内并行(Intra-operator Parallelism)** 是将一个算子的输入数据集分割为若干个子集后在子集中并行地使用该算子的并行方式。

**算子间并行(Inter-operator Parallelism)** 是同时执行若干个算子的并行方式。

**交换算子(Exchange Operator)** 是数据库系统用于封装并行处理的算子。其阻止数据库系统在该算子完成前执行该算子结点之上的算子。通常有：聚合，分发，重分割三种类型。

---

## 查询优化(Query Optimization)

*优化器(Optimizer)* 依据逻辑查询计划生成高效的物理查询计划。优化器通常使用基于开销估计的优化或者基于规则的优化策略并且结合统计数据来自底向上或者自顶向下地执行优化过程。在基于开销估计的优化策略中，优化器枚举若干等价的物理查询计划，通过柱状图和取样等了解每个计划的开销。

度量一个查询计划开销的指标包括：CPU时间、I/O操作数、使用的内存和网络传输开销。
