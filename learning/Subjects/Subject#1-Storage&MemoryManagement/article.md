# 主题一： 存储和内存管理

---

## 存储

在面向磁盘的中，数据库通常被存储在一个或多个文件上。存储管理器管理数据库文件，负责数据库管理系统的I/O以及对数据库文件的解释。DBMS主要有行存储和列存储这两种存储模型，前者常用于OLTP DBMS中，而后者常用于OLATP DBMS中。DBMS支持很多的内置数据类型，包括数字数据类型、字符串数据类型和时间数据类型等，同时也有一些DBMS支持用户定义的数据类型。数据压缩技术能够提高存储空间的利用率和性能，因此其被广泛使用。

### 设备

存储设备是计算机用于存储数据和指令的部件。在存储层次体系中，离CPU越近的存储设备传输速度越大，存储容量也越小，每GB的价格也越高。有易失性存储器和非易失性存储器两种。

* 易失性存储器：数据会在断电后丢失的存储器。传输的单位是字节。例子：CPU缓存、主存储器(通常用内存指代)。

* 非易失性存储器：数据在断电后不会丢失的存储器。传输单位是页/块。例子：磁盘，固态硬盘(SSD)。

磁盘的访问代价由等待、寻道和传输这三部分组成，连续访问较随机访问有更好的性能。

后续的讨论是面向磁盘的数据库系统，用内存来指主存储器。

### 单位

**数据库** 通常是以单个文件或者文件层次体系的形式存储在磁盘中。文件由数据库管理系统的*存储管理器(storage manager)* 来管理。存储管理器解释数据库文件并且负责页的读写。数据库文件有堆文件组织、顺序文件组织、B+树文件组织等。

**页** 存储管理器把数据库文件解释为页。每一页由*元数据(metadata)* 和*有效载荷(payload)* 组成。元数据包括页大小、校验和等。有效载荷是存储元组、索引的部分。页的布局有*插槽页(slotted page)* 和*日志结构页(log-structured page)* 两种。

**元组** 一个字节序列。由数据库管理系统将该字节序列解释为属性类型和值。一个元组由*元组头(tuple header)* 和 *元组数据(tuple data)* 组成。元组头包含该元组的元数据（元组标识符、大小和是否分配标识等），元组数据包含该元组的真实数据。

### 模型

存储模型指定了数据库管理系统存储元组的方式。

* N-元存储模型(N-Ary Storage Model, NSM)
  
  * 别称：行存储。
  
  * 优点：
    
    * 快速的插入、删除、更新一个元组。
    
    * 对于需要整个元组的查询友好。
  
  * 缺点：当查询需要大量元组的某一属性值时，该模型表现不好，因为多读了若干没必要的属性值。

* 分解存储模型(Decomposition Storage Model, DSM)
  
  * 别称：列存储。
  
  * 优点：
    
    * 当查询需要大量元组的某一属性值时，表现友好。
    
    * 利于压缩数据和数据分析
  
  * 缺点：
    
    * 对于涉及整个元组的读取查询并不友好。

### 数据表示

数据库管理系统的*数据表示(data representing)* 指定其如何解释字节为值。常见的数据类型包括：

* 整数：INTEGER, BIGINT,SMALLINT等

* 不定精度数：REAL, DOUBLE PRECISION

* 用户指定的定精度数：DECIMAL, NUMERIC

* 日期和时间：DATE, TIME, TIMESTAMP

* 用户定义的数据类型：部分DBMS支持，如PostgreSQL支持，但MySQL不支持。

*系统目录(system catalog)* 是数据库管理系统内部的关于系统的元数据，记录了表、索引、聚合函数、语言等。

### 压缩

在面向磁盘的数据库系统中，I/O开销是该系统的主要瓶颈。通过数据压缩可以减少数据存储需要的容量，从而减少I/O开销。压缩技术广泛用于面向磁盘的数据库系统中，MySQL的InnoDB和PostgreSQL的TOAST均使用压缩技术。压缩技术通常需要在压缩速率和压缩率间做出选择。

一般来说，当数据分布不均(部分相同值出现很多次，有的值出现少数次)或者元组的属性间存在高度相关性时，使用数据压缩可以有较好的效果。

常见的压缩技术包括：通用压缩算法、增量压缩、字典映射等。

---

## 内存管理

为了能让数据库管理系统能对内存有更好的控制，其向操作系统申请一块内存区域作为自己的缓冲池，并用自己的替换策略来管理。

### 缓冲池

#### 介绍

*缓冲池(Buffer Pool)* 是缓存DBMS访问过的表数据和索引数据的主存区域。*缓冲池管理器(Buffer Pool Manager)* 负责将页写入磁盘和从磁盘读入页，它需要决定什么时候进行页的交换。缓冲池管理器的维护着页表、脏页和页的pin count等元信息，以便于其决定什么时候换出什么页。

#### 替换策略

当需要将页换出时，缓冲池管理器需要使用一定的策略来决定将什么页换出到磁盘中，其策略的好坏影响着DBMS的性能。将频繁访问的页保留在内存中是DBMS性能优化的一个重要方面。一般内存管理器使用的是LRU算法来决定换出什么页。一个另外的选择是*基于时钟的替换策略(replacement policy based on  clock)*

与基本的LRU算法相比，LRU-K算法具有更好的时间局部性以及更高的适应性。通过利用LRU-K算法的更好的时间局部性，DMBS可以处理一些连续访问问题。通过利用其更高的适应性，DBMS可以根据不同的访问模式调整K值，从而提高缓存命中率。

在LRU-K算法中，K值的选择影响着缓存命中率的高低，而其对不同的访问模式又有不同的最优解，也就是说K值的选择取决于我们应用的访问模式。一个朴素的办法是针对我们应用的访问模式对不同的K值进行实验测量，最后选择效果最好的K值。另一个更好的办法是对K值进行动态的调整，基本想法是，在一个时期内，DBMS记录下当前K值下的缓存命中率，然后调整K值在此收集数据，收集多组后进行选择最好的K值。过了一段时间在此进行这样的过程。

#### 优化技术

缓冲池管理器的一个优化技术是*预获取(pre-fetching)*，MySQL支持该技术。该技术基本想法是先读入一个页集合，然后缓冲池管理器在查询正在被处理的时候再读入另一个页集合。例如，对于一个`select * from table` ，如果table有N个页，可以先读入N/2个页，然后执行引擎在该N/2个页上进行处理。而缓冲池管理器在此期间将另一个N/2个页读入缓冲池。由此减少了处理第二个N/2个页所需要的等待时间。另外的优化技术包括*缓冲池旁路(buffer pool bypass)* 和*多缓冲区(multiple buffer pools)* 等。

#### 其他内存模型

DBMS不仅仅需要为缓存元组和索引的缓冲池，还需要为其他的数据提供缓冲区，包括：

* 日志缓冲区：用于缓存日志。例如，MySQL提供的Log Buffer。

* 变化缓冲区(Change Buffer)：用于缓存对辅助索引的改变。例如，MySQL的Change buffer。

### DBMS内存管理与OS内存管理的比较

选择DBMS的专用内存缓冲池管理器的原因有：

* DBMS可以选择更合适页替换策略，例如LRU-K算法。

* DBMS的性能具有可预测性，从而可以更好地优化和实现DBMS。

不幸的是，在现代的操作系统中，操作系统对虚拟内存有完全的控制，DBMS的缓冲池的页可能因为整个系统面临着内存压力而被换出到交换空间中。因此在此情况下，当DBMS需要写出缓冲池的页时，会产生更多的开销（正常情况下是将主存的页写入磁盘，而现在需要从交换空间读入主存，然后再写出）。

DBMS不一定都有自己的缓冲池管理器，比如PostgreSQL就直接使用OS提供的page cache，而MySQL使用自己的缓冲池管理器。
