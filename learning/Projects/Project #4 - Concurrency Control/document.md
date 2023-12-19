# Project #4 - Concurrency Control

---

## Task #1 - Lock Manager

### 阅读内容

* `transaction.h`和`lock_manager.h`的接口和成员变量。

* `[LOCK_NOTE]`、`[UNLOCK_NOTE]` 以及LockManager的函数注释。

* 关于并发控制的概念，包括锁与事务隔离等级。

### 完成目标

* 如何正确的响应给定的隔离等级和锁模式。

* 如何实现锁升级。

### 基本想法

关键词：锁模式、隔离等级、多粒度、锁升级

### 易错点

* 在等待队列中的锁升级请求可能并非是被第一个唤醒，如果使用`std::conditional_variable::notify_one`。

设想table-0的锁请求队列：(txn1,S,granted)->(txn2,S,granted)->(txn3,X,not-granted)。此时，如果txn申请锁升级为Exclusive模式，那么队列变为：(txn2,S,granted) -> (txn1,X,not-granted) -> (txn3,X,not-granted)。请注意，在此时，如果txn2释放了锁并且使用`std::conditional_variable::notify_one`，那么被唤醒的事务并非txn1，而是txn3！一个可行的解决方法是，使用`std::conditional_variable::notify_all`，然后让事务判断自己是否是期望被唤醒的事务即可。

### 细节理解

* 锁升级：

---

## Task #2 - Deadlock Detection

### 易错点

* 避免Detector线程与事务线程发送死锁。

* 条件变量的锁

---

## Task #3 - Concurrent Query Execution

### 基本想法

上锁：根据事务的隔离等级正确的上锁。失败则中止事务，抛出 `ExecutionException` 异常。

开锁：失败则中止事务。

需要更新write set。

Executor中需要对事务上锁。

在Executor中，只能在满足不会导致事务进入shrinking状态的前提下，开锁。这是受到一个事务中查询不止一个的约束。
