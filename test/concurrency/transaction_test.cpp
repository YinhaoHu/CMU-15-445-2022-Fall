//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_test.cpp
//
// Identification: test/concurrency/transaction_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction.h"

#include <atomic>
#include <cstdio>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "catalog/table_generator.h"
#include "common/bustub_instance.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_engine.h"
#include "execution/executor_context.h"
#include "execution/executors/insert_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/nested_index_join_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "gtest/gtest.h"
#include "test_util.h"  // NOLINT
#include "type/value_factory.h"

#define VAR_STRING(var) #var

#define EXECUTE_SQL(sql, writer)                                       \
  std::cerr << fmt::format("\tExecute SQL: {}\n", sql) << std::fflush; \
  bustub_->ExecuteSql(sql, writer);                                    \
  std::cerr << fmt::format("\tSQL: {} is executed.\n", sql) << std::fflush;

#define EXECUTE_SQL_TXN(sql, writer, txn)                                                                 \
  std::cerr << fmt::format("\tExecute SQL by txn {}: {}\n", txn->GetTransactionId(), sql) << std::fflush; \
  bustub_->ExecuteSqlTxn(sql, writer, txn);                                                               \
  std::cerr << fmt::format("\tSQL: {} is executed by txn {}.\n", sql, txn->GetTransactionId()) << std::fflush;

#define CONCAT(a, b) a##b

#define MAKE_SS_WRITER(number)          \
  std::stringstream CONCAT(ss, number); \
  auto CONCAT(writer, number) = SimpleStreamWriter(CONCAT(ss, number), true)

namespace bustub {

class TransactionTest : public ::testing::Test {
 public:
  // This function is called before every test.
  void SetUp() override {
    ::testing::Test::SetUp();
    bustub_ = std::make_unique<BustubInstance>("executor_test.db");
  }

  // This function is called after every test.
  void TearDown() override { remove("executor_test.db"); };

  std::unique_ptr<BustubInstance> bustub_;
};

// --- Helper functions ---
void CheckGrowing(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::GROWING); }

void CheckShrinking(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::SHRINKING); }

void CheckAborted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::ABORTED); }

void CheckCommitted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::COMMITTED); }

void CheckTxnRowLockSize(Transaction *txn, size_t shared_size, size_t exclusive_size) {
  EXPECT_EQ(txn->GetSharedLockSet()->size(), shared_size);
  EXPECT_EQ(txn->GetExclusiveLockSet()->size(), exclusive_size);
}

// Prefixes: ABLE, DISABLED.

// NOLINTNEXTLINE
TEST_F(TransactionTest, SimpleInsertRollbackTest) {
  // txn1: INSERT INTO empty_table2 VALUES (200, 20), (201, 21), (202, 22)
  // txn1: abort
  // txn2: SELECT * FROM empty_table2;

  auto noop_writer = NoopWriter();
  bustub_->ExecuteSql("CREATE TABLE empty_table2 (x int, y int);", noop_writer);

  auto *txn1 = bustub_->txn_manager_->Begin();
  bustub_->ExecuteSqlTxn("INSERT INTO empty_table2 VALUES(200, 20), (201, 21), (202, 22)", noop_writer, txn1);
  bustub_->txn_manager_->Abort(txn1);
  delete txn1;
  auto *txn2 = bustub_->txn_manager_->Begin();
  std::stringstream ss;
  auto writer2 = SimpleStreamWriter(ss, true);
  bustub_->ExecuteSqlTxn("SELECT * FROM empty_table2", writer2, txn2);
  EXPECT_EQ(ss.str(), "");
  bustub_->txn_manager_->Commit(txn2);
  delete txn2;
}

// NOLINTNEXTLINE
TEST_F(TransactionTest, SimpleDeleteRollbackTest) {
  auto noop_writer = NoopWriter();
  std::stringstream ss0;
  auto writer0 = SimpleStreamWriter(ss0, true);
  EXECUTE_SQL("CREATE TABLE test_table (val int);", noop_writer);
  EXECUTE_SQL("INSERT INTO test_table VALUES(100),(200),(300)", noop_writer);
  EXECUTE_SQL("SELECT * FROM test_table", writer0);

  auto *txn1 = bustub_->txn_manager_->Begin();
  EXECUTE_SQL_TXN("DELETE FROM test_table WHERE val = 100", noop_writer, txn1);
  bustub_->txn_manager_->Abort(txn1);
  delete txn1;
  auto *txn2 = bustub_->txn_manager_->Begin();
  std::stringstream ss;
  auto writer2 = SimpleStreamWriter(ss, true);
  EXECUTE_SQL_TXN("SELECT * FROM test_table", writer2, txn2);
  EXPECT_EQ(ss.str(), ss0.str());
  bustub_->txn_manager_->Commit(txn2);
  delete txn2;
}

// NOLINTNEXTLINE
TEST_F(TransactionTest, DirtyReadsTest) {
  // txn1: INSERT INTO empty_table2 VALUES (200, 20), (201, 21), (202, 22)
  // txn2: SELECT * FROM empty_table2;
  // txn1: abort

  auto noop_writer = NoopWriter();
  EXECUTE_SQL("CREATE TABLE empty_table2 (colA int, colB int)", noop_writer);
  auto *txn1 = bustub_->txn_manager_->Begin(nullptr, IsolationLevel::READ_UNCOMMITTED);
  EXECUTE_SQL_TXN("INSERT INTO empty_table2 VALUES (200, 20), (201, 21), (202, 22)", noop_writer, txn1);

  auto *txn2 = bustub_->txn_manager_->Begin(nullptr, IsolationLevel::READ_UNCOMMITTED);
  std::stringstream ss;
  auto writer2 = SimpleStreamWriter(ss, true);
  EXECUTE_SQL_TXN("SELECT * FROM empty_table2", writer2, txn2);

  EXPECT_EQ(ss.str(), "200\t20\t\n201\t21\t\n202\t22\t\n");

  bustub_->txn_manager_->Commit(txn2);
  delete txn2;

  bustub_->txn_manager_->Abort(txn1);
  delete txn1;
}

// NOLINTNEXTLINE
TEST_F(TransactionTest, RepeatableReadTest) {
  bustub_->GenerateTestTable();
  using namespace std::chrono_literals;
  auto noop_writer = NoopWriter();
  bustub_->ExecuteSql("CREATE TABLE test_table (col int)", noop_writer);
  auto *txn1 = bustub_->txn_manager_->Begin(nullptr, IsolationLevel::REPEATABLE_READ);
  auto *txn2 = bustub_->txn_manager_->Begin(nullptr, IsolationLevel::REPEATABLE_READ);
  std::stringstream ss1;
  std::stringstream ss2;
  std::stringstream ss3;
  auto writer1 = SimpleStreamWriter(ss1, true);
  auto writer2 = SimpleStreamWriter(ss2, true);
  auto writer3 = SimpleStreamWriter(ss3, true);
  std::thread txn1_thread([&]() {
    bustub_->ExecuteSqlTxn("INSERT INTO test_table VALUES (111), (222), (333)", noop_writer, txn1);
    bustub_->ExecuteSqlTxn("SELECT * from test_table", writer1, txn1);
    std::this_thread::sleep_for(300ms);
    bustub_->ExecuteSqlTxn("SELECT * from test_table", writer2, txn1);
    std::this_thread::sleep_for(200ms);
    bustub_->ExecuteSqlTxn("SELECT * from test_table", writer3, txn1);
  });
  std::thread txn2_thread([&]() {
    std::this_thread::sleep_for(200ms);
    bustub_->ExecuteSqlTxn("INSERT INTO test_table VALUES (444)", noop_writer, txn2);
  });
  std::this_thread::sleep_for(400ms);
  bustub_->txn_manager_->Abort(txn2);
  std::this_thread::sleep_for(200ms);
  bustub_->txn_manager_->Commit(txn1);
  txn1_thread.join();
  txn2_thread.join();
  EXPECT_EQ(ss1.str(), ss2.str());
  EXPECT_EQ(ss3.str(), ss2.str());
  delete txn1;
  delete txn2;
}

// NOLINTNEXTLINE
TEST_F(TransactionTest, DebugTest) {
  bustub_->GenerateTestTable();
  using namespace std::chrono_literals;
  auto noop_writer = NoopWriter();
  bustub_->ExecuteSql("CREATE TABLE test_table (col int)", noop_writer);
  bustub_->ExecuteSql("INSERT INTO test_table VALUES (000)", noop_writer);
  auto *txn1 = bustub_->txn_manager_->Begin(nullptr, IsolationLevel::READ_COMMITTED);
  auto *txn2 = bustub_->txn_manager_->Begin(nullptr, IsolationLevel::READ_COMMITTED);
  std::cerr << fmt::format("{}:id={},{}:id={}\n", VAR_STRING(txn1), txn1->GetTransactionId(), VAR_STRING(txn2),
                           txn2->GetTransactionId());
  bustub_->ExecuteSqlTxn("SELECT * FROM test_table", noop_writer, txn1);
  bustub_->txn_manager_->Commit(txn1);
  std::cerr << fmt::format("{}:id={} commit\n", VAR_STRING(txn1), txn1->GetTransactionId());
  bustub_->ExecuteSqlTxn("INSERT INTO test_table VALUES (111)", noop_writer, txn2);
  bustub_->txn_manager_->Commit(txn2);
  delete txn1;
  delete txn2;
}

// NOLINTNEXTLINE
TEST_F(TransactionTest, ABLED_MixTest) {
#define THREAD_BEGIN(num) \
  std::thread CONCAT(t,num)([&](){
#define THREAD_END \
  });
  using namespace std::chrono_literals;

  auto noop_writer = NoopWriter();
  EXECUTE_SQL("CREATE TABLE test (val int)", noop_writer);
  EXECUTE_SQL("INSERT INTO test VALUES (100),(200),(300)", noop_writer);

  auto *txn1 = bustub_->txn_manager_->Begin();
  auto *txn2 = bustub_->txn_manager_->Begin();
  auto *txn3 = bustub_->txn_manager_->Begin();

  MAKE_SS_WRITER(1);
  MAKE_SS_WRITER(2);
  MAKE_SS_WRITER(3);
  THREAD_BEGIN(1)
  EXECUTE_SQL_TXN("SELECT * FROM test", writer1, txn1);
  bustub_->txn_manager_->Commit(txn1);
  THREAD_END
  THREAD_BEGIN(2)
  std::this_thread::sleep_for(200ms);
  EXECUTE_SQL_TXN("DELETE FROM test WHERE val=100", writer2, txn2);
  EXECUTE_SQL_TXN("SELECT * FROM test", writer2, txn2);
  THREAD_END
  THREAD_BEGIN(3)
  std::this_thread::sleep_for(300ms);
  EXECUTE_SQL_TXN("SELECT * FROM test", writer3, txn3);
  THREAD_END

  t1.join();
  t2.join();
  t3.join();
  std::cerr << "\tbegin to clean up\n";
  bustub_->txn_manager_->Commit(txn2);
  bustub_->txn_manager_->Commit(txn3);

  delete txn1;
  delete txn2;
  delete txn3;
}

}  // namespace bustub
