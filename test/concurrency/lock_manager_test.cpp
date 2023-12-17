/**
 * lock_manager_test.cpp
 */

#include "concurrency/lock_manager.h"

#include <chrono>
#include <random>
#include <thread>  // NOLINT

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"

namespace bustub {

/*
 * This test is only a sanity check. Please do not rely on this test
 * to check the correctness.
 */

// --- Helper functions ---
void CheckGrowing(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::GROWING); }

void CheckShrinking(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::SHRINKING); }

void CheckAborted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::ABORTED); }

void CheckCommitted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::COMMITTED); }

void CheckTxnRowLockSize(Transaction *txn, table_oid_t oid, size_t shared_size, size_t exclusive_size) {
  EXPECT_EQ((*(txn->GetSharedRowLockSet()))[oid].size(), shared_size);
  EXPECT_EQ((*(txn->GetExclusiveRowLockSet()))[oid].size(), exclusive_size);
}

int GetTxnTableLockSize(Transaction *txn, LockManager::LockMode lock_mode) {
  switch (lock_mode) {
    case LockManager::LockMode::SHARED:
      return txn->GetSharedTableLockSet()->size();
    case LockManager::LockMode::EXCLUSIVE:
      return txn->GetExclusiveTableLockSet()->size();
    case LockManager::LockMode::INTENTION_SHARED:
      return txn->GetIntentionSharedTableLockSet()->size();
    case LockManager::LockMode::INTENTION_EXCLUSIVE:
      return txn->GetIntentionExclusiveTableLockSet()->size();
    case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
      return txn->GetSharedIntentionExclusiveTableLockSet()->size();
  }

  return -1;
}

void CheckTableLockSizes(Transaction *txn, size_t s_size, size_t x_size, size_t is_size, size_t ix_size,
                         size_t six_size) {
  EXPECT_EQ(s_size, txn->GetSharedTableLockSet()->size());
  EXPECT_EQ(x_size, txn->GetExclusiveTableLockSet()->size());
  EXPECT_EQ(is_size, txn->GetIntentionSharedTableLockSet()->size());
  EXPECT_EQ(ix_size, txn->GetIntentionExclusiveTableLockSet()->size());
  EXPECT_EQ(six_size, txn->GetSharedIntentionExclusiveTableLockSet()->size());
}

/**
 * ===================================================================================================
 *                                      Test Case Set for LockManager
 * ===================================================================================================
 *        Number            Test Case               Status          DATE                    Note
 *          1           TableLockTest1              DOING       9th,Dec -> ,Dec
 *          2           TableLockUpgradeTest1       DOING
 *          3           RowLockTest1                DOING
 *          4           TwoPLTest1                  DOING
 */

void TableLockTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  std::vector<table_oid_t> oids;
  std::vector<Transaction *> txns;

  /** 10 tables */
  int num_oids = 10;
  for (int i = 0; i < num_oids; i++) {
    table_oid_t oid{static_cast<uint32_t>(i)};
    oids.push_back(oid);
    txns.push_back(txn_mgr.Begin());
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }

  /** Each transaction takes an S lock on every table and then unlocks */
  auto task = [&](int txn_id) {
    bool res;
    for (const table_oid_t &oid : oids) {
      res = lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::EXCLUSIVE, oid);
      EXPECT_TRUE(res);
      CheckGrowing(txns[txn_id]);
    }
    for (const table_oid_t &oid : oids) {
      res = lock_mgr.UnlockTable(txns[txn_id], oid);
      EXPECT_TRUE(res);
      CheckShrinking(txns[txn_id]);
    }
    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);

    /** All locks should be dropped */
    CheckTableLockSizes(txns[txn_id], 0, 0, 0, 0, 0);
  };

  std::vector<std::thread> threads;
  threads.reserve(num_oids);

  for (int i = 0; i < num_oids; i++) {
    threads.emplace_back(std::thread{task, i});
  }

  for (int i = 0; i < num_oids; i++) {
    threads[i].join();
  }

  for (int i = 0; i < num_oids; i++) {
    delete txns[i];
  }
}
TEST(LockManagerTest, TableLockTest1) {
  try {
    TableLockTest1();
  } catch (std::exception &e) {
    std::cerr << "catch: " << e.what() << std::endl;
  }
}  // NOLINT

/** Upgrading single transaction from S -> X */
void TableLockUpgradeTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t oid = 0;
  auto txn1 = txn_mgr.Begin();

  /** Take S lock */
  EXPECT_EQ(true, lock_mgr.LockTable(txn1, LockManager::LockMode::SHARED, oid));
  CheckTableLockSizes(txn1, 1, 0, 0, 0, 0);

  /** Upgrade S to X */
  EXPECT_EQ(true, lock_mgr.LockTable(txn1, LockManager::LockMode::EXCLUSIVE, oid));
  CheckTableLockSizes(txn1, 0, 1, 0, 0, 0);

  /** Clean up */
  txn_mgr.Commit(txn1);
  CheckCommitted(txn1);
  CheckTableLockSizes(txn1, 0, 0, 0, 0, 0);

  delete txn1;
}
TEST(LockManagerTest, TableLockUpgradeTest1) { TableLockUpgradeTest1(); }  // NOLINT

TEST(LockManagerTest, DebugFullCompatiableTest) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t oid = 0;
  auto txn1 = txn_mgr.Begin();
  auto txn2 = txn_mgr.Begin();
  EXPECT_EQ(txn1->GetTransactionId(), 0);
  EXPECT_EQ(txn2->GetTransactionId(), 1);

  EXPECT_EQ(true, lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_SHARED, oid));
  std::thread block_thread([&lock_mgr, txn2, oid]() {
    try {
      EXPECT_EQ(true, lock_mgr.LockTable(txn2, LockManager::LockMode::EXCLUSIVE, oid));
    } catch (const std::exception &err) {
      std::cerr << err.what();
    }
  });

  using namespace std::chrono_literals;
  std::this_thread::sleep_for(200ms);
  try {
    EXPECT_EQ(0, GetTxnTableLockSize(txn2, LockManager::LockMode::EXCLUSIVE));

    txn_mgr.Commit(txn1);
    CheckCommitted(txn1);
    CheckTableLockSizes(txn1, 0, 0, 0, 0, 0);
    txn_mgr.Commit(txn2);
    CheckCommitted(txn2);
    CheckTableLockSizes(txn2, 0, 0, 0, 0, 0);
  } catch (std::exception &err) {
  }

  block_thread.join();
  delete txn1;
  delete txn2;
}

void RowLockTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t oid = 0;
  RID rid{0, 0};

  int num_txns = 3;
  std::vector<Transaction *> txns;
  for (int i = 0; i < num_txns; i++) {
    txns.push_back(txn_mgr.Begin());
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }

  /** Each transaction takes an S lock on the same table and row and then
   * unlocks */
  auto task = [&](int txn_id) {
    bool res;

    res = lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::SHARED, oid);
    EXPECT_TRUE(res);
    CheckGrowing(txns[txn_id]);

    res = lock_mgr.LockRow(txns[txn_id], LockManager::LockMode::SHARED, oid, rid);
    EXPECT_TRUE(res);
    CheckGrowing(txns[txn_id]);
    /** Lock set should be updated */
    ASSERT_EQ(true, txns[txn_id]->IsRowSharedLocked(oid, rid));

    res = lock_mgr.UnlockRow(txns[txn_id], oid, rid);
    EXPECT_TRUE(res);
    CheckShrinking(txns[txn_id]);
    /** Lock set should be updated */
    ASSERT_EQ(false, txns[txn_id]->IsRowSharedLocked(oid, rid));

    res = lock_mgr.UnlockTable(txns[txn_id], oid);
    EXPECT_TRUE(res);
    CheckShrinking(txns[txn_id]);

    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);
  };

  std::vector<std::thread> threads;
  threads.reserve(num_txns);

  for (int i = 0; i < num_txns; i++) {
    threads.emplace_back(std::thread{task, i});
  }

  for (int i = 0; i < num_txns; i++) {
    threads[i].join();
    delete txns[i];
  }
}
TEST(LockManagerTest, RowLockTest1) {
  try {
    RowLockTest1();
  } catch (TransactionAbortException &abort_exception) {
    std::cerr << abort_exception.GetInfo();
  }
}  // NOLINT

void TwoPLTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  table_oid_t oid = 0;

  RID rid0{0, 0};
  RID rid1{0, 1};

  auto *txn = txn_mgr.Begin();
  EXPECT_EQ(0, txn->GetTransactionId());

  bool res;
  res = lock_mgr.LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, oid);
  EXPECT_TRUE(res);

  res = lock_mgr.LockRow(txn, LockManager::LockMode::SHARED, oid, rid0);
  EXPECT_TRUE(res);

  CheckGrowing(txn);
  CheckTxnRowLockSize(txn, oid, 1, 0);

  res = lock_mgr.LockRow(txn, LockManager::LockMode::EXCLUSIVE, oid, rid1);
  EXPECT_TRUE(res);
  CheckGrowing(txn);
  CheckTxnRowLockSize(txn, oid, 1, 1);

  res = lock_mgr.UnlockRow(txn, oid, rid0);
  EXPECT_TRUE(res);
  CheckShrinking(txn);
  CheckTxnRowLockSize(txn, oid, 0, 1);

  try {
    lock_mgr.LockRow(txn, LockManager::LockMode::SHARED, oid, rid0);
  } catch (TransactionAbortException &e) {
    CheckAborted(txn);
    CheckTxnRowLockSize(txn, oid, 0, 1);
  }

  // Need to call txn_mgr's abort
  txn_mgr.Abort(txn);
  CheckAborted(txn);
  CheckTxnRowLockSize(txn, oid, 0, 0);
  CheckTableLockSizes(txn, 0, 0, 0, 0, 0);

  delete txn;
}

TEST(LockManagerTest, TwoPLTest1) {
  try {
    TwoPLTest1();
  } catch (TransactionAbortException &err) {
    std::cerr << err.GetInfo() << std::endl;
  }
}  // NOLINT

TEST(LockManagerTest, AbortTest) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  int n_threads = 3;
  table_oid_t oid = 0;
  std::vector<Transaction *> txns;

  txns.reserve(n_threads);
  for (int i = 0; i < n_threads; ++i) {
    txns.push_back(txn_mgr.Begin());
  }

  using namespace std::chrono_literals;

  auto task1 = [&](int txn_id) {
    lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::EXCLUSIVE, oid);
    std::this_thread::sleep_for(1000ms);
    lock_mgr.UnlockTable(txns[txn_id], oid);
  };

  auto task2 = [&](int txn_id) {
    std::this_thread::sleep_for(200ms);
    lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::EXCLUSIVE, oid);
    std::this_thread::sleep_for(200ms);
  };

  auto task3 = [&](int txn_id) {
    std::this_thread::sleep_for(500ms);
    txn_mgr.Abort(txns[1]);
    lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::EXCLUSIVE, oid);
    std::this_thread::sleep_for(800ms);
  };

  std::vector<std::thread> threads;
  threads.reserve(n_threads);

  threads.emplace_back(task1, 0);
  threads.emplace_back(task2, 1);
  threads.emplace_back(task3, 2);

  for (int txn_id = 0; txn_id < n_threads; txn_id++) {
    threads[txn_id].join();
    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);

    /** All locks should be dropped */
    CheckTableLockSizes(txns[txn_id], 0, 0, 0, 0, 0);
  }

  for (int i = 0; i < n_threads; i++) {
    delete txns[i];
  }
}

TEST(LockManagerTest, BlockTest) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  int n_txn = 3;
  std::vector<Transaction *> txns;

  txns.reserve(n_txn);
  for (int i = 0; i < n_txn; ++i) {
    txns.push_back(txn_mgr.Begin());
  }

  lock_mgr.LockTable(txns[0], LockManager::LockMode::EXCLUSIVE, 0);
  std::thread thread_1(
      [txns, &lock_mgr]() { EXPECT_TRUE(lock_mgr.LockTable(txns[1], LockManager::LockMode::INTENTION_EXCLUSIVE, 0)); });
  std::thread thread_2(
      [txns, &lock_mgr]() { EXPECT_TRUE(lock_mgr.LockTable(txns[2], LockManager::LockMode::INTENTION_SHARED, 0)); });
  using namespace std::chrono_literals;
  std::this_thread::sleep_for(300ms);
  lock_mgr.UnlockTable(txns[0], 0);

  std::this_thread::sleep_for(500ms);

  EXPECT_EQ(1, txns[1]->GetIntentionExclusiveTableLockSet()->size());
  EXPECT_EQ(1, txns[2]->GetIntentionSharedTableLockSet()->size());

  for (int txn_id = 0; txn_id < n_txn; txn_id++) {
    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);

    /** All locks should be dropped */
    CheckTableLockSizes(txns[txn_id], 0, 0, 0, 0, 0);
  }

  thread_1.join();
  thread_2.join();

  for (int i = 0; i < n_txn; i++) {
    delete txns[i];
  }
}

TEST(LockManagerTest, TableLockUpgradeTest) {
#define IGNORE_EXCEPTION(expr)     \
  try {                            \
    expr                           \
  } catch (std::exception & err) { \
  }
  using namespace std::chrono_literals;
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  int n_txn = 3;
  std::vector<Transaction *> txns;

  txns.reserve(n_txn);
  for (int i = 0; i < n_txn; ++i) {
    txns.push_back(txn_mgr.Begin(nullptr, IsolationLevel::READ_COMMITTED));
  }

  auto txn1_task = [&](int txn_id) {
    IGNORE_EXCEPTION(lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::SHARED, 0);)
    std::this_thread::sleep_for(450ms);
    IGNORE_EXCEPTION(lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::EXCLUSIVE, 0);)
    IGNORE_EXCEPTION(lock_mgr.UnlockTable(txns[txn_id], 0);)
    IGNORE_EXCEPTION(lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::INTENTION_SHARED, 0);)
    IGNORE_EXCEPTION(lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::EXCLUSIVE, 0);)
    IGNORE_EXCEPTION(lock_mgr.UnlockTable(txns[txn_id], 0);)
  };

  auto txn2_task = [&](int txn_id) {
    std::this_thread::sleep_for(150ms);
    IGNORE_EXCEPTION(lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::SHARED, 0);)
    std::this_thread::sleep_for(600ms);
    IGNORE_EXCEPTION(lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::EXCLUSIVE, 0);)
    IGNORE_EXCEPTION(lock_mgr.UnlockTable(txns[txn_id], 0);)
    std::this_thread::sleep_for(900ms);
    IGNORE_EXCEPTION(lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::INTENTION_SHARED, 0);)
    IGNORE_EXCEPTION(lock_mgr.UnlockTable(txns[txn_id], 0);)
  };

  auto txn3_task = [&](int txn_id) {
    std::this_thread::sleep_for(300ms);
    IGNORE_EXCEPTION(lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::SHARED, 0);)
    std::this_thread::sleep_for(750ms);
    IGNORE_EXCEPTION(lock_mgr.UnlockTable(txns[txn_id], 0);)
    IGNORE_EXCEPTION(lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::EXCLUSIVE, 0);)
    IGNORE_EXCEPTION(lock_mgr.UnlockTable(txns[txn_id], 0);)
  };

  std::vector<std::thread> threads;
  threads.reserve(n_txn);
  threads.emplace_back(std::move(txn1_task), 0);
  threads.emplace_back(std::move(txn2_task), 1);
  threads.emplace_back(std::move(txn3_task), 2);

  for (int txn_id = 0; txn_id < n_txn; txn_id++) {
    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);

    /** All locks should be dropped */
    CheckTableLockSizes(txns[txn_id], 0, 0, 0, 0, 0);
  }
  for (auto &thread : threads) {
    thread.join();
  }
  for (int i = 0; i < n_txn; i++) {
    delete txns[i];
  }
}

}  // namespace bustub
