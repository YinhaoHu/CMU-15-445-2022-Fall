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
TEST(LockManagerTest,ABLED_TableLockTest1) { TableLockTest1(); }  // NOLINT

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
TEST(LockManagerTest,ABLED_TableLockUpgradeTest1) { TableLockUpgradeTest1(); }  // NOLINT

TEST(LockManagerTest,ABLED_DebugFullCompatiableTest) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t oid = 0;
  auto txn1 = txn_mgr.Begin();
  auto txn2 = txn_mgr.Begin();
  EXPECT_EQ(txn1->GetTransactionId(), 0);
  EXPECT_EQ(txn2->GetTransactionId(), 1);

  EXPECT_EQ(true, lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_SHARED, oid));
  std::thread block_thread(
      [&lock_mgr, txn2, oid]() { EXPECT_EQ(true, lock_mgr.LockTable(txn2, LockManager::LockMode::EXCLUSIVE, oid)); });

  using namespace std::chrono_literals;
  std::this_thread::sleep_for(200ms);
  EXPECT_EQ(0, GetTxnTableLockSize(txn2, LockManager::LockMode::EXCLUSIVE));

  txn_mgr.Commit(txn1);
  CheckCommitted(txn1);
  CheckTableLockSizes(txn1, 0, 0, 0, 0, 0);
  txn_mgr.Commit(txn2);
  CheckCommitted(txn2);
  CheckTableLockSizes(txn2, 0, 0, 0, 0, 0);

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
TEST(LockManagerTest,ABLED_RowLockTest1) { RowLockTest1(); }  // NOLINT

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

  EXPECT_ANY_THROW(lock_mgr.LockRow(txn, LockManager::LockMode::SHARED, oid, rid0));

  // Need to call txn_mgr's abort
  txn_mgr.Abort(txn);
  CheckAborted(txn);
  CheckTxnRowLockSize(txn, oid, 0, 0);
  CheckTableLockSizes(txn, 0, 0, 0, 0, 0);

  delete txn;
}

TEST(LockManagerTest,ABLED_TwoPLTest1) { TwoPLTest1(); }  // NOLINT

TEST(LockManagerTest,ABLED_AbortTest) {
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

TEST(LockManagerTest,ABLED_BlockTest) {
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

TEST(LockmanagerTest,ABLED_RowAbortTest1) {
  using namespace std::chrono_literals;
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  auto txn0 = txn_mgr.Begin();
  auto txn1 = txn_mgr.Begin();
  table_oid_t oid = 0;
  RID rid1{0, 0};
  RID rid2{0, 1};

  std::thread t0([&]() {
    EXPECT_TRUE(lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_EXCLUSIVE, oid));
    EXPECT_TRUE(lock_mgr.LockRow(txn0, LockManager::LockMode::EXCLUSIVE, oid, rid2));
    std::this_thread::sleep_for(200ms);
    EXPECT_TRUE(lock_mgr.LockRow(txn0, LockManager::LockMode::EXCLUSIVE, oid, rid1));
    EXPECT_EQ(1, txn0->GetIntentionExclusiveTableLockSet()->count(oid));
    EXPECT_EQ(2, txn0->GetExclusiveRowLockSet()->at(oid).size());
    txn_mgr.Commit(txn0);
    CheckCommitted(txn0);
  });
  std::thread t1([&]() {
    EXPECT_TRUE(lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_EXCLUSIVE, oid));
    EXPECT_TRUE(lock_mgr.LockRow(txn1, LockManager::LockMode::EXCLUSIVE, oid, rid1));
    std::this_thread::sleep_for(200ms);
    EXPECT_FALSE(lock_mgr.LockRow(txn1, LockManager::LockMode::EXCLUSIVE, oid, rid2));
    CheckAborted(txn1);
    txn_mgr.Abort(txn1);
  });
  t1.join();
  t0.join();
  delete txn1;
  delete txn0;
}

void RowAbortTest() {
  using namespace std::chrono_literals;
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  std::vector<Transaction *> transactions;
  std::vector<std::thread> threads;
  table_oid_t oid = 0;
  RID rid{0, 0};
  size_t num_txn = 10;  // id: [0 , 9]
  for (size_t i = 0; i < num_txn; ++i) {
    transactions.emplace_back(txn_mgr.Begin());
    EXPECT_EQ(i, transactions.back()->GetTransactionId());
  }
  std::atomic_bool aborted{false};
  const auto task = [&](size_t txn_id) {
    auto txn = TransactionManager::GetTransaction(txn_id);
    lock_mgr.LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, oid);
    std::this_thread::sleep_for(txn_id * 60ms);
    lock_mgr.LockRow(txn, LockManager::LockMode::EXCLUSIVE, oid, rid);
    if (txn_id == 0) {
      for (; !aborted;) {
      }
      txn_mgr.Commit(txn);
    } else if (txn_id == 9) {
      EXPECT_EQ(1, transactions[txn_id]->GetExclusiveRowLockSet()->at(oid).count(rid));
      txn_mgr.Commit(txn);
    }
  };
  std::thread daemon([&]() {
    std::this_thread::sleep_for(500ms);
    for (size_t txn_id = 1; txn_id < 9; txn_id++) {
      txn_mgr.Abort(transactions[txn_id]);
    }
    aborted = true;
  });
  for (size_t i = 0; i < num_txn; ++i) {
    threads.emplace_back(task, i);
  }
  for (auto &thread : threads) {
    thread.join();
  }
  daemon.join();
  for (auto &txn : transactions) {
    delete txn;
  }
}

TEST(LockmanagerTest,ABLED_RowAbortTest2) {
  for (size_t i = 0; i < 2; ++i) {
    RowAbortTest();
  }
}

TEST(LockManagerTableTest,ABLED_ScaleBasicLockTest) {
  using namespace std::chrono_literals;
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  std::vector<Transaction *> transactions;
  std::vector<std::thread> threads;
  size_t n_txn = 3;
  table_oid_t oid = 0;
  size_t n_loops = 3000;
  const auto t0 = [&]() {
    if (lock_mgr.LockTable(transactions[0], LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE, oid)) {
      lock_mgr.UnlockTable(transactions[0], oid);
    }
  };
  const auto t1 = [&]() {
    if (lock_mgr.LockTable(transactions[1], LockManager::LockMode::EXCLUSIVE, oid)) {
      lock_mgr.UnlockTable(transactions[1], oid);
    }
  };
  const auto t2 = [&]() {
    if (lock_mgr.LockTable(transactions[2], LockManager::LockMode::SHARED, oid)) {
      lock_mgr.UnlockTable(transactions[2], oid);
    }
  };
  for (size_t i_loop = 0; i_loop < n_loops; ++i_loop) {
    for (size_t i = 0; i < n_txn; ++i) {
      transactions.emplace_back(txn_mgr.Begin());
    }
    threads.emplace_back(t0);
    threads.emplace_back(t1);
    threads.emplace_back(t2);
    for (size_t i = 0; i < n_txn; ++i) {
      threads[i].join();
    }
    threads.clear();
    for (size_t i = 0; i < n_txn; ++i) {
      txn_mgr.Commit(transactions[i]);
      delete transactions[i];
    }
    transactions.clear();
  }
}

TEST(LockManagerTableTest, DISABLED_ScaleUpgradeLockTest) {
  // NOTE: a buggy test.
  using namespace std::chrono_literals;
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  std::vector<Transaction *> transactions;
  std::vector<std::thread> threads;
  size_t n_txn = 3;
  table_oid_t oid = 0;
  size_t n_loops = 3000;
  const auto t0 = [&]() {
    if (lock_mgr.LockTable(transactions[0], LockManager::LockMode::INTENTION_EXCLUSIVE, oid)) {
      if (lock_mgr.LockTable(transactions[0], LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE, oid)) {
        lock_mgr.UnlockTable(transactions[0], oid);
        return;
      }
      lock_mgr.UnlockTable(transactions[0], oid);
    }
  };
  const auto t1 = [&]() {
    if (lock_mgr.LockTable(transactions[1], LockManager::LockMode::INTENTION_EXCLUSIVE, oid)) {
      if (lock_mgr.LockTable(transactions[1], LockManager::LockMode::EXCLUSIVE, oid)) {
        lock_mgr.UnlockTable(transactions[1], oid);
        return;
      }
      lock_mgr.UnlockTable(transactions[1], oid);
    }
  };
  const auto t2 = [&]() {
    if (lock_mgr.LockTable(transactions[2], LockManager::LockMode::SHARED, oid)) {
      if (lock_mgr.LockTable(transactions[2], LockManager::LockMode::EXCLUSIVE, oid)) {
        lock_mgr.UnlockTable(transactions[2], oid);
        return;
      }
      lock_mgr.UnlockTable(transactions[2], oid);
    }
  };
  for (size_t i_loop = 0; i_loop < n_loops; ++i_loop) {
    for (size_t i = 0; i < n_txn; ++i) {
      transactions.emplace_back(txn_mgr.Begin());
    }
    threads.emplace_back(t0);
    threads.emplace_back(t1);
    threads.emplace_back(t2);
    for (size_t i = 0; i < n_txn; ++i) {
      threads[i].join();
    }
    threads.clear();
    for (size_t i = 0; i < n_txn; ++i) {
      txn_mgr.Commit(transactions[i]);
      delete transactions[i];
    }
    transactions.clear();
  }
}

TEST(LockManagerTableTest, ABLED_ScaleUpgradeLockTest2) {
  using namespace std::chrono_literals;
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  std::vector<Transaction *> txn;
  std::vector<std::thread> threads;
  size_t n_txn = 3;
  table_oid_t oid = 0;
  size_t n_loops = 10;
  const auto t0 = [&]() {
    lock_mgr.LockTable(txn[0], LockManager::LockMode::SHARED, oid);
    lock_mgr.LockTable(txn[1], LockManager::LockMode::SHARED, oid);
    lock_mgr.LockTable(txn[2], LockManager::LockMode::SHARED, oid);
  };
  const auto t1 = [&]() { lock_mgr.LockTable(txn[0], LockManager::LockMode::EXCLUSIVE, oid); };
  for (size_t i_loop = 0; i_loop < n_loops; ++i_loop) {
    for (size_t i = 0; i < n_txn; ++i) {
      txn.emplace_back(txn_mgr.Begin());
    }
    t0();
    std::thread th1(t1);
    std::this_thread::sleep_for(50ms);
    lock_mgr.UnlockTable(txn[1], oid);
    lock_mgr.UnlockTable(txn[2], oid);
    th1.join();
    for (size_t i = 0; i < n_txn; ++i) {
      txn_mgr.Commit(txn[i]);
      delete txn[i];
    }
    txn.clear();
  }
}

}  // namespace bustub
