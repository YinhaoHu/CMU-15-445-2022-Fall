/**
 * deadlock_detection_test.cpp
 */

#include <atomic>
#include <random>
#include <thread>  // NOLINT

#include "common/config.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"
#define TEST_TIMEOUT_BEGIN                           \
  std::promise<bool> promisedFinished;               \
  auto futureResult = promisedFinished.get_future(); \
                              std::thread([](std::promise<bool>& finished) {
#define TEST_TIMEOUT_FAIL_END(X)                                                                  \
  finished.set_value(true);                                                                       \
  }, std::ref(promisedFinished)).detach();                                                        \
  EXPECT_TRUE(futureResult.wait_for(std::chrono::milliseconds(X)) != std::future_status::timeout) \
      << "Test Failed Due to Time Out";

namespace bustub {

TEST(LockManagerDeadlockDetectionTest, DISABLED_BasicTest1) {
  // Test HasCycle.
  // WARNING: to test this, deadlock detection is not allowed to start!!!
  LockManager lock_manager;

  lock_manager.AddEdge(1, 2);
  lock_manager.AddEdge(2, 3);
  lock_manager.AddEdge(3, 1);

  txn_id_t cycle_maker = INVALID_TXN_ID;
  auto has_cycle = lock_manager.HasCycle(&cycle_maker);
  EXPECT_TRUE(has_cycle);
  EXPECT_EQ(1, cycle_maker);

  lock_manager.RemoveEdge(3, 1);
  has_cycle = lock_manager.HasCycle(&cycle_maker);
  EXPECT_FALSE(has_cycle);

  lock_manager.AddEdge(5, 4);
  lock_manager.AddEdge(4, 6);
  lock_manager.AddEdge(6, 5);
  has_cycle = lock_manager.HasCycle(&cycle_maker);
  EXPECT_TRUE(has_cycle);
  EXPECT_EQ(4, cycle_maker);
}

TEST(LockManagerDeadlockDetectionTest, DISABLED_EdgeTest) {
  LockManager lock_mgr{};

  const int num_nodes = 100;
  const int num_edges = num_nodes / 2;
  const int seed = 15445;
  std::srand(seed);

  // Create txn ids and shuffle
  std::vector<txn_id_t> txn_ids;
  txn_ids.reserve(num_nodes);
  for (int i = 0; i < num_nodes; i++) {
    txn_ids.push_back(i);
  }
  EXPECT_EQ(num_nodes, txn_ids.size());
  auto rng = std::default_random_engine{};
  std::shuffle(txn_ids.begin(), txn_ids.end(), rng);
  EXPECT_EQ(num_nodes, txn_ids.size());

  // Create edges by pairing adjacent txn_ids
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (int i = 0; i < num_nodes; i += 2) {
    EXPECT_EQ(i / 2, lock_mgr.GetEdgeList().size());
    auto t1 = txn_ids[i];
    auto t2 = txn_ids[i + 1];
    lock_mgr.AddEdge(t1, t2);
    edges.emplace_back(t1, t2);
    EXPECT_EQ((i / 2) + 1, lock_mgr.GetEdgeList().size());
  }

  auto lock_mgr_edges = lock_mgr.GetEdgeList();
  EXPECT_EQ(num_edges, lock_mgr_edges.size());
  EXPECT_EQ(num_edges, edges.size());

  std::sort(lock_mgr_edges.begin(), lock_mgr_edges.end());
  std::sort(edges.begin(), edges.end());

  for (int i = 0; i < num_edges; i++) {
    EXPECT_EQ(edges[i], lock_mgr_edges[i]);
  }
}

TEST(LockManagerDeadlockDetectionTest, DISABLED_BasicDeadlockDetectionTest) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t toid{0};
  RID rid0{0, 0};
  RID rid1{1, 1};
  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  EXPECT_EQ(0, txn0->GetTransactionId());
  EXPECT_EQ(1, txn1->GetTransactionId());

  std::thread t0([&] {
    // Lock and sleep
    bool res = lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(true, res);
    res = lock_mgr.LockRow(txn0, LockManager::LockMode::EXCLUSIVE, toid, rid0);
    EXPECT_EQ(true, res);
    EXPECT_EQ(TransactionState::GROWING, txn1->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // This will block
    res = lock_mgr.LockRow(txn0, LockManager::LockMode::EXCLUSIVE, toid, rid1);
    EXPECT_EQ(true, res);

    lock_mgr.UnlockRow(txn0, toid, rid1);
    lock_mgr.UnlockRow(txn0, toid, rid0);
    lock_mgr.UnlockTable(txn0, toid);

    txn_mgr.Commit(txn0);
    EXPECT_EQ(TransactionState::COMMITTED, txn0->GetState());
  });

  std::thread t1([&] {
    // Sleep so T0 can take necessary locks
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    bool res = lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(res, true);

    res = lock_mgr.LockRow(txn1, LockManager::LockMode::EXCLUSIVE, toid, rid1);
    EXPECT_EQ(TransactionState::GROWING, txn1->GetState());

    // This will block
    res = lock_mgr.LockRow(txn1, LockManager::LockMode::EXCLUSIVE, toid, rid0);
    EXPECT_EQ(res, false);

    EXPECT_EQ(TransactionState::ABORTED, txn1->GetState());
    txn_mgr.Abort(txn1);
  });

  // Sleep for enough time to break cycle
  std::this_thread::sleep_for(cycle_detection_interval * 2);

  t0.join();
  t1.join();

  delete txn0;
  delete txn1;
}

void CycleTest(){
  using namespace std::chrono_literals;
  LockManager lock_manager{};
  TransactionManager txn_manager{&lock_manager};
  constexpr size_t n_iter = 50;
  table_oid_t oid0 = 0;
  table_oid_t oid1 = 1;
  table_oid_t oid2 = 2;
  table_oid_t oid3 = 3;
  for (size_t iter = 0; iter < n_iter; ++iter) {
    auto txn0 = txn_manager.Begin();
    auto txn1 = txn_manager.Begin();
    auto txn2 = txn_manager.Begin();
    auto txn3 = txn_manager.Begin();
    std::thread t1([&] {
      lock_manager.LockTable(txn0, LockManager::LockMode::EXCLUSIVE, oid0);
      std::this_thread::sleep_for(50ms);
      lock_manager.LockTable(txn0, LockManager::LockMode::EXCLUSIVE, oid1);
      txn_manager.Commit(txn0);
    });
    std::thread t2([&]() {
      lock_manager.LockTable(txn1, LockManager::LockMode::EXCLUSIVE, oid1);
      std::this_thread::sleep_for(50ms);
      lock_manager.LockTable(txn1, LockManager::LockMode::EXCLUSIVE, oid0);
      txn_manager.Abort(txn1);
    });
    std::thread t3([&]() {
      lock_manager.LockTable(txn2, LockManager::LockMode::EXCLUSIVE, oid2);
      std::this_thread::sleep_for(50ms);
      lock_manager.LockTable(txn2, LockManager::LockMode::EXCLUSIVE, oid3);
      txn_manager.Commit(txn2);
    });
    std::thread t4([&]() {
      lock_manager.LockTable(txn3, LockManager::LockMode::EXCLUSIVE, oid3);
      std::this_thread::sleep_for(50ms);
      lock_manager.LockTable(txn3, LockManager::LockMode::EXCLUSIVE, oid2);
      txn_manager.Abort(txn3);
    });

    t1.join();
    t2.join();
    t3.join();
    t4.join();
    delete txn0;
    delete txn1;
    delete txn2;
    delete txn3;
  }
}

TEST(LockManagerDeadlockDetectionTest, CycleTest) {
  size_t n_iters = 5;
  for(size_t iter = 0; iter < n_iters; ++iter){
    CycleTest();
  }
}

}  // namespace bustub
