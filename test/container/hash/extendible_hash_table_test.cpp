/**
 * extendible_hash_test.cpp
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <thread>         // NOLINT
#include <unordered_map>  // NOLINT

#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"

namespace bustub {

TEST(ExtendibleHashTableTest, SampleTest) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);

  table->Insert(1, "a");
  table->Insert(2, "b");
  table->Insert(3, "c");
  table->Insert(4, "d");
  table->Insert(5, "e");
  table->Insert(6, "f");
  table->Insert(7, "g");
  table->Insert(8, "h");
  table->Insert(9, "i");

  EXPECT_EQ(2, table->GetLocalDepth(0));
  EXPECT_EQ(3, table->GetLocalDepth(1));
  EXPECT_EQ(2, table->GetLocalDepth(2));
  EXPECT_EQ(2, table->GetLocalDepth(3));

  std::string result;
  table->Find(9, result);
  EXPECT_EQ("i", result);
  table->Find(8, result);
  EXPECT_EQ("h", result);
  table->Find(2, result);
  EXPECT_EQ("b", result);
  EXPECT_FALSE(table->Find(10, result));

  EXPECT_TRUE(table->Remove(8));
  EXPECT_TRUE(table->Remove(4));
  EXPECT_TRUE(table->Remove(1));
  EXPECT_FALSE(table->Remove(20));
}

TEST(ExtendibleHashTableTest, ConcurrentInsertTest) {
  const int num_runs = 50;
  const int num_threads = 3;

  // Run concurrent test multiple times to guarantee correctness.
  for (int run = 0; run < num_runs; run++) {
    auto table = std::make_unique<ExtendibleHashTable<int, int>>(2);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int tid = 0; tid < num_threads; tid++) {
      threads.emplace_back([tid, &table]() { table->Insert(tid, tid); });
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    EXPECT_EQ(table->GetGlobalDepth(), 1);
    for (int i = 0; i < num_threads; i++) {
      int val;
      EXPECT_TRUE(table->Find(i, val));
      EXPECT_EQ(i, val);
    }
  }
}

TEST(ExtendibleHashTableTest, StrongConcurrentInsertTest) {
  const int num_runs = 500;
  const int num_threads = 5;

  std::vector<std::vector<int>> threads_tasks{{28, 25}, {30, 11}, {23, 8}, {22, 27}, {7, 18}};
  std::vector<int> expected_depth{2, 2, 2, 3, 2, 2, 2, 3};
  int chosen_remove_num = threads_tasks.back().front();

  // Run concurrent test multiple times to guarantee correctness.
  for (int run = 0; run < num_runs; run++) {
    auto table = std::make_unique<ExtendibleHashTable<int, int>>(3);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int tid = 0; tid < num_threads; tid++) {
      threads.emplace_back([tid, &table, &threads_tasks]() {
        for (auto num : threads_tasks[tid]) {
          table->Insert(num, num);
        }
      });
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    table->Remove(chosen_remove_num);
    EXPECT_EQ(3, table->GetGlobalDepth());
    for (int i = 0; i < num_threads; i++) {
      int res;
      for (auto num : threads_tasks[i]) {
        if (num == chosen_remove_num) {
          EXPECT_FALSE(table->Find(num, res));
        } else {
          EXPECT_TRUE(table->Find(num, res));
          EXPECT_EQ(num, res);
        }
      }
    }
  }
}

TEST(ExtendibleHashTableTest, ConcurrentInsertFind) {
  auto run_one = []() {
    auto table = std::make_unique<ExtendibleHashTable<int, int>>(5);

    constexpr int nvals{100};

    std::vector<int> flags(nvals, 0);
    std::thread write_thread([&table]() {
      for (int i = 0; i < nvals; ++i) {
        table->Insert(i, i);
      }
    });

    std::thread read_thread([&flags, &table]() {
      int64_t count{0};

      while (count != nvals) {
        for (int i = 0; i < nvals; ++i) {
          int val;
          auto found = table->Find(i, val);
          if (found) {
            EXPECT_EQ(val, i);
            if (flags[i] == 0) {
              flags[i] = 1;
              ++count;
            }
          }
        }
      }
    });

    write_thread.join();
    read_thread.join();
  };

  constexpr size_t runs{100};

  for (size_t run = 0; run < runs; ++run) {
    run_one();
  }
}

TEST(ExtendibleHashTableTest, GetNumBucketsTest) {
  auto table = std::make_unique<ExtendibleHashTable<int, int>>(3);

  std::vector<int> items{28, 8, 25, 30, 22, 18, 11, 27, 23, 7};

  for (const auto &item : items) {
    table->Insert(item, item);
  }

  std::vector<int> local_depth_vec{2, 2, 2, 3, 2, 2, 2, 3};

  for (size_t i = 0; i < local_depth_vec.size(); ++i) {
    EXPECT_EQ(local_depth_vec[i], table->GetLocalDepth(i));
  }

  EXPECT_EQ(table->GetNumBuckets(), 5);
  EXPECT_EQ(table->GetGlobalDepth(), 3);
  EXPECT_TRUE(table->Remove(items.back()));

  int val;
  EXPECT_FALSE(table->Find(items.back(), val));
}

TEST(ExtendibleHashTableTest, StrongGetNumBucketsTest) {
  auto table = std::make_unique<ExtendibleHashTable<int, int>>(1);

  constexpr int nvals{1000};
  for (int i = 0; i < nvals; ++i) {
    table->Insert(i, i);
  }

  EXPECT_EQ(nvals, table->GetNumBuckets());
}

TEST(ExtendibleHashTableTest, SingleThreadBenchmarkTest) {
  auto table = std::make_unique<ExtendibleHashTable<int, int>>(100);
  int min_val = 0, max_val = (1 << 20);

  for (int val = min_val; val != max_val; ++val) {
    table->Insert(val, val);
  }
  for (int val = min_val; val != max_val; ++val) {
    int temp;
    table->Find(val, temp);
  }
  for (int val = min_val; val != max_val; ++val) {
    table->Remove(val);
  }
}

TEST(ExtendibleHashTableTest, DISABLED_MultiThreadBenchmarkTest) {
  auto table = std::make_shared<ExtendibleHashTable<int, int>>(2);
  constexpr int min_val = 0, max_val = (1 << 18);
  constexpr size_t nworkers = 8;
  constexpr size_t runs = 3;
  auto work1 = [table]() {
    for (int val = min_val; val != max_val; ++val) {
      table->Insert(val, val);
    }
    for (int val = min_val; val != max_val; ++val) {
      int temp;
      table->Find(val, temp);
    }
    for (int val = min_val; val != max_val; ++val) {
      table->Remove(val);
    }
  };
  auto work2 = [table]() {
    for (int val = max_val; val != min_val; --val) {
      int temp;
      table->Find(val, temp);
    }
    for (int val = max_val; val != min_val; --val) {
      table->Insert(val, val);
    }
    for (int val = max_val; val != min_val; --val) {
      table->Remove(val);
    }
  };

  std::vector<int64_t> times;
  for (size_t run = 0; run < runs; ++run) {
    std::vector<std::thread> workers;
    auto start = std::chrono::high_resolution_clock::now();
    for (size_t n = 0; n < nworkers; n += 2) {
      workers.emplace_back(std::thread(work1));
      workers.emplace_back(std::thread(work2));
    }
    for (auto &worker : workers) {
      worker.join();
    }
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
    times.emplace_back(duration.count());
  }
  std::printf("\n\tAverage time: %ld ms\n\n", [&times]() {
    int64_t sum = 0;
    for (auto &t : times) {
      sum += t;
    }
    return sum;
  }() / times.size());
}

}  // namespace bustub
