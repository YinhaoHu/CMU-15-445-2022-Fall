/**
 * lru_k_replacer_test.cpp
 */

#include "buffer/lru_k_replacer.h"

#include <algorithm>
#include <cstdio>
#include <memory>
#include <random>
#include <set>
#include <thread>  // NOLINT

#include <vector>

#include "common/logger.h"
#include "gtest/gtest.h"

namespace bustub {

TEST(LRUKReplacerTest, SampleTest) {
  LRUKReplacer lru_replacer(7, 2);

  // Scenario: add six elements to the replacer. We have [1,2,3,4,5]. Frame 6 is non-evictable.
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(5);
  lru_replacer.RecordAccess(6);
  lru_replacer.SetEvictable(1, true);
  lru_replacer.SetEvictable(2, true);
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);
  lru_replacer.SetEvictable(5, true);
  lru_replacer.SetEvictable(6, false);
  ASSERT_EQ(5, lru_replacer.Size());

  // Scenario: Insert access history for frame 1. Now frame 1 has two access histories.
  // All other frames have max backward k-dist. The order of eviction is [2,3,4,5,1].
  lru_replacer.RecordAccess(1);

  // Scenario: Evict three pages from the replacer. Elements with max k-distance should be popped
  // first based on LRU.
  int value;
  lru_replacer.Evict(&value);
  ASSERT_EQ(2, value);
  lru_replacer.Evict(&value);
  ASSERT_EQ(3, value);
  lru_replacer.Evict(&value);
  ASSERT_EQ(4, value);
  ASSERT_EQ(2, lru_replacer.Size());

  // Scenario: Now replacer has frames [5,1].
  // Insert new frames 3, 4, and update access history for 5. We should end with [3,1,5,4]
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(5);
  lru_replacer.RecordAccess(4);
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);
  ASSERT_EQ(4, lru_replacer.Size());

  // Scenario: continue looking for victims. We expect 3 to be evicted next.
  lru_replacer.Evict(&value);
  ASSERT_EQ(3, value);
  ASSERT_EQ(3, lru_replacer.Size());

  // Set 6 to be evictable. 6 Should be evicted next since it has max backward k-dist.
  lru_replacer.SetEvictable(6, true);
  ASSERT_EQ(4, lru_replacer.Size());
  lru_replacer.Evict(&value);
  ASSERT_EQ(6, value);
  ASSERT_EQ(3, lru_replacer.Size());

  // Now we have [1,5,4]. Continue looking for victims.
  lru_replacer.SetEvictable(1, false);
  ASSERT_EQ(2, lru_replacer.Size());
  ASSERT_EQ(true, lru_replacer.Evict(&value));
  ASSERT_EQ(5, value);
  ASSERT_EQ(1, lru_replacer.Size());

  // Update access history for 1. Now we have [4,1]. Next victim is 4.
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(1);
  lru_replacer.SetEvictable(1, true);
  ASSERT_EQ(2, lru_replacer.Size());
  ASSERT_EQ(true, lru_replacer.Evict(&value));
  ASSERT_EQ(value, 4);

  ASSERT_EQ(1, lru_replacer.Size());
  lru_replacer.Evict(&value);
  ASSERT_EQ(value, 1);
  ASSERT_EQ(0, lru_replacer.Size());

  // This operation should not modify size
  ASSERT_EQ(false, lru_replacer.Evict(&value));
  ASSERT_EQ(0, lru_replacer.Size());
}

TEST(LRUKReplacerTest, ConcurrenyTest) {
  // TODO: Why does not the lambda object need to capture these
  // constexpr variables.
  constexpr const size_t nframe{128}, k{8}, nthreads{8}, runs{300};

  for (size_t run = 0; run < runs; ++run) {
    std::vector<std::thread> threads;
    auto replacer{std::make_shared<LRUKReplacer>(nframe, k)};
    std::mutex first_write_mutex;
    auto first_write_frame{nframe};
    threads.reserve(nthreads);
    for (size_t tid = 0; tid < nthreads; ++tid) {
      threads.emplace_back([tid, &first_write_mutex, &first_write_frame, replacer]() {
        // Scenario: Concurrently record.
        // Test the memory safety and deadlock-free for record.
        auto record_k_times = [replacer](size_t frame) {
          for (auto n = k; n != 0; --n) {
            replacer->RecordAccess(frame);
          }
        };
        auto thread_nframe{nframe / nthreads}, thread_basis{thread_nframe * tid};
        for (size_t f = 0; f < thread_nframe; ++f) {
          if (f == 0) {
            std::scoped_lock lock(first_write_mutex);
            if (first_write_frame == nframe) {
              first_write_frame = f + thread_basis;
            }
            replacer->RecordAccess(f + thread_basis);
          } else {
            record_k_times(f + thread_basis);
          }
        }
        for (size_t f = 1; f < thread_nframe; ++f) {
          record_k_times(f);
        }
        return;
      });
    }
    for (auto &th : threads) {
      th.join();
    }
    threads.clear();

    // Scenario: Concurrently evict.
    // Test the memory safety and deadlock-free for evict.
    auto nevict_per_thread = (replacer->Size() - nthreads) / nthreads;
    for (size_t tid = 0; tid < nthreads; ++tid) {
      threads.emplace_back([nevict_per_thread, replacer]() {
        for (size_t i = 0; i < nevict_per_thread; ++i) {
          int local_evict;
          ASSERT_TRUE(replacer->Evict(&local_evict));
        }
      });
    }
    for (auto &thread : threads) {
      thread.join();
    }
  }
}

TEST(LRUKReplacerTest, SingleThreadEvictTest) {
  constexpr const size_t nframe = 1024;
  constexpr const size_t k = 2;

  auto replacer = std::make_unique<LRUKReplacer>(nframe, k);
  frame_id_t protected_frame_id = nframe / 2;

  for (size_t i = 0; i < nframe; ++i) {
    replacer->RecordAccess(i);
  }
  for (size_t i = nframe - 1;; --i) {
    replacer->RecordAccess(i);
    if (i == 0) {
      break;
    }
  }
  replacer->SetEvictable(protected_frame_id, false);

  for (frame_id_t expect_id = 0; expect_id < static_cast<frame_id_t>(nframe); ++expect_id) {
    frame_id_t val;
    if (expect_id == protected_frame_id) {
      continue;
    }
    EXPECT_TRUE(replacer->Evict(&val));
    EXPECT_EQ(expect_id, val);
  }
  EXPECT_EQ(0, replacer->Size());

  replacer->SetEvictable(protected_frame_id, true);
  EXPECT_EQ(1, replacer->Size());
  frame_id_t val;
  EXPECT_TRUE(replacer->Evict(&val));
  EXPECT_EQ(protected_frame_id, val);
}

TEST(LRUKReplacerTest, MultiThreadEvictTest) {
  constexpr frame_id_t total_frame = 128;  // Must be even number.
  constexpr size_t k = 2;
  constexpr size_t runs = 300;

  for (size_t run = 0; run < runs; ++run) {
    std::vector<std::thread> workers;
    auto replacer = std::make_shared<LRUKReplacer>(total_frame, k);

    auto record_task_1 = [replacer]() {
      for (frame_id_t i = 0; i < total_frame; ++i) {
        replacer->RecordAccess(i);
      }
    };
    auto record_task_2 = [replacer]() {
      for (frame_id_t i = total_frame - 1;; --i) {
        replacer->RecordAccess(i);
        replacer->RecordAccess(i);
        if (i == 0) {
          break;
        }
      }
    };
    auto evict_task = [replacer]() {
      frame_id_t total = total_frame / 2;
      for (frame_id_t count = 0; count < total; ++count) {
        frame_id_t id;
        ASSERT_TRUE(replacer->Evict(&id));
      }
    };

    // Multi-thread recording.
    {
      workers.emplace_back(std::thread(record_task_1));
      workers.emplace_back(std::thread(record_task_2));
      for (auto &worker : workers) {
        worker.join();
      }
      workers.clear();
      ASSERT_EQ(total_frame, replacer->Size());
    }

    // Multi-thread evicting
    {
      workers.emplace_back(std::thread(evict_task));
      workers.emplace_back(std::thread(evict_task));
      for (auto &worker : workers) {
        worker.join();
      }
      workers.clear();
      ASSERT_EQ(0, replacer->Size());
    }
  }
}

TEST(LRUKReplacerTest, BenchmarkTest) {
  constexpr frame_id_t nframe = 1 << 13;
  constexpr size_t k = 4;
  constexpr size_t runs = 3;
  auto replacer = std::make_unique<LRUKReplacer>(nframe, k);
  std::vector<int64_t> times;

  for (auto run = 0UL; run < runs; ++run) {
    auto start = std::chrono::high_resolution_clock::now();
    for (frame_id_t i = 0; i < nframe; ++i) {
      for (auto count = 0UL; count < k; ++count) {
        replacer->RecordAccess(i);
      }
      replacer->SetEvictable(i, true);
    }
    for (frame_id_t i = 0; i < nframe; ++i) {
      frame_id_t frame_id;
      replacer->Evict(&frame_id);
    }
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
    times.emplace_back(duration.count());
  }

  auto average_time = [&times]() {
    int64_t sum = 0;
    for (auto &t : times) {
      sum += t;
    }
    return sum / times.size();
  }();
  auto stddev = [average_time, &times]() {
    auto sum = 0.0;
    for (auto time : times) {
      sum += (time - average_time) * (time - average_time);
    }
    auto res = std::sqrt(sum / times.size());
    return res;
  }();
  std::printf("\n\tAverage time: %ld ms | Standard deviation: %.3lf\n\n", average_time, stddev);
  // ==============================================================================
  //  v1:  avg = 4752ms  stddev = 10.551
  //  v2:  avg = 2307ms  stddev = 53.160
  //  v3:  avg = 1769ms  stddev = 10.344
  //
  //  best:avg = 20  ms in 13, 2806 ms in 20
  //  set version:45 in 13,  5512 in 20
}

}  // namespace bustub
