//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_concurrent_test.cpp
//
// Identification: test/storage/b_plus_tree_concurrent_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <chrono>  // NOLINT
#include <cstdio>
#include <functional>
#include <random>
#include <thread>  // NOLINT

#include "buffer/buffer_pool_manager_instance.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {
// helper function to launch multiple threads
template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, Args &&...args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(args..., thread_itr));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

// helper function to insert
void InsertHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree->Insert(index_key, rid, transaction);
  }
  delete transaction;
}

// helper function to seperate insert
void InsertHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                       int total_threads, __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      tree->Insert(index_key, rid, transaction);
    }
  }
  delete transaction;
}

// helper function to delete
void DeleteHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &remove_keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree->Remove(index_key, transaction);
  }
  delete transaction;
}

// helper function to seperate delete
void DeleteHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree,
                       const std::vector<int64_t> &remove_keys, int total_threads,
                       __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      index_key.SetFromInteger(key);
      tree->Remove(index_key, transaction);
    }
  }
  delete transaction;
}

void BenchmarkHelper(const int64_t kTestTimes, const int64_t kMinKey, const int64_t kMaxKey, const int kNumThreads,
                     const int max_leaf_size, const int max_internal_size) {
  for (int i = 0; i < kTestTimes; ++i) {
    system("rm -f test.db test.log");
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    auto *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, max_internal_size,
                                                             max_leaf_size);
    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(&page_id);
    (void)header_page;
    // sequential insert
    std::vector<int64_t> keys;
    for (int64_t key = kMinKey; key <= kMaxKey; ++key) {
      keys.push_back(key);
    }
    LaunchParallelTest(kNumThreads, InsertHelperSplit, &tree, keys, kNumThreads);
    LaunchParallelTest(kNumThreads, DeleteHelperSplit, &tree, keys, kNumThreads);
    size_t size = 0;
    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
      ++size;
    }

    EXPECT_EQ(size, 0);
    bpm->UnpinPage(HEADER_PAGE_ID, true);
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void StrongDeleteTestHelper(const int64_t kTestTimes, const int64_t kMinKey, const int64_t kMaxKey,
                            const int kNumThreads, const int max_leaf_size, const int max_internal_size) {
  const int64_t kNumberDelete = kMaxKey;
  for (int i = 0; i < kTestTimes; ++i) {
    system("rm -f test.db test.log");
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    auto *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, max_internal_size,
                                                             max_leaf_size);
    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(&page_id);
    (void)header_page;
    // sequential insert
    std::vector<int64_t> keys;
    for (int64_t key = kMinKey; key <= kMaxKey; ++key) {
      keys.push_back(key);
    }
    InsertHelper(&tree, keys);

    // remove odd number keys.
    std::vector<int64_t> remove_keys;
    std::unordered_set<int64_t> removed_keys;

    for (int64_t count = 1; count <= kNumberDelete; ++count) {
      std::random_device rd;   // a seed source for the random number engine
      std::mt19937 gen(rd());  // mersenne_twister_engine seeded with rd()
      std::uniform_int_distribution<int64_t> distrib(kMinKey, kMaxKey);
      auto this_rand = count;
      if (removed_keys.count(this_rand) == 0) {
        remove_keys.push_back(this_rand);
        removed_keys.insert(this_rand);
      }
    }
    LaunchParallelTest(kNumThreads, DeleteHelperSplit, &tree, remove_keys, kNumThreads);

    size_t size = 0;
    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
      auto key = (*iterator).first;
      std::stringstream str;
      str << key;
      auto this_key = std::atol(str.str().c_str());
      if (removed_keys.count(this_key) != 0) {
        tree.Draw(bpm, "t");
        std::fprintf(stderr, "Error key : %ld\n", this_key);
        exit(1);
      }
      EXPECT_EQ(0, removed_keys.count(this_key));
      ++size;
    }

    EXPECT_EQ(size, keys.size() - remove_keys.size());
    bpm->UnpinPage(HEADER_PAGE_ID, true);
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

/**
 * ==============================================
 *                  Test cases.
 * ==============================================
 */

TEST(BPlusTreeConcurrentTest, DISABLED_InsertTest1) {
  system("rm -f test.db test.log");
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(2, InsertHelper, &tree, keys);

  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DISABLED_InsertTest2) {
  system("rm -f test.db test.log");
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 250, 250);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  const auto NUM_THREADS = 2;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(NUM_THREADS, InsertHelperSplit, &tree, keys, NUM_THREADS);
  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    EXPECT_EQ(true, tree.GetValue(index_key, &rids));
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);
  tree.Draw(bpm, "t");
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  tree.Draw(bpm, "t");
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DISABLED_DiagnoseInsert) {
  system("rm -f test.db test.log");
  auto NUM_THREADS{2};
  auto SCALE_FACTOR{1000};
  auto MAX_INTERNAL_SIZE{5};
  auto MAX_LEAF_SIZE{3};
  {  // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());
    auto *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, MAX_LEAF_SIZE,
                                                             MAX_INTERNAL_SIZE);
    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(&page_id);
    (void)header_page;
    // keys to Insert
    std::vector<int64_t> keys;
    for (int64_t key = 1; key < SCALE_FACTOR; key++) {
      keys.push_back(key);
    }
    LaunchParallelTest(NUM_THREADS, InsertHelperSplit, &tree, keys, NUM_THREADS);
    std::vector<RID> rids;
    GenericKey<8> index_key;
    for (auto key : keys) {
      rids.clear();
      index_key.SetFromInteger(key);
      EXPECT_EQ(true, tree.GetValue(index_key, &rids));
      EXPECT_EQ(rids.size(), 1);

      int64_t value = key & 0xFFFFFFFF;
      EXPECT_EQ(rids[0].GetSlotNum(), value);
    }

    int64_t start_key = 1;
    int64_t current_key = start_key;
    index_key.SetFromInteger(start_key);
    for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
      auto location = (*iterator).second;
      EXPECT_EQ(location.GetPageId(), 0);
      EXPECT_EQ(location.GetSlotNum(), current_key);
      current_key = current_key + 1;
    }

    EXPECT_EQ(current_key, keys.size() + 1);

    bpm->UnpinPage(HEADER_PAGE_ID, true);
    tree.Draw(bpm, "t");
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

TEST(BPlusTreeConcurrentTest, DISABLED_StrongInsertTest) {
  system("rm -f test.db test.log");
  std::vector<int> num_threads_options{1, 2, 4, 8};
  std::vector<int64_t> scale_factor_options{4, 8, 16, 32, 128, 512, 1024};
  std::vector<int> max_internal_size_options{3, 6, 10, 30, 100};
  std::vector<int> max_leaf_size_options{3, 6, 10, 30, 100};
  const auto num_loops_per_option = 3;
  const auto total_options = num_threads_options.size() * scale_factor_options.size() *
                             max_internal_size_options.size() * max_leaf_size_options.size() * num_loops_per_option;
  size_t cur_option = 0;
  for (auto NUM_THREADS : num_threads_options) {
    for (auto SCALE_FACTOR : scale_factor_options) {
      for (auto MAX_INTERNAL_SIZE : max_internal_size_options) {
        for (auto MAX_LEAF_SIZE : max_leaf_size_options) {
          for (int i = 0; i < num_loops_per_option; ++i) {
            ++cur_option;
            {  // create KeyComparator and index schema
              auto key_schema = ParseCreateStatement("a bigint");
              GenericComparator<8> comparator(key_schema.get());
              auto *disk_manager = new DiskManager("test.db");
              BufferPoolManager *bpm = new BufferPoolManagerInstance(100, disk_manager);
              // create b+ tree
              BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, MAX_LEAF_SIZE,
                                                                       MAX_INTERNAL_SIZE);
              // create and fetch header_page
              page_id_t page_id;
              auto header_page = bpm->NewPage(&page_id);
              (void)header_page;
              // keys to Insert
              std::vector<int64_t> keys;
              for (int64_t key = 1; key < SCALE_FACTOR; key++) {
                keys.push_back(key);
              }
              LaunchParallelTest(NUM_THREADS, InsertHelperSplit, &tree, keys, NUM_THREADS);
              std::vector<RID> rids;
              GenericKey<8> index_key;
              for (auto key : keys) {
                rids.clear();
                index_key.SetFromInteger(key);
                EXPECT_EQ(true, tree.GetValue(index_key, &rids));
                EXPECT_EQ(rids.size(), 1);

                int64_t value = key & 0xFFFFFFFF;
                EXPECT_EQ(rids[0].GetSlotNum(), value);
              }

              int64_t start_key = 1;
              int64_t current_key = start_key;
              index_key.SetFromInteger(start_key);
              for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
                auto location = (*iterator).second;
                EXPECT_EQ(location.GetPageId(), 0);
                EXPECT_EQ(location.GetSlotNum(), current_key);
                current_key = current_key + 1;
              }

              EXPECT_EQ(current_key, keys.size() + 1);

              bpm->UnpinPage(HEADER_PAGE_ID, true);
              tree.Draw(bpm, "t");
              delete disk_manager;
              delete bpm;
              remove("test.db");
              remove("test.log");
            }
            fprintf(stderr, "\r\b-> Testing: %.2f%%",
                    static_cast<double>(cur_option) / static_cast<double>(total_options) * 100);
          }
        }
      }
    }
  }
  fprintf(stderr, "\n");
}

TEST(BPlusTreeConcurrentTest, DISABLED_DeleteTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // sequential insert
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  LaunchParallelTest(2, DeleteHelper, &tree, remove_keys);

  int64_t start_key = 2;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DISABLED_DeleteTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // sequential insert
  std::vector<int64_t> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {1, 4, 3, 2, 5, 6};
  LaunchParallelTest(2, DeleteHelperSplit, &tree, remove_keys, 2);

  int64_t start_key = 7;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 4);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DISABLED_StrongDeleteTest) {
  const int num_loops_per_option = 3;
  std::vector<int64_t> max_key_options{512, 1024, 10000};
  std::vector<int> num_threads_options{2, 4};
  std::vector<int> max_internal_size_options{5, 250};
  std::vector<int> max_leaf_size_options{3, 250};
  const int64_t min_key = 1;
  const size_t total_options = max_key_options.size() * num_threads_options.size() * max_internal_size_options.size() *
                               max_leaf_size_options.size() * num_loops_per_option;
  size_t current_options = 0;
  for (auto max_key : max_key_options) {
    for (auto num_threads : num_threads_options) {
      for (auto max_internal : max_internal_size_options) {
        for (auto max_leaf : max_leaf_size_options) {
          for (int num_loops = 1; num_loops <= num_loops_per_option; ++num_loops) {
            fprintf(stderr,
                    "\rrunning(%.2lf%%): min_key=%-4ld max_key=%-4ld num_threads=%-4d max_leaf=%-4d max_internal=%-4d",
                    100 * static_cast<double>(current_options) / static_cast<double>(total_options), min_key, max_key,
                    num_threads, max_leaf, max_internal);
            StrongDeleteTestHelper(num_loops_per_option, min_key, max_key, num_threads, max_leaf, max_internal);
            fprintf(stderr,
                    "\rrunning(%.2lf%%): min_key=%-4ld max_key=%-4ld num_threads=%-4d max_leaf=%-4d max_internal=%-4d",
                    100 * static_cast<double>(++current_options) / static_cast<double>(total_options), min_key, max_key,
                    num_threads, max_leaf, max_internal);
          }
        }
      }
    }
  }
  fprintf(stderr, "\n");
}

TEST(BPlusTreeConcurrentTest, DISABLED_MixTest) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // first, populate index
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(&tree, keys);

  // concurrent insert
  keys.clear();
  for (int i = 6; i <= 10; i++) {
    keys.push_back(i);
  }
  LaunchParallelTest(1, InsertHelper, &tree, keys);
  // concurrent delete
  std::vector<int64_t> remove_keys = {1, 4, 3, 5, 6};
  LaunchParallelTest(1, DeleteHelper, &tree, remove_keys);

  int64_t start_key = 2;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    size = size + 1;
  }

  EXPECT_EQ(size, 5);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, BenchTest) {
  std::vector<std::chrono::milliseconds> times;
  int64_t total_time = 0;
  for (int i = 0; i < 5; ++i) {
    auto start_time = std::chrono::high_resolution_clock::now();
    BenchmarkHelper(1, 1, 123456, 4, 254, 254);
    auto end_time = std::chrono::high_resolution_clock::now();
    times.push_back(std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time));
  }
  // Output the duration in microseconds
  std::cerr << ">>> BenchMark Result: \n";
  std::cerr << "Each time(microseconds): ";
  for (auto used_time : times) {
    std::cerr << used_time.count() << " ";
    total_time += used_time.count();
  }
  std::cerr << std::endl;
  std::cerr << "Average time taken: " << total_time / times.size() << " microseconds" << std::endl;
  std::cerr << "<<< End\n";
}

}  // namespace bustub
