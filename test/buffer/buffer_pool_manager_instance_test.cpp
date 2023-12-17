//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance_test.cpp
//
// Identification: test/buffer/buffer_pool_manager_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include <array>
#include <cstdio>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"
#include "gtest/gtest.h"

[[maybe_unused]] static void STOP_TEST() {
  printf("=================STOP_TEST===============\n");
  exit(EXIT_SUCCESS);
}

namespace bustub {

// NOLINTNEXTLINE
// Check whether pages containing terminal characters can be recovered
TEST(BufferPoolManagerInstanceTest, BinaryDataTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  std::random_device r;
  std::default_random_engine rng(r());
  std::uniform_int_distribution<char> uniform_dist(0);

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManagerInstance(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  char random_binary_data[BUSTUB_PAGE_SIZE];
  // Generate random binary data
  for (char &i : random_binary_data) {
    i = uniform_dist(rng);
  }

  // Insert terminal characters both in the middle and at end
  random_binary_data[BUSTUB_PAGE_SIZE / 2] = '\0';
  random_binary_data[BUSTUB_PAGE_SIZE - 1] = '\0';

  // Scenario: Once we have a page, we should be able to read and write content.
  std::memcpy(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE);
  EXPECT_EQ(0, std::memcmp(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE));

  // Scenario: We should be able to create new pages until we fill up the buffer
  // pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }
  // Scenario: Once the buffer pool is full, we should not be able to create any
  // new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }
  // Scenario: After unpinning pages {0, 1, 2, 3, 4} we should be able to create
  // 5 new pages
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
    bpm->FlushPage(i);
  }
  for (int i = 0; i < 5; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
    bpm->UnpinPage(page_id_temp, false);
  }
  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, memcmp(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE));
  EXPECT_EQ(true, bpm->UnpinPage(0, true));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

// NOLINTNEXTLINE
TEST(BufferPoolManagerInstanceTest, SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManagerInstance(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  // Scenario: Once we have a page, we should be able to read and write content.
  snprintf(page0->GetData(), BUSTUB_PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: We should be able to create new pages until we fill up the buffer
  // pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any
  // new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new
  // pages, there would still be one buffer page left for reading page 0.
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
  }
  for (int i = 0; i < 4; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: If we unpin page 0 and then make a new page, all the buffer pages
  // should now be pinned. Fetching page 0 should fail.
  EXPECT_EQ(true, bpm->UnpinPage(0, true));
  EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  EXPECT_EQ(nullptr, bpm->FetchPage(0));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerInstanceTest, CompleteTest) {
  /**
   *  TEST INSTRUCTIONS
   *
   * 1. To make us compute the expected result easily, we
   * let the buffer pool be small.
   *
   * 2. We use [pid,fid](pin_count) to denote a pair which
   * is pinned for pin_count times.
   */
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 2;
  const size_t k = 2;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManagerInstance(buffer_pool_size, disk_manager, k);

  [[maybe_unused]] page_id_t first_page_id, second_page_id, temp_page_id;
  [[maybe_unused]] Page *first_page_ptr, *second_page_ptr;

  // Sequance 1: add pages.
  // Expect : [0,0](1), [1,1](1)
  EXPECT_NE(nullptr, bpm->NewPage(&first_page_id));
  EXPECT_NE(nullptr, bpm->NewPage(&second_page_id));

  EXPECT_EQ(nullptr, bpm->NewPage(&temp_page_id));

  // Sequence 2: test the basic functionality of delete page.
  // Expect : [*,*](0), [1,1](1)
  EXPECT_TRUE(bpm->DeletePage(3));
  LOG_DEBUG("ok ");
  EXPECT_FALSE(bpm->DeletePage(0));
  LOG_DEBUG("ok ");
  EXPECT_TRUE(bpm->UnpinPage(0, true));
  LOG_DEBUG("ok ");
  EXPECT_TRUE(bpm->DeletePage(0));
  LOG_DEBUG("ok ");
  // Sequence 3: test the basic functionality of fetch page.
  // EXPECT : [*,*](0), [1,1](2)
  EXPECT_NE(nullptr, bpm->FetchPage(0));
  EXPECT_NE(nullptr, bpm->FetchPage(1));
  EXPECT_TRUE(bpm->UnpinPage(1, true));
  EXPECT_NE(nullptr, bpm->FetchPage(1));
  EXPECT_TRUE(bpm->UnpinPage(0, true));
  EXPECT_TRUE(bpm->DeletePage(0));
  // Sequence 4: test the basic functionality of new page.
  // EXPECT: [2,0](1), [1,1](2)
  EXPECT_NE(nullptr, (first_page_ptr = bpm->NewPage(&first_page_id)));
  EXPECT_EQ(2, first_page_id);
  EXPECT_EQ(2, first_page_ptr->GetPageId());

  // Sequence 5: test unpin and delete.
  // EXPECT: [2,0](1), [4,1](1)
  EXPECT_FALSE(bpm->DeletePage(1));
  EXPECT_TRUE(bpm->UnpinPage(1, false));
  EXPECT_TRUE(bpm->UnpinPage(1, false));
  EXPECT_TRUE(bpm->FetchPage(4));

  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerInstanceTest, DiskTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 2;
  const size_t k = 2;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManagerInstance(buffer_pool_size, disk_manager, k);

  std::array<page_id_t, 2> page_ids;
  std::array<Page *, 2> pages;
  std::string page_content{"foo and bar."};
  page_content.resize(BUSTUB_PAGE_SIZE);

  EXPECT_TRUE(bpm->NewPage(&page_ids[0]));
  EXPECT_TRUE(bpm->NewPage(&page_ids[1]));
  EXPECT_NE(nullptr, (pages[0] = bpm->FetchPage(page_ids[0])));

  std::strcpy(pages[0]->GetData(), page_content.data());
  EXPECT_TRUE(bpm->UnpinPage(page_ids[0], true));
  EXPECT_TRUE(bpm->UnpinPage(page_ids[0], true));
  EXPECT_TRUE(bpm->FlushPage(page_ids[0]));
  EXPECT_TRUE(bpm->NewPage(&page_ids[1]));
  EXPECT_TRUE(bpm->UnpinPage(page_ids[1], false));
  EXPECT_NE(nullptr, pages[0] = bpm->FetchPage(page_ids[0]));
  EXPECT_EQ(0, std::strcmp(page_content.data(), pages[0]->GetData()));

  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerInstanceTest, DISABLED_BenchmarkNewPageTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 200;
  const size_t k = 4;
  constexpr size_t runs = 1000;

  for (size_t run = 0; run < runs; ++run) {
    auto *disk_manager = new DiskManager(db_name);
    auto *bpm = new BufferPoolManagerInstance(buffer_pool_size, disk_manager, k);

    auto work = [bpm](page_id_t begin_page_id, page_id_t end_page_id) {
      for (page_id_t page_id = begin_page_id; page_id != end_page_id; ++page_id) {
        page_id_t local_page_id;
        ASSERT_NE(nullptr, bpm->NewPage(&local_page_id));
      }
    };

    std::vector<std::thread> workers;

    for (page_id_t page_id = 0; static_cast<size_t>(page_id) < buffer_pool_size; page_id += 50) {
      workers.emplace_back([&work, page_id]() { work(page_id, page_id + 50); });
    }
    for (auto &worker : workers) {
      worker.join();
    }

    disk_manager->ShutDown();
    remove("test.db");

    delete bpm;
    delete disk_manager;
  }
}

TEST(BufferPoolManagerInstanceTest, BenchmarkTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 200;
  constexpr size_t nworkers = 8;
  const size_t k = 4;
  constexpr size_t runs = 1;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManagerInstance(buffer_pool_size, disk_manager, k);

  auto work = [bpm](page_id_t begin_page_id, page_id_t end_page_id) {
    for (size_t run = 0; run < runs; ++run) {
      for (page_id_t page_id = begin_page_id; page_id != end_page_id; ++page_id) {
        (bpm->FetchPage(page_id));
        (bpm->UnpinPage(page_id, true));
        (bpm->FetchPage(page_id));
        (bpm->UnpinPage(page_id, true));
        (bpm->FetchPage(page_id));
        (bpm->UnpinPage(page_id, true));
        (bpm->FetchPage(page_id));
        (bpm->UnpinPage(page_id, true));
      }
    }
  };

  std::vector<std::thread> workers;

  for (auto count = nworkers; count != 0; --count) {
    workers.emplace_back([&work]() { work(0, buffer_pool_size); });
  }
  for (auto &worker : workers) {
    worker.join();
  }

  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}
}  // namespace bustub
