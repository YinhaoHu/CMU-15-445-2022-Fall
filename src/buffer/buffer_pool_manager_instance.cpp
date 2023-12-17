//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

// FIXME : Diskmanager log debug

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock lock(latch_);
  auto frame_id = static_cast<frame_id_t>(0);
  auto page = static_cast<Page *>(nullptr);
  auto next_page_id = static_cast<page_id_t>(0);
  if (free_list_.empty()) {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
  } else {
    frame_id = free_list_.front();
    free_list_.pop_front();
  }
  page = &pages_[frame_id];
  page_table_->Remove(page->page_id_);
  next_page_id = AllocatePage();
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  page_table_->Insert(next_page_id, frame_id);

  if (page->is_dirty_) {
    disk_manager_->WritePage(page->page_id_, page->data_);
  }
  page->ResetMemory();
  page->page_id_ = next_page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  *page_id = next_page_id;
  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock lock(latch_);
  auto found_frame = false;
  auto frame_id = static_cast<frame_id_t>(0);
  auto page = static_cast<Page *>(nullptr);

  found_frame = page_table_->Find(page_id, frame_id);
  if (!found_frame) {
    if (free_list_.empty()) {
      found_frame = false;
    } else {
      frame_id = free_list_.front();
      free_list_.pop_front();
      found_frame = true;
    }
  } else {
    page = &pages_[frame_id];
    page->pin_count_++;
    replacer_->SetEvictable(frame_id, false);
    replacer_->RecordAccess(frame_id);
    return page;
  }
  if (!found_frame) {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    page_table_->Remove(pages_[frame_id].page_id_);
    if (pages_[frame_id].is_dirty_) {
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
    }
  }
  page = &pages_[frame_id];
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  page_table_->Insert(page_id, frame_id);

  disk_manager_->ReadPage(page_id, page->data_);
  page->page_id_ = page_id;
  page->is_dirty_ = false;
  page->pin_count_ = 1;
  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock lock(latch_);
  auto page = static_cast<Page *>(nullptr);
  auto frame_id = static_cast<frame_id_t>(0);
  if (page_table_->Find(page_id, frame_id)) {
    page = &pages_[frame_id];
  } else {
    return false;
  }
  auto res = false;
  if (page->pin_count_ > 0) {
    page->pin_count_--;
    if (is_dirty) {
      page->is_dirty_ = true;
    }
    if (page->pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, true);
    }
    res = true;
  }
  return res;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock lock(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto frame_id = static_cast<frame_id_t>(0);
  auto found_page = false;
  auto page = static_cast<Page *>(nullptr);
  found_page = page_table_->Find(page_id, frame_id);
  if (!found_page) {
    return false;
  }
  page = &pages_[frame_id];
  disk_manager_->WritePage(page->page_id_, page->data_);
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock lock(latch_);
  for (frame_id_t frame_id = 0; static_cast<size_t>(frame_id) < pool_size_; ++frame_id) {
    auto page = &pages_[frame_id];
    disk_manager_->WritePage(page->page_id_, page->data_);
    page->is_dirty_ = false;
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock lock(latch_);
  auto frame_id = static_cast<frame_id_t>(0);
  auto found_page = false;
  auto page = static_cast<Page *>(nullptr);
  found_page = page_table_->Find(page_id, frame_id);
  if (!found_page) {
    return true;
  }
  page = &pages_[frame_id];
  if (page->pin_count_ > 0) {
    return false;
  }
  page_table_->Remove(page_id);
  DeallocatePage(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_front(frame_id);
  page->ResetMemory();
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
