//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

#include <mutex>  // NOLINT

#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : size_{0}, replacer_size_{num_frames}, k_{k} {
  temp_pool_ = new TempPool;
  cache_pool_ = new CachePool;
  timer_ = new Timer;
}

LRUKReplacer::~LRUKReplacer() {
  delete temp_pool_;
  delete cache_pool_;
  delete timer_;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  auto result_id = -1;
  auto evicted = false;
  std::unique_lock lock(latch_);
  if (size_ == 0) {
    LOG_DEBUG("empty replacer, not evict-able.");
    return false;
  }
  if (!temp_pool_->IsEmpty()) {
    evicted = temp_pool_->EvictableFront(result_id);
  }
  if (!evicted && !cache_pool_->IsEmpty()) {
    evicted = cache_pool_->PopEvictedFront(result_id);
  }
  if (!evicted) {
    LOG_DEBUG("no proper pool, not evict-able.");
    return false;
  }
  size_--;
  *frame_id = result_id;
  return true;
}

auto LRUKReplacer::RecordAccess(frame_id_t frame_id) -> void {
  CheckAndHandleFrameID(frame_id);
  std::unique_lock lock(latch_);
  if (!temp_pool_->Contain(frame_id) && !cache_pool_->Contain(frame_id)) {
    temp_pool_->Insert(frame_id, std::make_shared<Entry>(k_, frame_id, timer_));
    size_++;
  }
  auto entry = temp_pool_->Contain(frame_id) ? temp_pool_->Get(frame_id) : cache_pool_->Get(frame_id);
  entry->RecordAccess();
  if (entry->GetSize() == k_) {
    if (temp_pool_->Contain(entry->GetID())) {
      auto new_entry = temp_pool_->Remove(frame_id);
      cache_pool_->Insert(frame_id, std::move(new_entry));
    } else {
      cache_pool_->Adjust(frame_id);
    }
  }
}

auto LRUKReplacer::Remove(frame_id_t frame_id) -> void {
  CheckAndHandleFrameID(frame_id);
  std::unique_lock lock(latch_);
  if (temp_pool_->Contain(frame_id)) {
    auto entry = temp_pool_->Get(frame_id);
    if (entry->IsEvictable()) {
      size_--;
    } else {
      throw std::runtime_error("try to remove an inevitable frame.");
    }
    temp_pool_->Remove(frame_id);
  } else if (cache_pool_->Contain(frame_id)) {
    auto entry = cache_pool_->Get(frame_id);
    if (entry->IsEvictable()) {
      size_--;
    } else {
      throw std::runtime_error("try to remove an inevitable frame.");
    }
    cache_pool_->Remove(frame_id);
  } else {
    return;
  }
}

auto LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) -> void {
  CheckAndHandleFrameID(frame_id);
  std::unique_lock lock(latch_);
  std::shared_ptr<Entry> entry;
  if (temp_pool_->Contain(frame_id)) {
    entry = temp_pool_->Get(frame_id);
  } else if (cache_pool_->Contain(frame_id)) {
    entry = cache_pool_->Get(frame_id);
  } else {
    return;
  }
  if (!entry->IsEvictable() && set_evictable) {
    ++size_;
  } else if (entry->IsEvictable() && !set_evictable) {
    --size_;
  }
  entry->SetEvictable(set_evictable);
}

auto LRUKReplacer::Size() const -> size_t {
  std::shared_lock lock(latch_);
  return size_;
}
}  // namespace bustub
