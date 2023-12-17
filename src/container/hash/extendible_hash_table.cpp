
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <bitset>
#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
#include <shared_mutex>
#include <utility>

#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : bucket_size_(bucket_size), global_depth_(0), num_buckets_(1) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size_));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  std::shared_lock lock(dir_rwlatch_);
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  std::shared_lock lock(dir_rwlatch_);
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  std::shared_lock lock(dir_rwlatch_);
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::shared_lock lock(dir_rwlatch_);
  auto bucket_index = IndexOf(key);
  auto &bucket = dir_[bucket_index];
  return bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::shared_lock lock(dir_rwlatch_);
  auto bucket_index = IndexOf(key);
  auto &bucket = dir_[bucket_index];
  return bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::unique_lock lock(dir_rwlatch_);
  for (;;) {
    auto bucket_index = IndexOf(key);
    auto bucket = (this->dir_)[bucket_index];
    auto bucket_old_depth = bucket->GetDepth();
    auto bucket_new_depth = bucket->GetDepth() + 1;
    if (bucket->Insert(key, value)) {
      return;
    }
    if (bucket_old_depth == global_depth_) {
      auto old_size = dir_.size();
      dir_.reserve(old_size * 2);
      for (size_t i = 0; i < old_size; ++i) {
        dir_.emplace_back(dir_[i]);
      }
      ++global_depth_;
    }
    auto bucket_valid_hash_val = (bucket_index & ((1 << bucket_old_depth) - 1));
    auto redistributed_kvpairs = std::move(bucket->GetItems());
    auto new_bucket = std::make_shared<Bucket>(bucket_size_, bucket_new_depth);
    auto num = 1 << (global_depth_ - bucket_new_depth);
    auto low_base_hash_val = bucket_valid_hash_val;
    auto high_base_hash_val = bucket_valid_hash_val | (1 << (bucket_new_depth - 1));
    bucket->IncrementDepth();
    for (auto i = 0; i < num; ++i) {
      auto low = ((i << bucket_new_depth) | low_base_hash_val);
      auto high = ((i << bucket_new_depth) | high_base_hash_val);
      dir_[low] = bucket;
      dir_[high] = new_bucket;
    }
    num_buckets_++;
    for (const auto &[k, v] : redistributed_kvpairs) {
      auto key_index = IndexOf(k);
      dir_[key_index]->Insert(k, v);
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {
  container_.reserve(array_size);
}

// My implementation
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  std::shared_lock lock(rwlatch_);
  if (container_.count(key) != 0) {
    value = container_[key];
    return true;
  }
  return false;
}

// My implementation
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  std::unique_lock lock(rwlatch_);
  if (container_.count(key) != 0) {
    container_.erase(key);
    return true;
  }
  return false;
}

// My implementation
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  std::unique_lock lock(rwlatch_);
  if (container_.size() == size_) {
    return false;
  }
  container_[key] = value;
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
