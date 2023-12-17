
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <map>
#include <memory>
#include <mutex>  // NOLINT
#include <set>
#include <shared_mutex>  // NOLINT
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time
 * between current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward
 * k-distance, classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be
   * required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer();

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that
   * frame. Only frames that are marked as 'evictable' are candidates for
   * eviction.
   *
   * A frame with less than k historical references is given +inf as its
   * backward k-distance. If multiple frames have inf backward k-distance, then
   * evict the frame with the earliest timestamp overall.
   *
   * Successful eviction of a frame should decrement the size of replacer and
   * remove the frame's access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be
   * evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current
   * timestamp. Create a new entry for access history if frame id has not been
   * seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an
   * exception. You can also use BUSTUB_ASSERT to abort the process if frame id
   * is invalid.
   *
   * @param frame_id id of frame that received a new access.
   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function
   * also controls replacer's size. Note that size is equal to number of
   * evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then
   * size should decrement. If a frame was previously non-evictable and is to be
   * set to evictable, then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying
   * anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access
   * history. This function should also decrement replacer's size if removal is
   * successful.
   *
   * Note that this is different from evicting a frame, which always remove the
   * frame with largest backward k-distance. This function removes specified
   * frame id, no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort
   * the process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() const -> size_t;

 private:
  /**
   * Timer simulates a tiktok timer.
   */
  class Timer {
   public:
    /**
     * @brief A new timer.
     */
    Timer() = default;

    /**
     * @note Default destructor is enough.
     */
    ~Timer() = default;

    /**
     * @brief Get the simulated time.
     *
     * @return size_t
     */
    inline auto GetTime() -> size_t { return ++time_; }

   private:
    size_t time_{0UL};
  };

  /**
   * Entry helps the LRUKReplacer to manage the access history for every frame.
   */
  class Entry {
   public:
    /**
     * @brief A new LRU-K Replacer Entry. Call the Record() INSTANTLY when you
     * allocate a new entry.
     */
    explicit Entry(size_t k, frame_id_t frame_id, Timer *timer)
        : k_{k},
          history_front_idx_{0},
          history_back_idx_{0},
          history_size_{0},
          id_(frame_id),
          evictable_{true},
          timer_{timer} {
      history_ = new size_t[k_];
    }

    DISALLOW_COPY_AND_MOVE(Entry);

    /**
     * @brief Delete a entry.
     */
    ~Entry() { delete[] history_; }

    /**
     * @brief Check whether this entry can be evicted or not.
     *
     * @return True if evictable, false if not.
     */
    inline auto IsEvictable() const -> bool { return evictable_; }

    /**
     * @brief Get the earliest timestamp which you might want in the case where
     * there are multiple Inf+ K-Distance values.
     *
     * @return size_t
     */
    inline auto GetEarliestTimestamp() const -> size_t { return history_[history_front_idx_]; }

    /**
     * @brief Record the access according to the current timestamp.
     */
    inline auto RecordAccess() -> void {
      if (history_size_ == k_) {
        history_front_idx_ = (history_front_idx_ + 1) % k_;
      } else {
        history_size_++;
      }
      history_[history_back_idx_] = timer_->GetTime();
      history_back_idx_ = (history_back_idx_ + 1) % k_;
    }

    /**
     * @brief Get the size(#<=k) of the history.
     */
    inline auto GetSize() const -> size_t { return history_size_; }
    /**
     * @brief Get the id of the frame.
     */
    inline auto GetID() const -> frame_id_t { return id_; }

    /**
     * @brief Set the evictable status of this entry.
     *
     * @param set_evictable The evictable status.
     */
    inline auto SetEvictable(bool set_evictable) -> void { evictable_ = set_evictable; }

   private:
    size_t k_;
    size_t history_front_idx_;
    size_t history_back_idx_;
    size_t history_size_;
    size_t *history_;
    frame_id_t id_;
    bool evictable_;
    Timer *timer_;
  };

  class TempPool {
   public:
    TempPool() = default;

    ~TempPool() = default;

    DISALLOW_COPY_AND_MOVE(TempPool);

    inline auto IsEmpty() const -> bool { return pool_.empty(); }

    inline auto Get(frame_id_t frame_id) -> std::shared_ptr<Entry> { return *(map_[frame_id]); };

    inline auto Contain(frame_id_t frame_id) const -> bool { return map_.count(frame_id) != 0; }

    inline auto Remove(frame_id_t frame_id) -> std::shared_ptr<Entry> {
      if (map_.count(frame_id) != 0) {
        auto iter = map_[frame_id];
        auto res = std::move(*iter);
        pool_.erase(iter);
        map_.erase(frame_id);
        return res;
      }
      return {nullptr};
    }

    inline auto Insert(frame_id_t frame_id, std::shared_ptr<Entry> entry) -> void {
      pool_.emplace_back(std::move(entry));
      map_.emplace(frame_id, --pool_.end());
    }

    inline auto EvictableFront(frame_id_t &result) -> bool {
      for (auto &entry : pool_) {
        if (entry->IsEvictable()) {
          auto id = entry->GetID();
          auto &iter = map_[id];
          pool_.erase(iter);
          map_.erase(id);
          result = id;
          return true;
        }
      }
      return false;
    }

   private:
    std::list<std::shared_ptr<Entry>> pool_;
    std::unordered_map<frame_id_t, decltype(pool_)::iterator> map_;
  };

  class CachePool {
   public:
    CachePool() = default;

    ~CachePool() = default;

    DISALLOW_COPY_AND_MOVE(CachePool);

    inline auto Contain(frame_id_t frame_id) const -> bool { return map_.count(frame_id) != 0; }

    inline auto Get(frame_id_t frame_id) -> std::shared_ptr<Entry> { return *(map_[frame_id]); };

    inline void Adjust(frame_id_t frame_id) {
      auto old_iter = map_[frame_id];
      auto entry = *old_iter;
      pool_.erase(old_iter);
      auto [new_iter, _] = pool_.emplace(std::move(entry));
      map_[frame_id] = new_iter;
    }

    inline void Insert(frame_id_t frame_id, std::shared_ptr<Entry> entry) {
      auto [iter, _] = pool_.emplace(std::move(entry));
      map_.emplace(frame_id, iter);
    }

    inline auto PopEvictedFront(frame_id_t &result) -> bool {
      for (auto iter = pool_.begin(); iter != pool_.end(); ++iter) {
        auto &entry = *iter;
        if (entry->IsEvictable()) {
          auto id = entry->GetID();
          pool_.erase(iter);
          map_.erase(id);
          result = id;
          return true;
        }
      }
      return false;
    }

    inline auto Remove(frame_id_t frame_id) -> bool {
      if (map_.count(frame_id) == 0) {
        return false;
      }
      auto iter = map_[frame_id];
      pool_.erase(iter);
      map_.erase(frame_id);
      return true;
    }

    inline auto IsEmpty() const -> bool { return pool_.empty(); }

   private:
    struct Compare {
      inline auto operator()(const std::shared_ptr<Entry> &x, const std::shared_ptr<Entry> &y) const -> bool {
        return x->GetEarliestTimestamp() < y->GetEarliestTimestamp();
      }
    };
    std::set<std::shared_ptr<Entry>, Compare> pool_;
    std::unordered_map<frame_id_t, decltype(pool_)::iterator> map_;
  };

  /**
   * @brief IsValidFrameID checks whether the frame_id is in the range [0,replacer_size_-1]
   */
  inline auto CheckAndHandleFrameID(frame_id_t frame_id) -> void {
    if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
      throw std::out_of_range("Frame id is out of range");
    }
  }

  size_t size_;
  size_t replacer_size_;
  size_t k_;
  TempPool *temp_pool_;
  CachePool *cache_pool_;

  Timer *timer_;
  mutable std::shared_mutex latch_;
};
}  // namespace bustub
