//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>
#include <vector>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 24
#define INTERNAL_PAGE_SIZE ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)))
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // must call initialize method after "create" a new node
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);

  auto KeyAt(int index) const -> KeyType;
  void SetKeyAt(int index, const KeyType &key);
  auto ValueAt(int index) const -> ValueType;
  /**
   * @brief Get the adjacent(the next by default) node of the value.
   *
   * @param value The in-parent-value of the node.
   *
   * @return If node is the last one, the front node of it
   * will be returned. Otherwise, the next one.
   */
  auto Adjacent(const ValueType &value) -> ValueType;

  /**
   * @brief Get the relationship between the v and v_other.
   *
   * @param v The value used to be compared.
   * @param v_other The other value
   * @return True if v_other is the predecessor of v. Otherwise, return false.
   */
  auto IsPredecessor(const ValueType &v, const ValueType &v_other) -> bool;

  /**
   * @brief Get the key index whose key is between va and vb.
   *
   * @param va First value
   * @param vb Second value
   * @return The key index.
   */
  auto BetweenKeyIndex(const ValueType &va, const ValueType &vb) const -> int;
  /**
   * @brief Insert the pair(new_key,new_value) after value.
   *
   * @param value
   * @param new_key
   * @param new_value
   */
  void InsertAfter(const ValueType &value, const KeyType &new_key, const ValueType &new_value);

  void PushBack(const KeyType &key, const ValueType &value);

  void PushFront(const ValueType &value);

  void Put(const ValueType &left, const KeyType &key, const ValueType &right);

  auto ExtractHalf() -> std::vector<MappingType>;

  /**
   * @brife Used for coalescing and let size be 0.
   *
   * @return All pairs.
   */
  auto ExtractAll() -> std::vector<MappingType>;

  void EmplaceBack(const std::vector<MappingType> &pairs);

  void Remove(const KeyType &key, const KeyComparator &comparator);
  /**
   * @brife Pop the last pair and decrease the size.
   *
   * @return The back pair.
   */
  auto PopBack() -> MappingType;

  auto PopFront() -> MappingType;

  inline auto Get() -> MappingType * { return array_; }

 private:
  auto KeyToString(const KeyType &key) const -> std::string;

  // Flexible array member for page data.
  MappingType array_[(INTERNAL_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / sizeof(MappingType)];
};
}  // namespace bustub
