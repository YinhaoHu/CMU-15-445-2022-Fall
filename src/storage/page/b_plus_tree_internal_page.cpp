//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/logger.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set 1) page type, 2) set current size, 3) set page id, 4) set parent id and
 * 5) set max page size
 *
 * [IMPLEMENTATION] Status: DONE
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetMaxSize(max_size);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 *
 * [IMPLEMENTATION] Status: DONE
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 *
 * [IMPLEMENTATION] Status: DONE
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Adjacent(const ValueType &value) -> ValueType {
  const auto size = GetSize();
  // If the size is not greater than 2, how can there be an adjacent node?
  BUSTUB_ASSERT(size > 1, "B_PLUS_TREE_INTERNAL_PAGE_TYPE::Adjacent - Unexpected case.");
  ValueType res = INVALID_PAGE_ID;
  for (int i = 0; i < size; ++i) {
    if (value == array_[i].second) {
      res = (i == (size - 1)) ? array_[i - 1].second : array_[i + 1].second;
      break;
    }
  }
  BUSTUB_ASSERT(res != INVALID_PAGE_ID, "unexpected case");
  return std::move(res);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::IsPredecessor(const ValueType &v, const ValueType &v_other) -> bool {
  const auto size = GetSize();
  for (int i = 0; i < size; ++i) {
    if (array_[i].second == v) {
      BUSTUB_ASSERT(array_[i + 1].second == v_other, "unexpected case");
      return false;
    }
    if (array_[i].second == v_other) {
      BUSTUB_ASSERT(array_[i + 1].second == v, "unexpected case");
      return true;
    }
  }
  BUSTUB_ASSERT(0, "unexpected code is reached.");
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::BetweenKeyIndex(const ValueType &va, const ValueType &vb) const -> int {
  const auto size = GetSize();
  int i;
  for (i = 0; i < size; ++i) {
    if (array_[i].second == va || array_[i].second == vb) {
      break;
    }
  }
  BUSTUB_ASSERT(array_[i + 1].second == vb || array_[i + 1].second == va,
                "B_PLUS_TREE_INTERNAL_PAGE_TYPE::BetweenKeyIndex - Unexpected case.");
  return i + 1;
}

/**
 * Function family of Insertion
 */

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAfter(const ValueType &value, const KeyType &new_key,
                                                 const ValueType &new_value) {
  if (GetSize() == GetMaxSize()) {
    return;
  }
  auto index = 0;
  const auto size = this->GetSize();
  for (; index < size; ++index) {
    if (array_[index].second == value) {
      ++index;
      break;
    }
  }
  for (auto move_index = size; move_index > index; --move_index) {
    array_[move_index] = std::move(array_[move_index - 1]);
  }
  array_[index] = std::make_pair(new_key, new_value);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PushBack(const KeyType &key, const ValueType &value) {
  auto index = GetSize();
  array_[index].first = key;
  array_[index].second = value;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PushFront(const ValueType &value) {
  for (int index = GetSize(); index > 0; --index) {
    array_[index] = array_[index - 1];
  }
  array_[0].second = value;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::EmplaceBack(const std::vector<std::pair<KeyType, ValueType>> &pairs) {
  const auto pairs_size = static_cast<int>(pairs.size());
  const auto array_size = GetSize();
  for (int i = 0; i < pairs_size; ++i) {
    array_[i + array_size] = pairs[i];
  }
  IncreaseSize(pairs_size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Put(const ValueType &left, const KeyType &key, const ValueType &right) {
  array_[0].second = left;
  array_[1].first = key;
  array_[1].second = right;
  SetSize(2);
}

/**
 * Function family of Remove
 */

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) {
  const auto size = GetSize();
  auto i = 1;
  for (; i < size; ++i) {
    if (comparator(key, array_[i].first) == 0) {
      break;
    }
  }
  BUSTUB_ASSERT(i != size, "B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove - i should not be identical with size");
  for (; i < (size - 1); ++i) {
    array_[i] = array_[i + 1];
  }
  SetSize(size - 1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ExtractHalf() -> std::vector<std::pair<KeyType, ValueType>> {
  auto size = GetSize();
  auto res = std::vector<MappingType>{};
  res.reserve(size - GetMinSize());
  for (int i = GetMinSize(); i < size; ++i) {
    res.emplace_back(array_[i]);
  }
  SetSize(GetMinSize());
  return std::move(res);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ExtractAll() -> std::vector<std::pair<KeyType, ValueType>> {
  BUSTUB_ASSERT(GetSize() != 0, "B_PLUS_TREE_INTERNAL_PAGE_TYPE::ExtractAll() - Unexpected case");
  auto res = std::vector<MappingType>{};
  auto size = GetSize();
  for (int i = 0; i < size; ++i) {
    res.emplace_back(array_[i]);
  }
  SetSize(0);
  return std::move(res);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopBack() -> std::pair<KeyType, ValueType> {
  auto res = array_[GetSize() - 1];
  SetSize(GetSize() - 1);
  return std::move(res);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopFront() -> std::pair<KeyType, ValueType> {
  auto res = array_[0];
  const auto size = GetSize();
  for (int i = 1; i < size; ++i) {
    array_[i - 1] = array_[i];
  }
  SetSize(size - 1);
  return std::move(res);
}

/**
 * Helper function family
 */

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyToString(const KeyType &key) const -> std::string {
  std::stringstream buf;
  buf << key;
  return buf.str();
}

// value type for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
