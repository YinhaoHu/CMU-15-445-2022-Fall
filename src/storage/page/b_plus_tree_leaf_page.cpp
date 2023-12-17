//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including 1) set page type, 2) set current size to zero, 3) set page id/parent id,
 * 4) set next page id and 5) set max size
 *
 * [IMPEMENTATION] Status: DONE
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetNextPageId(INVALID_PAGE_ID);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetMaxSize(max_size);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetSize(0);
}

/**
 * Helper methods to set/get next page id
 *
 * [IMPEMENTATION] Status: DONE
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 *
 * [IMPLEMENTATION] Status: DONE
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  if (GetSize() == GetMaxSize()) {
    return;
  }
  auto index = 0;
  auto size = this->GetSize();
  for (; index < size; ++index) {
    const auto &k = array_[index].first;
    if (comparator(key, k) < 0) {
      break;
    }
  }
  for (auto move_index = size; move_index > index; --move_index) {
    array_[move_index] = array_[move_index - 1];
  }
  array_[index] = std::make_pair(key, value);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) {
  const auto size = GetSize();
  BUSTUB_ASSERT(size != 0, "unexpected size");
  auto i = 0;
  for (; i < size; ++i) {
    if (comparator(key, array_[i].first) == 0) {
      break;
    }
  }
  if (i == size) {
    return;
  }
  for (; i < (size - 1); ++i) {
    array_[i] = array_[i + 1];
  }
  SetSize(size - 1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::EmplaceBack(const std::vector<std::pair<KeyType, ValueType>> &paris) {
  const auto pairs_size = static_cast<int>(paris.size());
  const auto array_size = GetSize();
  for (int i = 0; i < pairs_size; ++i) {
    array_[i + array_size] = paris[i];
  }
  IncreaseSize(pairs_size);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ExtractAll() -> std::vector<std::pair<KeyType, ValueType>> {
  auto res = std::vector<MappingType>{};
  auto size = GetSize();
  for (int i = 0; i < size; ++i) {
    res.emplace_back(array_[i]);
  }
  SetSize(0);
  return std::move(res);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ExtractHalf() -> std::vector<MappingType> {
  auto size = GetSize();
  auto res = std::vector<MappingType>{};
  for (int i = GetMinSize(); i < size; ++i) {
    res.emplace_back(array_[i]);
  }
  SetSize(GetMinSize());
  return std::move(res);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Contain(const KeyType &key, const KeyComparator &comparator) -> bool {
  const auto size = GetSize();
  for (int i = 0; i < size; ++i) {
    if (comparator(array_[i].first, key) == 0) {
      return true;
    }
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::PopBack() -> std::pair<KeyType, ValueType> {
  auto res = array_[GetSize() - 1];
  SetSize(GetSize() - 1);
  return std::move(res);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::PopFront() -> std::pair<KeyType, ValueType> {
  auto res = array_[0];
  const auto size = GetSize();
  for (int i = 1; i < size; ++i) {
    array_[i - 1] = array_[i];
  }
  IncreaseSize(-1);
  return std::move(res);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
