/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/logger.h"
#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t begin_page_id, int begin_index, BufferPoolManager *buffer_pool_manager)
    : page_id_(begin_page_id), index_(begin_index), buffer_pool_manager_(buffer_pool_manager) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_id_ == INVALID_PAGE_ID && index_ == 0; }
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  // Fetch every time to keep the pointer dereference semantics.
  auto page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(page_id_));
  pair_.first = page->KeyAt(index_);
  pair_.second = page->ValueAt(index_);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return pair_;
}
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  auto page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(page_id_));
  if ((index_ + 1) < page->GetSize()) {
    ++index_;
  } else if (page->GetNextPageId() != INVALID_PAGE_ID) {
    page_id_ = page->GetNextPageId();
    index_ = 0;
  } else {
    page_id_ = INVALID_PAGE_ID;
    index_ = 0;
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return *this;
}
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator<KeyType, ValueType, KeyComparator> &itr) const -> bool {
  return (page_id_ != itr.page_id_) || (index_ != itr.index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator<KeyType, ValueType, KeyComparator> &itr) const -> bool {
  return (page_id_ == itr.page_id_) && (index_ == itr.index_);
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
