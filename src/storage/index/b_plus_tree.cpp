
#include <algorithm>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

// Uncomment the following define statement will show the log, otherwise, not.
// #define HOO_ALLOW_DEBUG_LOG

#ifdef HOO_ALLOW_DEBUG_LOG
static std::mutex debug_log_mutex;
#define DEBUG_THREAD_ID (pthread_self() % 1000)
#define THREAD_DEBUG_LOG(...) \
  debug_log_mutex.lock();     \
  LOG_DEBUG(__VA_ARGS__);     \
  debug_log_mutex.unlock();
#else
template <typename... Args>
inline void foo(Args... args) {}
#define DEBUG_THREAD_ID (1L)
#define THREAD_DEBUG_LOG(...) foo(__VA_ARGS__);
#endif

namespace bustub {
/*Use to see the benchmark statics*/

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      root_page_id_page_(new Page) {
  THREAD_DEBUG_LOG("Tree scale: leaf_max_size=%d,internal_max_size=%d", leaf_max_size, internal_max_size);
}

/*
 * Helper function to decide whether current b+tree is empty
 *
 * [IMPLEMENTATION] Status: DONE
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 *
 * [IMPLEMENTATION] Status: DONE | Note: transaction is ignored in checkpoint 1.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  THREAD_DEBUG_LOG("Enter | Parameter: key=%s", KeyToString(key).c_str());
  bool optimistic_success;
  const auto leaf = OptimisticSearch(key, SearchMode::Find, transaction, optimistic_success);
  if (leaf == nullptr) {
    return false;
  }
  const auto size = leaf->GetSize();
  auto i = 0;
  for (i = 0; i < size; ++i) {
    const auto value = leaf->KeyAt(i);
    if (comparator_(value, key) == 0) {
      result->emplace_back(leaf->ValueAt(i));
      break;
    }
  }
  DisusePage(ToRawPage(leaf), UseMode::Read);
  return (i != size);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  THREAD_DEBUG_LOG("(thread %ld) Enter | Parameters: key=%s,value=%s", DEBUG_THREAD_ID, KeyToString(key).c_str(),
                   ValueToString(value).c_str());
  auto latched_pages = std::unique_ptr<LatchedPageContainer, std::function<void(LatchedPageContainer *)>>(
      new LatchedPageContainer, [this](LatchedPageContainer *object) {
        for (auto &page : *object) {
          DisusePage(page, UseMode::Write);
        }
        delete object;
      });
  bool optimistic_success;
  LeafPage *leaf = OptimisticSearch(key, SearchMode::Insert, transaction, optimistic_success);
  if (!optimistic_success) {
    if (leaf != nullptr) {
      ToRawPage(leaf)->WUnlatch();
      buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    }
    leaf = PessimisticSearch(key, SearchMode::Insert, transaction, latched_pages.get());
  } else if (leaf != nullptr) {
    latched_pages->push_back(ToRawPage(leaf));
    if (leaf->Contain(key, comparator_)) {
      return false;
    }
    leaf->Insert(key, value, comparator_);
    return true;
  }
  if (leaf == nullptr) {
    page_id_t new_page_id;
    auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
    leaf = ToLeaf(ToTreePage(new_page));
    leaf->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
    leaf->SetPageType(IndexPageType::LEAF_PAGE);
    leaf->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    root_page_id_ = new_page_id;
    UpdateRootPageId(new_page_id);
    return true;
  }
  if (leaf->Contain(key, comparator_)) {
    return false;
  }
  if ((leaf->GetSize() + 1) == leaf->GetMaxSize()) {
    BUSTUB_ASSERT(optimistic_success == false, "unreasonable case");
    THREAD_DEBUG_LOG("(thread %ld) Enter case 3", DEBUG_THREAD_ID);
    leaf->Insert(key, value, comparator_);
    auto last_half = leaf->ExtractHalf();
    page_id_t new_page_id;
    auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
    auto new_leaf = reinterpret_cast<LeafPage *>(new_page);
    new_leaf->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
    new_leaf->SetPageType(IndexPageType::LEAF_PAGE);
    new_leaf->SetNextPageId(leaf->GetNextPageId());
    new_leaf->EmplaceBack(last_half);
    leaf->SetNextPageId(new_leaf->GetPageId());
    InsertInParent(leaf, last_half.front().first, new_leaf, latched_pages.get(), transaction);
  } else {
    leaf->Insert(key, value, comparator_);
  }
  THREAD_DEBUG_LOG("(thread %ld) Return case 3", DEBUG_THREAD_ID);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(LeafPage *node, const KeyType &key, BPlusTreePage *other_node,
                                    LatchedPageContainer *latched_pages, Transaction *transaction) {
  auto value = other_node->GetPageId();
  if (node->GetPageId() == GetRootPageId()) {
    page_id_t new_root_id;
    auto new_root = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&new_root_id));
    new_root->Init(new_root_id, INVALID_PAGE_ID, internal_max_size_);
    new_root->Put(node->GetPageId(), key, value);
    new_root->SetPageType(IndexPageType::INTERNAL_PAGE);
    node->SetParentPageId(new_root_id);
    other_node->SetParentPageId(new_root_id);
    root_page_id_ = new_root_id;
    UpdateRootPageId(new_root_id);
    buffer_pool_manager_->UnpinPage(new_root_id, true);
    buffer_pool_manager_->UnpinPage(value, true);
    return;
  }
  auto parent = ToInternal(ToTreePage(*std::find_if(latched_pages->begin(), latched_pages->end(), [node](Page *page) {
    return page->GetPageId() == node->GetParentPageId();
  })));
  if (parent->GetSize() == parent->GetMaxSize()) {
    page_id_t new_internal_id;
    auto new_internal = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&new_internal_id));
    auto pairs = parent->ExtractAll();
    pairs.insert(std::find_if(pairs.cbegin(), pairs.cend(),
                              [val = node->GetPageId()](const auto &pair) { return pair.second == val; }) +
                     1,
                 std::make_pair(key, value));
    auto right_first_index = parent->GetMinSize();
    auto right_first_key = pairs[right_first_index].first;
    std::vector<std::pair<KeyType, page_id_t>> left_pairs{pairs.cbegin(), pairs.cbegin() + right_first_index};
    std::vector<std::pair<KeyType, page_id_t>> right_pairs{pairs.cbegin() + right_first_index, pairs.cend()};
    parent->EmplaceBack(left_pairs);
    new_internal->Init(new_internal_id, INVALID_PAGE_ID, internal_max_size_);
    new_internal->SetPageType(IndexPageType::INTERNAL_PAGE);
    new_internal->EmplaceBack(right_pairs);
    if (comparator_(key, right_first_key) < 0) {
      NodeChangeParent(value, parent->GetPageId(), latched_pages);
    }
    for (const auto &[_, page_id] : right_pairs) {
      NodeChangeParent(page_id, new_internal->GetPageId(), latched_pages);
    }
    buffer_pool_manager_->UnpinPage(value, true);
    InsertInParent(reinterpret_cast<LeafPage *>(parent), right_first_key, new_internal, latched_pages, transaction);
  } else {
    other_node->SetParentPageId(parent->GetPageId());
    parent->InsertAfter(node->GetPageId(), key, value);
    buffer_pool_manager_->UnpinPage(value, true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  THREAD_DEBUG_LOG("(thread %ld) Enter | Parameters: key=%s", DEBUG_THREAD_ID, KeyToString(key).c_str());
  auto latched_pages = std::unique_ptr<LatchedPageContainer, std::function<void(LatchedPageContainer *)>>(
      new LatchedPageContainer, [this](LatchedPageContainer *object) {
        for (auto &page : *object) {
          DisusePage(page, UseMode::Write);
        }
        delete object;
      });
  bool optimistic_success;
  LeafPage *leaf = OptimisticSearch(key, SearchMode::Delete, transaction, optimistic_success);
  if (!optimistic_success) {
    if (leaf != nullptr) {
      ToRawPage(leaf)->WUnlatch();
      buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    }
    leaf = PessimisticSearch(key, SearchMode::Delete, transaction, latched_pages.get());
  } else if (leaf != nullptr) {
    latched_pages->push_back(ToRawPage(leaf));
    leaf->Remove(key, comparator_);
    return;
  }
  if (leaf == nullptr) {
    return;
  }
  RemoveEntry(leaf, key, latched_pages.get(), transaction);
  THREAD_DEBUG_LOG("(thread %ld) Return", DEBUG_THREAD_ID);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveEntry(BPlusTreePage *node, const KeyType &key, LatchedPageContainer *latched_pages,
                                 Transaction *transaction) {
  THREAD_DEBUG_LOG("(thread %ld) Enter| Parameters: node_page_id=%d, key=%s", DEBUG_THREAD_ID, node->GetPageId(),
                   KeyToString(key).c_str());
  node->IsLeafPage() ? ToLeaf(node)->Remove(key, comparator_) : ToInternal(node)->Remove(key, comparator_);
  // N is the root and N has only one remaining child
  if (node->IsRootPage()) {
    if (node->GetSize() == 1 && !node->IsLeafPage()) {
      THREAD_DEBUG_LOG("(thread %ld) execution branch 1", DEBUG_THREAD_ID);
      BPlusTreePage *child_node;
      auto found_iter = std::find_if(latched_pages->begin(), latched_pages->end(),
                                     [want_page_id = ToInternal(node)->ValueAt(0)](Page *page) {
                                       THREAD_DEBUG_LOG("(thread %ld) want page id %d", DEBUG_THREAD_ID, want_page_id);
                                       return page->GetPageId() == want_page_id;
                                     });
      if (found_iter == latched_pages->end()) {
        child_node = ToTreePage(UsePage(ToInternal(node)->ValueAt(0), UseMode::Write, transaction));
        latched_pages->push_back(ToRawPage(child_node));
      } else {
        child_node = ToTreePage(*found_iter);
      }
      child_node->SetParentPageId(INVALID_PAGE_ID);
      root_page_id_ = child_node->GetPageId();
      UpdateRootPageId(child_node->GetPageId());
      BUSTUB_ASSERT(std::find_if(latched_pages->begin(), latched_pages->end(),
                                 [page_id = node->GetPageId()](Page *page) { return page_id == page->GetPageId(); }) !=
                        latched_pages->end(),
                    "unexpected case");
      latched_pages->erase(
          std::remove_if(latched_pages->begin(), latched_pages->end(),
                         [page_id = node->GetPageId()](Page *page) { return page_id == page->GetPageId(); }));
      DeletePage(ToRawPage(node), UseMode::Write, transaction);
    } else if (node->GetSize() == 0) {
      latched_pages->erase(
          std::remove_if(latched_pages->begin(), latched_pages->end(),
                         [page_id = node->GetPageId()](Page *page) { return page_id == page->GetPageId(); }));
      DeletePage(ToRawPage(node), UseMode::Write, transaction);
      root_page_id_ = INVALID_PAGE_ID;
    }
  } else if (node->GetSize() < node->GetMinSize()) {
    THREAD_DEBUG_LOG("(thread %ld) execution branch 2", DEBUG_THREAD_ID);
    auto parent_iter = std::find_if(
        latched_pages->begin(), latched_pages->end(),
        [want_page_id = node->GetParentPageId()](Page *page) { return page->GetPageId() == want_page_id; });
    BUSTUB_ASSERT(parent_iter != latched_pages->end(), "unexpected case");
    auto parent_node = ToInternal(ToTreePage(*(parent_iter)));
    // same level, should not be in the latch_pages.
    auto adjacent_node = ToTreePage(UsePage(parent_node->Adjacent(node->GetPageId()), UseMode::Write, transaction));
    latched_pages->insert(
        std::find_if(latched_pages->begin(), latched_pages->end(),
                     [page_id = node->GetPageId()](Page *page) { return page->GetPageId() == page_id; }),
        ToRawPage(adjacent_node));
    auto between_key_index = parent_node->BetweenKeyIndex(node->GetPageId(), adjacent_node->GetPageId());
    auto between_key = parent_node->KeyAt(between_key_index);
    auto adjacent_is_predecessor = parent_node->IsPredecessor(node->GetPageId(), adjacent_node->GetPageId());
    THREAD_DEBUG_LOG(
        "(thread %ld) adjacent(%s,%d) %s node(%s,%d)", DEBUG_THREAD_ID,
        KeyToString(
            (ToInternal(parent_node)->Get())[adjacent_is_predecessor ? between_key_index - 1 : between_key_index].first)
            .c_str(),
        adjacent_node->GetPageId(), adjacent_is_predecessor ? "->" : "<-",
        KeyToString(
            (ToInternal(parent_node)->Get())[adjacent_is_predecessor ? between_key_index : between_key_index - 1].first)
            .c_str(),
        node->GetPageId());

    // Coalesce: entries in N and N′ can fit in a single node
    auto single_node_max = node->IsLeafPage() ? node->GetMaxSize() - 1 : node->GetMaxSize();
    if ((adjacent_node->GetSize() + node->GetSize()) <= single_node_max) {
      if (adjacent_is_predecessor) {
        Coalesce(adjacent_node, node, between_key, latched_pages, transaction);
      } else {
        Coalesce(node, adjacent_node, between_key, latched_pages, transaction);
      }
      RemoveEntry(parent_node, between_key, latched_pages, transaction);
    } else /* Redistribution: borrow an entry from N′ */ {
      if (adjacent_is_predecessor) {
        THREAD_DEBUG_LOG("(thread %ld) redistribute - page %d <- page %d", DEBUG_THREAD_ID, node->GetPageId(),
                         adjacent_node->GetPageId());
        if (!node->IsLeafPage()) {
          auto pair = ToInternal(adjacent_node)->PopBack();
          ToInternal(node)->Get()[0].first = between_key;
          ToInternal(node)->PushFront(pair.second);
          parent_node->SetKeyAt(between_key_index, pair.first);
          NodeChangeParent(pair.second, node->GetPageId(), latched_pages);
        } else {
          auto pair = ToLeaf(adjacent_node)->PopBack();
          ToLeaf(node)->Insert(pair.first, pair.second, comparator_);
          parent_node->SetKeyAt(between_key_index, pair.first);
        }
      } else /* Symmetric case*/ {
        if (!node->IsLeafPage()) {
          auto pair = ToInternal(adjacent_node)->PopFront();
          ToInternal(node)->PushBack(pair.first, pair.second);
          parent_node->SetKeyAt(between_key_index, ToInternal(adjacent_node)->Get()[0].first);
          NodeChangeParent(pair.second, node->GetPageId(), latched_pages);
        } else {
          auto pair = ToLeaf(adjacent_node)->PopFront();
          ToLeaf(node)->Insert(pair.first, pair.second, comparator_);
          parent_node->SetKeyAt(between_key_index, ToLeaf(adjacent_node)->KeyAt(0));
        }
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Coalesce(bustub::BPlusTreePage *predecessor, bustub::BPlusTreePage *node,
                              const KeyType &between_key, LatchedPageContainer *latched_pages,
                              Transaction *transaction) {
  THREAD_DEBUG_LOG("(thread %ld) coalesce page %d to page %d", DEBUG_THREAD_ID, node->GetPageId(),
                   predecessor->GetPageId());
  if (!node->IsLeafPage()) {
    auto pairs = ToInternal(node)->ExtractAll();
    pairs[0].first = between_key;
    ToInternal(predecessor)->EmplaceBack(pairs);
    for (const auto &[_, page_id] : pairs) {
      NodeChangeParent(page_id, predecessor->GetPageId(), latched_pages);
    }
  } else {
    auto pairs = ToLeaf(node)->ExtractAll();
    ToLeaf(predecessor)->EmplaceBack(pairs);
    ToLeaf(predecessor)->SetNextPageId(ToLeaf(node)->GetNextPageId());
  }
  latched_pages->erase(
      std::remove_if(latched_pages->begin(), latched_pages->end(),
                     [page_id = node->GetPageId()](Page *page) { return page_id == page->GetPageId(); }));
  DeletePage(ToRawPage(node), UseMode::Write, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::NodeChangeParent(page_id_t page_id, page_id_t parent_id, LatchedPageContainer *latched_pages) {
  auto found_iter = std::find_if(latched_pages->begin(), latched_pages->end(),
                                 [page_id](Page *page) { return page->GetPageId() == page_id; });
  if (found_iter != latched_pages->end()) {
    ToTreePage(*found_iter)->SetParentPageId(parent_id);
  } else {
    auto page = buffer_pool_manager_->FetchPage(page_id);
    page->WLatch();
    ToTreePage(page)->SetParentPageId(parent_id);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  // TODO(hoo): Implementation for concurrent correctness after finishing project 2.
  THREAD_DEBUG_LOG("Enters");
  if (GetRootPageId() == INVALID_PAGE_ID) {
    return End();
  }
  auto page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_));
  BUSTUB_ASSERT(page != nullptr, "unexpected case");
  while (!page->IsLeafPage()) {
    auto internal_page = static_cast<InternalPage *>(page);
    BUSTUB_ASSERT(internal_page->GetSize() != 0, "unexpected size");
    auto next_page_id = internal_page->ValueAt(0);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(next_page_id));
    THREAD_DEBUG_LOG("next page id = %d, page_id=%d", next_page_id, page->GetPageId());
    BUSTUB_ASSERT(next_page_id == page->GetPageId(), "unexpected case");
  }
  auto itr_page_id = page->GetPageId();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return INDEXITERATOR_TYPE(itr_page_id, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low-key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  // TODO(hoo): Implementation for concurrent correctness after finishing project 2.
  THREAD_DEBUG_LOG("enters");
  bool success;
  auto leaf = OptimisticSearch(key, SearchMode::Find, nullptr, success);
  const auto size = leaf->GetSize();
  int i;
  for (i = 0; i < size; ++i) {
    if (comparator_(key, leaf->KeyAt(i)) == 0) {
      break;
    }
  }
  auto itr_page_id = (i == size) ? INVALID_PAGE_ID : leaf->GetPageId();
  auto itr_index = i;
  auto itr_buffer_pool_manager = (i == size) ? nullptr : buffer_pool_manager_;
  ToRawPage(leaf)->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  THREAD_DEBUG_LOG("return valid pointer ? %s", (i == size) ? "invalid" : "valid");
  return INDEXITERATOR_TYPE(itr_page_id, itr_index, itr_buffer_pool_manager);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(INVALID_PAGE_ID, 0, nullptr); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::UsePage(page_id_t page_id, UseMode mode, Transaction *transaction) -> Page * {
  THREAD_DEBUG_LOG("(thread %ld) use page %d status : begin", DEBUG_THREAD_ID, page_id);
  auto page = (page_id != INVALID_PAGE_ID) ? buffer_pool_manager_->FetchPage(page_id) : root_page_id_page_.get();
  switch (mode) {
    case UseMode::Read:
      page->RLatch();
      break;
    case UseMode::Write:
      page->WLatch();
      break;
  }
  if (page_id != INVALID_PAGE_ID && transaction != nullptr) {
    transaction->AddIntoPageSet(page);
  }
  THREAD_DEBUG_LOG("(thread %ld) use page %d status : success", DEBUG_THREAD_ID, page_id);
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DisusePage(Page *page, UseMode mode) {
  THREAD_DEBUG_LOG("(thread %ld) disuse page %d : enter", DEBUG_THREAD_ID, page->GetPageId());
  bool is_dirty;
  switch (mode) {
    case UseMode::Read:
      page->RUnlatch();
      is_dirty = false;
      break;
    case UseMode::Write:
      page->WUnlatch();
      is_dirty = true;
      break;
  }
  if (page->GetPageId() != INVALID_PAGE_ID) {
    buffer_pool_manager_->UnpinPage(page->GetPageId(), is_dirty);
  }
  THREAD_DEBUG_LOG("(thread %ld) disuse page %d : done", DEBUG_THREAD_ID, page->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeletePage(Page *page, UseMode mode, bustub::Transaction *transaction) {
  THREAD_DEBUG_LOG("(thread %ld) delete page %d", DEBUG_THREAD_ID, page->GetPageId());
  const auto page_id = page->GetPageId();
  DisusePage(page, mode);
  if (transaction != nullptr) {
    transaction->AddIntoDeletedPageSet(page_id);
  }
  buffer_pool_manager_->DeletePage(page_id);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::PessimisticSearch(const KeyType &key, SearchMode mode, Transaction *transaction,
                                       LatchedPageContainer *latched_pages) -> LeafPage * {
  const auto use_mode = UseMode::Write;
  // This would add the page to latched_pages automatically if insert or delete.
  const auto smart_use = [this, transaction, latched_pages](page_id_t page_id) -> Page * {
    Page *page;
    page = UsePage(page_id, use_mode, transaction);
    latched_pages->push_back(page);
    return page;
  };
  const auto reset_latched_pages = [this, latched_pages](BPlusTreePage *tree_page) {
    auto num_remove_items = latched_pages->size() - 1;
    for (auto iter = latched_pages->begin(); num_remove_items != 0; --num_remove_items) {
      DisusePage(*iter, use_mode);
      ++iter;
      latched_pages->pop_front();
    }
  };
  const auto is_safe_predicate = (mode == SearchMode::Insert)
      ? [](BPlusTreePage *tree_page, int cur_size_for_insert, int cur_size_for_delete) -> bool {
    return cur_size_for_insert < tree_page->GetMaxSize();
  }
  : [](BPlusTreePage *tree_page, int cur_size_for_insert, int cur_size_for_delete) -> bool {
      return (cur_size_for_delete > tree_page->GetMinSize()) &&
             (cur_size_for_delete > ((tree_page->IsLeafPage() ? tree_page->GetMaxSize() - 1 : tree_page->GetMaxSize()) -
                                     tree_page->GetMinSize() + 1));
    };
  auto root_id_page = smart_use(INVALID_PAGE_ID);
  (void)(root_id_page);
  const bool has_root = (root_page_id_ != INVALID_PAGE_ID);
  if (!has_root) {
    return nullptr;
  }
  auto tree_page = ToTreePage(smart_use(root_page_id_));
  while (!tree_page->IsLeafPage()) {
    if (!tree_page->IsRootPage() && is_safe_predicate(tree_page, tree_page->GetSize(), tree_page->GetSize())) {
      reset_latched_pages(tree_page);
    }
    const auto size = ToInternal(tree_page)->GetSize();
    page_id_t next_page_id = ToInternal(tree_page)->ValueAt(0);
    for (int i = 0; i < size; ++i) {
      if ((i == (size - 1)) || (comparator_(ToInternal(tree_page)->KeyAt(i + 1), key) > 0)) {
        next_page_id = ToInternal(tree_page)->ValueAt(i);
        break;
      }
    }
    tree_page = ToTreePage(smart_use(next_page_id));
  }
  if (!tree_page->IsRootPage() && is_safe_predicate(tree_page, tree_page->GetSize() + 1, tree_page->GetSize())) {
    reset_latched_pages(tree_page);
  }
  return static_cast<LeafPage *>(tree_page);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::OptimisticSearch(const KeyType &key, SearchMode mode, bustub::Transaction *transaction,
                                      bool &success) -> LeafPage * {
  const auto use_mode = UseMode::Read;
  const auto is_safe_predicate = (mode == SearchMode::Insert)
      ? [](BPlusTreePage *tree_page, int cur_size_for_insert, int cur_size_for_delete) -> bool {
    return cur_size_for_insert < tree_page->GetMaxSize();
  }
  : [](BPlusTreePage *tree_page, int cur_size_for_insert, int cur_size_for_delete) -> bool {
      return (cur_size_for_delete > tree_page->GetMinSize()) &&
             (cur_size_for_delete > ((tree_page->IsLeafPage() ? tree_page->GetMaxSize() - 1 : tree_page->GetMaxSize()) -
                                     tree_page->GetMinSize() + 1));
    };
  const auto smart_latch = [this, transaction](BPlusTreePage *tree_page) {
    tree_page->IsLeafPage() ? ToRawPage(tree_page)->WLatch() : ToRawPage(tree_page)->RLatch();
    if (transaction != nullptr) {
      transaction->AddIntoPageSet(ToRawPage(tree_page));
    }
  };
  auto root_id_page = UsePage(INVALID_PAGE_ID, use_mode, transaction);
  auto root_id = root_page_id_;
  success = true;
  if (root_page_id_ == INVALID_PAGE_ID) {
    DisusePage(root_id_page, use_mode);
    success = false;
    return nullptr;
  }
  auto tree_page = ToTreePage(buffer_pool_manager_->FetchPage(root_id));
  smart_latch(tree_page);
  DisusePage(root_id_page, use_mode);
  while (!tree_page->IsLeafPage()) {
    const auto size = ToInternal(tree_page)->GetSize();
    page_id_t next_page_id = ToInternal(tree_page)->ValueAt(0);
    for (int i = 0; i < size; ++i) {
      if ((i == (size - 1)) || (comparator_(ToInternal(tree_page)->KeyAt(i + 1), key) > 0)) {
        next_page_id = ToInternal(tree_page)->ValueAt(i);
        break;
      }
    }
    auto next_page = ToTreePage(buffer_pool_manager_->FetchPage(next_page_id));
    smart_latch(next_page);
    ToRawPage(tree_page)->RUnlatch();
    buffer_pool_manager_->UnpinPage(tree_page->GetPageId(), false);
    tree_page = next_page;
  }
  if (mode != SearchMode::Find && !is_safe_predicate(tree_page, tree_page->GetSize() + 1, tree_page->GetSize())) {
    success = false;
  }
  return static_cast<LeafPage *>(tree_page);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  THREAD_DEBUG_LOG("(thread %ld) update root id to be %d", DEBUG_THREAD_ID, insert_record);
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
           "CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
           "CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::KeyToString(const KeyType &key) const -> std::string {
#ifdef HOO_ALLOW_DEBUG_LOG
  std::stringstream buf;
  buf << key;
  return buf.str();
#else
  return std::string{};
#endif
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ValueToString(const ValueType &value) const -> std::string {
  std::stringstream buf;
  buf << value;
  auto res = buf.str();
  res.resize(res.size() - 1);
  return res;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
