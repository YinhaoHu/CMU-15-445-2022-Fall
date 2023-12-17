//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <list>
#include <memory>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  using LatchedPageContainer = std::list<Page *>;
  enum class SearchMode { Find, Insert, Delete };
  enum class UseMode { Read, Write };

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

 private:
  auto UsePage(page_id_t page_id, UseMode mode, Transaction *transaction) -> Page *;

  void DisusePage(Page *page, UseMode mode);

  void DeletePage(Page *page, UseMode mode, Transaction *transaction);

  /**
   * @brief Find the leaf to which the key should go.
   *
   * @return The pointer to the page if there is a potentially proper page the key should go,
   * nullptr if not.
   *
   * @note Remember to `unpin` the page after accessing/modifying the page.
   */
  auto PessimisticSearch(const KeyType &key, SearchMode mode, Transaction *transaction = nullptr,
                         LatchedPageContainer *latched_pages = nullptr) -> LeafPage *;

  auto OptimisticSearch(const KeyType &key, SearchMode mode, Transaction *transaction, bool &success) -> LeafPage *;

  /**
   * @brief Insert the kv pair into the parent node of `node`.
   *
   * @param node The same level node with value.
   * @param key The key of the kv pair
   * @param value The value of the kv pair
   */
  void InsertInParent(LeafPage *node, const KeyType &key, BPlusTreePage *other_node,
                      LatchedPageContainer *latched_pages, Transaction *transaction);

  /**
   * @brife Remove the entry associated with the key in the node.
   * @param node
   * @param key
   *
   * @note Do NOT unpin the node page before invoking this function.
   */
  void RemoveEntry(BPlusTreePage *node, const KeyType &key, LatchedPageContainer *latched_pages,
                   Transaction *transaction);

  /**
   * @brife Coalesce the entries in node to predecessor.
   */
  void Coalesce(BPlusTreePage *predecessor, BPlusTreePage *node, const KeyType &between_key,
                LatchedPageContainer *latched_pages, Transaction *transaction);

  auto inline ToRawPage(BPlusTreePage *tree_page) -> Page * { return reinterpret_cast<Page *>(tree_page); };

  auto inline ToTreePage(Page *page) -> BPlusTreePage * { return reinterpret_cast<BPlusTreePage *>(page); }

  auto inline ToLeaf(BPlusTreePage *node) -> LeafPage * { return reinterpret_cast<LeafPage *>(node); }

  auto inline ToInternal(BPlusTreePage *node) -> InternalPage * { return reinterpret_cast<InternalPage *>(node); }

  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  auto KeyToString(const KeyType &key) const -> std::string;

  auto ValueToString(const ValueType &value) const -> std::string;

  void NodeChangeParent(page_id_t page_id, page_id_t parent_id, LatchedPageContainer *latched_pages);

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
  // Used for uniformly handle the unlatch case.
  std::unique_ptr<Page> root_page_id_page_;
};

}  // namespace bustub
