//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());
  current_iterator_ =
      std::make_unique<IndexIterator<GenericKey<4>, RID, GenericComparator<4>>>(tree_->GetBeginIterator());
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (*current_iterator_ == tree_->GetEndIterator()) {
    return false;
  }
  *rid = (**current_iterator_).second;
  table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  ++(*current_iterator_);
  return true;
}

}  // namespace bustub
