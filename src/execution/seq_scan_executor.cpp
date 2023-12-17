

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  table_current_iterator_ = std::make_unique<TableIterator>(table_->Begin(exec_ctx_->GetTransaction()));
  table_end_iterator_ = std::make_unique<TableIterator>(table_->End());
  // Lock the table.
  try {
    auto txn = exec_ctx_->GetTransaction();
    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      auto ok = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
                                                       LockManager::LockMode::INTENTION_SHARED, plan_->table_oid_);
      if (!ok) {
        throw ExecutionException("SqeScanExecutor fails to lock table");
      }
    }
  } catch (TransactionAbortException e) {
    throw ExecutionException(e.GetInfo());
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (*table_current_iterator_ == table_->End()) {
    // Unlock the rows.
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      std::for_each(locked_rids_.cbegin(), locked_rids_.cend(), [&](const RID &locked_rid) {
        auto ok = exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->table_oid_, locked_rid);
        if (!ok) {
          exec_ctx_->GetTransaction()->LockTxn();
          exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
          exec_ctx_->GetTransaction()->UnlockTxn();
        }
      });
      // Unlock the table.
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
        auto ok = exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), plan_->table_oid_);
        if (!ok) {
          exec_ctx_->GetTransaction()->LockTxn();
          exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
          exec_ctx_->GetTransaction()->UnlockTxn();
        }
      }
    }
    return false;
  }
  *rid = (*table_current_iterator_)->GetRid();
  table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  // Lock row.
  try {
    auto txn = exec_ctx_->GetTransaction();
    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      auto ok = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                     plan_->table_oid_, *rid);
      if (!ok) {
        throw ExecutionException("SqeScanExecutor fails to lock row");
      }
      locked_rids_.emplace_back(*rid);
    }
  } catch (TransactionAbortException e) {
    throw ExecutionException(e.GetInfo());
  }
  ++(*table_current_iterator_);
  return true;
}

}  // namespace bustub
