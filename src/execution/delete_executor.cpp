//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), done_(false), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  table_ = table_info->table_.get();
  schema_ = std::make_unique<Schema>(table_info->schema_);
  indices_ = std::make_unique<std::vector<IndexInfo *>>(exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_));
  done_ = false;
  // Lock table. IX mode in any isolation level.
  try {
    auto ok = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
                                                     LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->table_oid_);
    if (!ok) {
      exec_ctx_->GetTransaction()->LockTxn();
      exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
      exec_ctx_->GetTransaction()->UnlockTxn();
      throw ExecutionException("DeleteExecutor fails to lock table");
    }
  } catch (TransactionAbortException &err) {
    throw ExecutionException(err.GetInfo());
  }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    // Unlock the rows. It is not allowed under any isolation level.
    // Unlock the table. Under repeatable read is not allowed ,too.
    return false;
  }
  Tuple tuple_to_delete;
  RID rid_to_delete;
  int32_t num_deleted(0);
  Schema schema(std::vector<Column>{Column("size", TypeId::INTEGER)});
  while (child_executor_->Next(&tuple_to_delete, &rid_to_delete)) {
    // Lock the row in X mode under any isolation level.
    try {
      auto ok = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                     plan_->table_oid_, rid_to_delete);
      if (!ok) {
        exec_ctx_->GetTransaction()->LockTxn();
        exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
        exec_ctx_->GetTransaction()->UnlockTxn();
        throw ExecutionException("DeleteExecutor fails to lock row");
      }
      locked_rids_.emplace_back(rid_to_delete);
    } catch (TransactionAbortException &err) {
      throw ExecutionException(err.GetInfo());
    }
    table_->MarkDelete(rid_to_delete, exec_ctx_->GetTransaction());
    for (auto index_info : *indices_) {
      index_info->index_->DeleteEntry(
          tuple_to_delete.KeyFromTuple(*schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          rid_to_delete, exec_ctx_->GetTransaction());
    }
    ++num_deleted;
  }
  *tuple = Tuple(std::vector<Value>{Value(TypeId::INTEGER, num_deleted)}, &schema);
  done_ = true;
  return true;
}
}  // namespace bustub
