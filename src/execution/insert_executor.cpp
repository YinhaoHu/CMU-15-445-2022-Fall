//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * =======================INSERT=======================
 *
 * The InsertExecutor inserts tuples into a table and updates indexes.
 *
 * Input: It has exactly one child producing values to be inserted into the table.
 * The planner will ensure values have the same schema as the table.
 *
 * Output: The executor will produce a single tuple of an integer number as the output,
 * indicating how many rows have been inserted into the table, after all rows are inserted.
 *
 * Remember to update the index when inserting into the table, if it has an index associated with it.
 *
 * Hint: You will need to lookup table information for the target of the insert during executor initialization.
 * See the System Catalog section below for additional information on accessing the catalog.
 *
 * Hint: You will need to update all indexes for the table into which tuples are inserted.
 * See the Index Updates section below for further details.
 *
 * Hint: You will need to use the TableHeap class to perform table modifications.
 *
 * Keys: table , index.
 */

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
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
      throw ExecutionException("InsertExecutor fails to lock table");
    }
  } catch (TransactionAbortException &err) {
    throw ExecutionException(err.GetInfo());
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    // Unlock the rows. It is not allowed under any isolation level.
    // Unlock the table. Under repeatable read.
    return false;
  }
  Tuple tuple_to_insert;
  RID rid_to_insert;
  int32_t num_inserted(0);
  Schema schema(std::vector<Column>{Column("size", TypeId::INTEGER)});
  while (child_executor_->Next(&tuple_to_insert, &rid_to_insert)) {
    // Lock the row in X mode under any isolation level.
    try {
      auto ok = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                     plan_->table_oid_, rid_to_insert);
      if (!ok) {
        exec_ctx_->GetTransaction()->LockTxn();
        exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
        exec_ctx_->GetTransaction()->UnlockTxn();
        throw ExecutionException("InsertExecutor fails to lock row");
      }
      locked_rids_.emplace_back(rid_to_insert);
    } catch (TransactionAbortException &err) {
      throw ExecutionException(err.GetInfo());
    }
    table_->InsertTuple(tuple_to_insert, &rid_to_insert, exec_ctx_->GetTransaction());
    for (auto index_info : *indices_) {
      Tuple index_tuple =
          tuple_to_insert.KeyFromTuple(*schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(index_tuple, rid_to_insert, exec_ctx_->GetTransaction());
    }
    ++num_inserted;
  }
  *tuple = Tuple(std::vector<Value>{Value(TypeId::INTEGER, num_inserted)}, &schema);
  done_ = true;
  return true;
}

}  // namespace bustub
