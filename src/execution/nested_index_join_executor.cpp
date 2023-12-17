//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple left_tuple;
  RID left_rid;
  while (child_executor_->Next(&left_tuple, &left_rid)) {
    // Generate search key.
    auto &index = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_;
    auto key_value = plan_->KeyPredicate()->Evaluate(&left_tuple, child_executor_->GetOutputSchema());
    std::vector<Value> key;
    key.emplace_back(key_value);
    Schema schema(std::vector<Column>{Column("column", key_value.GetTypeId())});
    Tuple search_key(key, &schema);
    std::vector<RID> result;
    index->ScanKey(search_key, &result, exec_ctx_->GetTransaction());
    auto inner_table = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
    if (result.empty()) {
      if (plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> values;
        AddTupleValuesTo(values, &left_tuple, child_executor_->GetOutputSchema());
        const auto column_count = inner_table->schema_.GetColumnCount();
        for (uint32_t i = 0; i < column_count; ++i) {
          values.emplace_back(ValueFactory::GetNullValueByType(inner_table->schema_.GetColumn(i).GetType()));
        }
        *tuple = Tuple(values, &GetOutputSchema());
        return true;
      }
      continue;
    }
    // Look up inner tuple.
    Tuple right_tuple;
    assert(inner_table->table_->GetTuple(result.front(), &right_tuple, exec_ctx_->GetTransaction()));
    std::vector<Value> values;
    AddTupleValuesTo(values, &left_tuple, child_executor_->GetOutputSchema());
    AddTupleValuesTo(values, &right_tuple, inner_table->schema_);
    *tuple = Tuple(values, &GetOutputSchema());
    return true;
  }
  return false;
}

void NestIndexJoinExecutor::AddTupleValuesTo(std::vector<Value> &values, bustub::Tuple *tuple,
                                             const bustub::Schema &schema) {
  const auto column_count = schema.GetColumnCount();
  for (uint32_t i = 0; i < column_count; ++i) {
    values.emplace_back(tuple->GetValue(&schema, i));
  }
}
}  // namespace bustub
