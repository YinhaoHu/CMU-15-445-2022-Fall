//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

// Note for 2022 Fall: You don't need to implement HashJoinExecutor to pass all
// tests. You ONLY need to implement it if you want to get faster in leaderboard
// tests.

namespace bustub {
auto operator==(const bustub::Value &x, const bustub::Value &y) -> bool {
  return x.CompareEquals(y) == bustub::CmpBool::CmpTrue;
}

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      done_(false),
      result_generated_(false),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  if (!result_generated_) {
    result_generated_ = true;
    left_child_->Init();
    right_child_->Init();
    Build();
    done_ = !left_child_->Next(&current_left_tuple_, &current_left_rid_);
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
START:
  if (done_) {
    return false;
  }
  Tuple right_tuple;
  RID right_rid;
  auto left_key = plan_->LeftJoinKeyExpression().Evaluate(&current_left_tuple_, left_child_->GetOutputSchema());
  if (hash_table_.Find(left_key, right_tuple, right_rid)) {
    std::vector<Value> values;
    AddTupleValuesTo(values, &current_left_tuple_, left_child_->GetOutputSchema());
    AddTupleValuesTo(values, &right_tuple, right_child_->GetOutputSchema());
    *tuple = Tuple(std::move(values), &GetOutputSchema());
    return true;
  }
  if (plan_->GetJoinType() == JoinType::LEFT && !hash_table_.Contain(left_key)) {
    std::vector<Value> values;
    AddTupleValuesTo(values, &current_left_tuple_, left_child_->GetOutputSchema());
    const auto column_count = right_child_->GetOutputSchema().GetColumnCount();
    for (uint32_t i = 0; i < column_count; ++i) {
      values.emplace_back(ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(i).GetType()));
    }
    *tuple = Tuple(std::move(values), &GetOutputSchema());
    done_ = !left_child_->Next(&current_left_tuple_, &current_left_rid_);
    return true;
  }
  hash_table_.Reset(plan_->LeftJoinKeyExpression().Evaluate(&current_left_tuple_, left_child_->GetOutputSchema()));
  done_ = !left_child_->Next(&current_left_tuple_, &current_left_rid_);
  goto START;
}

void HashJoinExecutor::Build() {
  // Build using the right table.
  Tuple tuple;
  RID rid;
  while (right_child_->Next(&tuple, &rid)) {
    hash_table_.Insert(plan_->RightJoinKeyExpression().Evaluate(&tuple, right_child_->GetOutputSchema()), tuple, rid);
  }
}

void HashJoinExecutor::AddTupleValuesTo(std::vector<Value> &values, bustub::Tuple *tuple,
                                        const bustub::Schema &schema) {
  const auto column_count = schema.GetColumnCount();
  for (uint32_t i = 0; i < column_count; ++i) {
    values.emplace_back(tuple->GetValue(&schema, i));
  }
}

}  // namespace bustub
