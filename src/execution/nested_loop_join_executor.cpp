//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_executor_(std::move(left_executor)),
      right_child_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_child_executor_->Init();
  right_child_executor_->Init();
  done_ = !left_child_executor_->Next(&current_left_tuple_, &current_left_rid_);
  current_left_tuple_matched_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  bool result = false;
  switch (plan_->GetJoinType()) {
    case JoinType::LEFT:
      result = LeftJoin(tuple, rid);
      break;
    case JoinType::INNER:
      result = InnerJoin(tuple, rid);
      break;
    default:
      throw bustub::NotImplementedException(fmt::format("Not implemented for join type {}", plan_->GetJoinType()));
  }
  return result;
}

auto NestedLoopJoinExecutor::LeftJoin(bustub::Tuple *tuple, bustub::RID *rid) -> bool {
START:
  if (done_) {
    return false;
  }
  Tuple right_tuple;
  RID right_rid;
  while (right_child_executor_->Next(&right_tuple, &right_rid)) {
    auto result = plan_->Predicate().EvaluateJoin(&current_left_tuple_, left_child_executor_->GetOutputSchema(),
                                                  &right_tuple, right_child_executor_->GetOutputSchema());
    if (!result.IsNull() && result.GetAs<bool>()) {
      std::vector<Value> values;
      AddTupleValuesTo(values, &current_left_tuple_, left_child_executor_->GetOutputSchema());
      AddTupleValuesTo(values, &right_tuple, right_child_executor_->GetOutputSchema());
      *tuple = Tuple(values, &GetOutputSchema());
      current_left_tuple_matched_ = true;
      return true;
    }
  }
  bool generated_for_unmatched = false;
  if (!current_left_tuple_matched_) {
    std::vector<Value> values;
    AddTupleValuesTo(values, &current_left_tuple_, left_child_executor_->GetOutputSchema());
    const auto column_count = right_child_executor_->GetOutputSchema().GetColumnCount();
    for (uint32_t i = 0; i < column_count; ++i) {
      values.emplace_back(
          ValueFactory::GetNullValueByType(right_child_executor_->GetOutputSchema().GetColumn(i).GetType()));
    }
    *tuple = Tuple(std::move(values), &GetOutputSchema());
    generated_for_unmatched = true;
  }
  done_ = !left_child_executor_->Next(&current_left_tuple_, &current_left_rid_);
  if (!done_) {
    right_child_executor_->Init();
    current_left_tuple_matched_ = false;
  }
  if (generated_for_unmatched) {
    return true;
  }
  goto START;
}

auto NestedLoopJoinExecutor::InnerJoin(bustub::Tuple *tuple, bustub::RID *rid) -> bool {
START:
  if (done_) {
    return false;
  }
  Tuple right_tuple;
  RID right_rid;
  while (right_child_executor_->Next(&right_tuple, &right_rid)) {
    auto result = plan_->Predicate().EvaluateJoin(&current_left_tuple_, left_child_executor_->GetOutputSchema(),
                                                  &right_tuple, right_child_executor_->GetOutputSchema());
    if (!result.IsNull() && result.GetAs<bool>()) {
      std::vector<Value> values;
      AddTupleValuesTo(values, &current_left_tuple_, left_child_executor_->GetOutputSchema());
      AddTupleValuesTo(values, &right_tuple, right_child_executor_->GetOutputSchema());
      *tuple = Tuple(values, &GetOutputSchema());
      return true;
    }
  }
  done_ = !left_child_executor_->Next(&current_left_tuple_, &current_left_rid_);
  if (!done_) {
    right_child_executor_->Init();
  }
  goto START;
}

void NestedLoopJoinExecutor::AddTupleValuesTo(std::vector<Value> &values, bustub::Tuple *tuple,
                                              const bustub::Schema &schema) {
  const auto column_count = schema.GetColumnCount();
  for (uint32_t i = 0; i < column_count; ++i) {
    values.emplace_back(tuple->GetValue(&schema, i));
  }
}

}  // namespace bustub
