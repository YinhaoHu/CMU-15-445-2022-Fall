//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()),
      empty_status_(EmptyStatus::kEmpty),
      initialized_(false) {}

void AggregationExecutor::Init() {
  if (child_ == nullptr) {
    return;
  }
  if (!initialized_) {
    child_->Init();
    Tuple tuple;
    RID rid;
    while (child_->Next(&tuple, &rid)) {
      aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
    }
    initialized_ = true;
  }
  aht_iterator_ = aht_.Begin();
  if (aht_iterator_ != aht_.End()) {
    empty_status_ = EmptyStatus::kNotEmpty;
  }
}
/**
 *  output format: [group_bys_] , [aggregates_]
 */
auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(Hoo): Problem might be cause by rid, but I do not know the use of rid.
  if (empty_status_ == EmptyStatus::kEmpty) {
    empty_status_ = EmptyStatus::kReturnedForEmpty;
    auto agg_values = aht_.GenerateInitialAggregateValue().aggregates_;
    auto delta_size = plan_->output_schema_->GetColumnCount() - agg_values.size();
    if (delta_size > 0) {
      return false;
    }
    *tuple = Tuple{agg_values, plan_->output_schema_.get()};
    return true;
  }
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  std::vector<Value> values;
  for (const auto &key : aht_iterator_.Key().group_bys_) {
    values.emplace_back(key);
  }
  for (const auto &value : aht_iterator_.Val().aggregates_) {
    values.emplace_back(value);
  }
  *tuple = Tuple(values, plan_->output_schema_.get());
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
