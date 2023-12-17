#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), result_generated_(false), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  if (!result_generated_) {
    child_executor_->Init();
    result_generated_ = true;
    result_ = std::make_unique<ResultContainer>();
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
      result_->emplace_back(tuple, rid);
    }
    std::sort(result_->begin(), result_->end(),
              [this](const std::pair<Tuple, RID> &x, const std::pair<Tuple, RID> &y) -> bool {
                const auto order_bys = plan_->GetOrderBy();
                const auto &x_tuple = x.first;
                const auto &y_tuple = y.first;
                for (const auto &order_by : order_bys) {
                  bool less_than = true;
                  const auto x_key = order_by.second->Evaluate(&x_tuple, child_executor_->GetOutputSchema());
                  const auto y_key = order_by.second->Evaluate(&y_tuple, child_executor_->GetOutputSchema());
                  switch (order_by.first) {
                    case OrderByType::INVALID:
                      bustub::NotImplementedException("not implemented for OrderByType::INVALID");
                      break;
                    case OrderByType::DESC:
                      less_than = x_key.CompareGreaterThan(y_key) == CmpBool::CmpTrue;
                      break;
                    default:
                      less_than = x_key.CompareLessThan(y_key) == CmpBool::CmpTrue;
                  }
                  if (less_than) {
                    return true;
                  }
                  if (x_key.CompareEquals(y_key) == CmpBool::CmpFalse) {
                    return false;
                  }
                }
                return false;
              });
  }
  current_iterator_ = result_->begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (current_iterator_ == result_->end()) {
    return false;
  }
  *tuple = current_iterator_->first;
  *rid = current_iterator_->second;
  ++current_iterator_;
  return true;
}

}  // namespace bustub
