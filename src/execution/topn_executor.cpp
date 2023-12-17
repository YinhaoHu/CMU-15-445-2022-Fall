#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), result_generated_(false), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  if (!result_generated_) {
    child_executor_->Init();
    result_generated_ = true;
    auto cmp = [this](const std::pair<Tuple, RID> &x, const std::pair<Tuple, RID> &y) -> bool {
      const auto order_bys = plan_->GetOrderBy();
      const auto &x_tuple = x.first;
      const auto &y_tuple = y.first;
      for (const auto &order_by : order_bys) {
        bool result = true;
        const auto x_key = order_by.second->Evaluate(&x_tuple, child_executor_->GetOutputSchema());
        const auto y_key = order_by.second->Evaluate(&y_tuple, child_executor_->GetOutputSchema());
        switch (order_by.first) {
          case OrderByType::INVALID:
            bustub::NotImplementedException("not implemented for OrderByType::INVALID");
            break;
          case OrderByType::DESC:
            result = x_key.CompareGreaterThan(y_key) == CmpBool::CmpTrue;
            break;
          default:
            result = x_key.CompareLessThan(y_key) == CmpBool::CmpTrue;
        }
        if (result) {
          return true;
        }
        if (x_key.CompareEquals(y_key) == CmpBool::CmpFalse) {
          return false;
        }
      }
      return false;
    };
    result_ = std::make_unique<ResultContainer>();
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
      result_->emplace_back(tuple, rid);
      std::push_heap(result_->begin(), result_->end(), cmp);
      if (result_->size() > plan_->GetN()) {
        std::pop_heap(result_->begin(), result_->end(), cmp);
        result_->pop_back();
      }
    }
    std::sort_heap(result_->begin(), result_->end(), cmp);
  }
  current_iterator_ = result_->begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (current_iterator_ == result_->end()) {
    return false;
  }
  *tuple = current_iterator_->first;
  *rid = current_iterator_->second;
  ++current_iterator_;
  return true;
}

}  // namespace bustub
