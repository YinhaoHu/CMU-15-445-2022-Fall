#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // FIXME :
  // explain select * from (select * from test_simple_seq_2 order by col1 desc limit 5) order by col1 asc limit 3;
  // TODO(Hoo) - 26,Nov : Fix the above case and probably the TOP-N algorithm(some will fail and some pass in test case
  // 13,14).
  std::vector<AbstractPlanNodeRef> children;
  for (auto &node : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(node));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Limit) {
    BUSTUB_ENSURE(optimized_plan->GetChildren().size() == 1, "limit node should has only one child.");
    auto child = optimized_plan->children_[0];
    if (child->GetType() == PlanType::Sort) {
      BUSTUB_ENSURE(child->GetChildren().size() == 1, "child sort node should has only one child.");
      const auto &this_node_as_limit = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
      const auto &child_node_as_sort = dynamic_cast<const SortPlanNode &>(*child);
      return std::make_shared<TopNPlanNode>(optimized_plan->output_schema_, child_node_as_sort.children_[0],
                                            child_node_as_sort.order_bys_, this_node_as_limit.limit_);
    }
  }
  return optimized_plan;
}

}  // namespace bustub
