#include <string>
#include <unordered_set>

#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/mock_scan_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/values_plan.h"
#include "optimizer/optimizer.h"

// Note for 2022 Fall: You can add all optimizer rule implementations and apply
// the rules as you want in this file. Note that for some test cases, we force
// using starter rules, so that the configuration here won't take effects.
// Starter rule can be forcibly enabled by `set
// force_optimizer_starter_rule=yes`.

namespace bustub {

// For leaderboard 1. (Not totally finished)

auto Optimizer::OptimizeReorderingJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // Optimization condition: two joins with three scan below.
  // Reorder the three scan first based on the cardinality.
  // Note: optimize only for the case where the top node is aggregation, which means for q1, for simplicity.
  // TODO(Hoo) - Non-determined date: I will implement this if I have time.
  return plan;
}

auto Optimizer::GetScanNodeTableName(const AbstractPlanNode &scan_plan) -> std::string {
  switch (scan_plan.GetType()) {
    case PlanType::MockScan:
      return dynamic_cast<const MockScanPlanNode &>(scan_plan).GetTable();
    case PlanType::SeqScan:
      return dynamic_cast<const SeqScanPlanNode &>(scan_plan).table_name_;
    default:
      BUSTUB_ASSERT(false, "Optimizer::GetScanNodeTableName - parameter is not an allowed scan node");
  }
  return std::string{};
}

void Optimizer::ResetNLJChildren(AbstractPlanNode &plan, const AbstractPlanNodeRef &left, uint32_t left_key_idx,
                                 TypeId left_return_type, const AbstractPlanNodeRef &right, uint32_t right_key_idx,
                                 TypeId right_return_type) {
  // They will change: NLJ output schema, NLJ predicate, NLJ children.
  auto &nlj_plan = dynamic_cast<NestedLoopJoinPlanNode &>(plan);
  BUSTUB_ENSURE(plan.children_.size() == 2, "nlj children should have exactly 2.");
  // Change children.
  nlj_plan.children_[0] = left;
  nlj_plan.children_[1] = right;
  // Change predicate.
  nlj_plan.predicate_->children_[0] = std::make_shared<ColumnValueExpression>(0, left_key_idx, left_return_type);
  nlj_plan.predicate_->children_[1] = std::make_shared<ColumnValueExpression>(1, right_key_idx, right_return_type);
  // Change output schema.
  std::vector<Column> new_output_schema_columns;
  for (size_t i = 0; i < left->output_schema_->GetColumnCount(); ++i) {
    new_output_schema_columns.emplace_back(left->output_schema_->GetColumn(i));
  }
  for (size_t i = 0; i < right->output_schema_->GetColumnCount(); ++i) {
    new_output_schema_columns.emplace_back(right->output_schema_->GetColumn(i));
  }
  nlj_plan.output_schema_ = std::make_unique<Schema>(std::move(new_output_schema_columns));
}

// For leaderboard 2.

auto Optimizer::OptimizeMergeEqualFilterNLJ(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  /* Optimize condition: current node is filter node whose predicate has at least two expressions
   * and one of them is eqi condition expression. Plus, the child node is a join node whose predicate
   * is only true.
   *
   * Optimize behaviour: extract the eqi condition expression of the current node and make the child
   * as the hash join node with the two keys of that eqi expression.
   */
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->children_) {
    children.emplace_back(OptimizeMergeEqualFilterNLJ(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Filter) {
    if (auto &filter_plan = dynamic_cast<FilterPlanNode &>(*optimized_plan);
        filter_plan.predicate_->children_.size() >= 2) {
      if (filter_plan.predicate_->children_[0]->children_.size() < 2) {
        return optimized_plan;
      }
      if (auto child_plan = optimized_plan->GetChildAt(0); child_plan->GetType() == PlanType::NestedLoopJoin) {
        const auto &child_nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*child_plan);
        if (IsPredicateTrue(child_nlj_plan.Predicate())) {
          auto &predicate = filter_plan.predicate_;
          auto [extracted, eqi_expression] = ExtractEqiExpression(predicate);
          auto &left_operand = dynamic_cast<ColumnValueExpression &>(*eqi_expression->children_[0]);
          auto &right_operand = dynamic_cast<ColumnValueExpression &>(*eqi_expression->children_[1]);
          if (left_operand.GetColIdx() > right_operand.GetColIdx()) {
            std::swap(left_operand, right_operand);
          }
          eqi_expression->children_[0] =
              std::make_shared<ColumnValueExpression>(0, left_operand.GetColIdx(), left_operand.GetReturnType());
          eqi_expression->children_[1] = std::make_shared<ColumnValueExpression>(
              0, right_operand.GetColIdx() - child_nlj_plan.GetLeftPlan()->output_schema_->GetColumnCount(),
              right_operand.GetReturnType());
          if (extracted) {
            auto new_left_operand = eqi_expression->children_[0];
            auto new_right_operand = eqi_expression->children_[1];
            auto hash_join_plan = std::make_shared<HashJoinPlanNode>(
                child_nlj_plan.output_schema_, child_nlj_plan.GetLeftPlan(), child_nlj_plan.GetRightPlan(),
                new_left_operand, new_right_operand, child_nlj_plan.GetJoinType());
            BUSTUB_ENSURE(filter_plan.children_.size() == 1, "filter should have only one child.");
            filter_plan.children_[0] = hash_join_plan;
          }
        }
      }
    }
  }
  return optimized_plan;
}

auto Optimizer::ExtractEqiExpressionHelper(bool &extracted, AbstractExpressionRef &eqi_expression,
                                           AbstractExpressionRef &expression) -> int32_t {
  for (auto iter = expression->children_.begin(); iter != expression->children_.end(); ++iter) {
    auto &child_expression = *iter;
    int32_t status = ExtractEqiExpressionHelper(extracted, eqi_expression, child_expression);
    if (status == 1) {
      expression = ((iter + 1) == expression->children_.end()) ? *(iter - 1) : *(iter + 1);
      return status + 1;
    }
    if (status > 1) {
      return status + 1;
    }
  }
  auto *this_expression_as_eqi = dynamic_cast<ComparisonExpression *>(expression.get());
  if (this_expression_as_eqi == nullptr) {
    return 0;
  }
  if (this_expression_as_eqi->comp_type_ != ComparisonType::Equal) {
    return 0;
  }
  if (auto *left_operand = dynamic_cast<ColumnValueExpression *>(this_expression_as_eqi->children_[0].get());
      left_operand == nullptr) {
    return 0;
  }
  if (auto *right_operand = dynamic_cast<ColumnValueExpression *>(this_expression_as_eqi->children_[1].get());
      right_operand == nullptr) {
    return 0;
  }
  eqi_expression = expression;
  extracted = true;
  return 1;
}

/**
 * @brief Extract the eqi expression from `expression`.
 *
 * @param expression The expression from which the equ expression will be extracted.
 * @return True with the extracted eqi expression if found one eqi expression.Otherwise, false.
 */
auto Optimizer::ExtractEqiExpression(AbstractExpressionRef &expression) -> std::pair<bool, AbstractExpressionRef> {
  bool extracted = false;
  AbstractExpressionRef eqi_expression{nullptr};
  ExtractEqiExpressionHelper(extracted, eqi_expression, expression);
  return std::make_pair(extracted, eqi_expression);
}

// For leaderboard 3.

/**
 * @brief Eliminate the common expressions of one aggregation plan node.
 */
auto Optimizer::OptimizeAlwaysFalseExpressionToDummyScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto children = plan->children_;
  for (auto &child_plan : children) {
    child_plan = OptimizeAlwaysFalseExpressionToDummyScan(child_plan);
  }
  AbstractPlanNodeRef optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    auto predicate = filter_plan.predicate_;
    if (auto cmp_expression = dynamic_cast<ComparisonExpression *>(predicate.get()); cmp_expression != nullptr) {
      if (auto left_operand = dynamic_cast<ConstantValueExpression *>(cmp_expression->children_[0].get());
          left_operand != nullptr) {
        if (auto right_operand = dynamic_cast<ConstantValueExpression *>(cmp_expression->children_[1].get());
            right_operand != nullptr) {
          Column left_column("left", left_operand->val_.GetTypeId());
          Column right_column("right", right_operand->val_.GetTypeId());
          Schema schema({left_column, right_column});
          Tuple tuple({left_operand->val_, right_operand->val_}, &schema);
          if (!cmp_expression->Evaluate(&tuple, schema).GetAs<bool>()) {
            auto child = optimized_plan->children_[0];
            auto dummy_scan = std::make_shared<ValuesPlanNode>(ValuesPlanNode(child->output_schema_, {}));
            optimized_plan = optimized_plan->CloneWithChildren({dummy_scan});
          }
        }
      }
    }
  }
  return optimized_plan;
}

auto Optimizer::OptimizeRemoveUnnecessaryComputation(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // Optimize from the top to the bottom. Each node tells the bottom node which columns it needs.
  auto optimized_plan = MergeTwoProjections(plan);
  optimized_plan = SimplifyAggregationBelowProjection(optimized_plan);
  return optimized_plan;
}

auto Optimizer::MergeTwoProjections(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto children = plan->children_;
  for (auto &child : children) {
    child = MergeTwoProjections(child);
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (auto this_proj_plan = dynamic_cast<ProjectionPlanNode *>(optimized_plan.get()); this_proj_plan != nullptr) {
    if (auto child_proj_plan = dynamic_cast<const ProjectionPlanNode *>(this_proj_plan->children_.front().get());
        child_proj_plan != nullptr) {
      std::vector<bool> needed_child_output_columns(child_proj_plan->output_schema_->GetColumnCount(), false);
      auto optimized_child_output_columns = std::vector<Column>{};
      auto optimized_child_expressions = std::vector<AbstractExpressionRef>{};
      for (auto &expr : this_proj_plan->GetExpressions()) {
        size_t col_idx = dynamic_cast<ColumnValueExpression &>(*expr).GetColIdx();
        needed_child_output_columns[col_idx] = true;
      }
      for (size_t i = 0; i < child_proj_plan->output_schema_->GetColumnCount(); ++i) {
        if (needed_child_output_columns[i]) {
          optimized_child_output_columns.emplace_back(child_proj_plan->OutputSchema().GetColumn(i));
          optimized_child_expressions.emplace_back(child_proj_plan->GetExpressions().at(i));
        }
      }
      optimized_plan =
          std::make_unique<ProjectionPlanNode>(std::make_unique<Schema>(optimized_child_output_columns),
                                               optimized_child_expressions, child_proj_plan->children_.front());
    }
  }
  return optimized_plan;
}

void Optimizer::GetAllColumnValueExpressions(const AbstractExpressionRef &root_expr,
                                             std::vector<AbstractExpressionRef> &result) {
  if (!root_expr->children_.empty()) {
    BUSTUB_ENSURE(root_expr->children_.size() == 2, "expression should have two children");
    GetAllColumnValueExpressions(root_expr->children_.front(), result);
    GetAllColumnValueExpressions(root_expr->children_.back(), result);
  }
  if (auto cv_expr = dynamic_cast<ColumnValueExpression *>(root_expr.get()); cv_expr != nullptr) {
    result.emplace_back(root_expr);
  }
}

auto Optimizer::SimplifyAggregationBelowProjection(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  /**
   * Algorithm:
   *
   * scan the projection plan node to know which output columns of the child agg plan node are needed.
   * record the column value expressions of the projection node.
   * reconstruct the aggregation plan node according to the necessity.
   * rewrite the projection plan node expressions.
   */
  auto children = plan->children_;
  for (auto &child : children) {
    child = SimplifyAggregationBelowProjection(child);
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (auto this_proj_plan = dynamic_cast<ProjectionPlanNode *>(optimized_plan.get()); this_proj_plan != nullptr) {
    if (auto child_agg_plan = dynamic_cast<const AggregationPlanNode *>(optimized_plan->children_.front().get());
        child_agg_plan != nullptr) {
      std::vector<bool> needed_child_agg_plan_agg_indices(child_agg_plan->GetAggregates().size(), false);
      std::unordered_map<uint32_t, std::vector<AbstractExpressionRef>> old_agg_indices_cv_expr_mapping;
      for (auto &expr : this_proj_plan->GetExpressions()) {
        std::vector<AbstractExpressionRef> all_cv_expressions;
        GetAllColumnValueExpressions(expr, all_cv_expressions);
        for (auto &this_expr : all_cv_expressions) {
          auto &cv_expr = dynamic_cast<ColumnValueExpression &>(*this_expr);
          auto col_idx = cv_expr.GetColIdx();
          if (col_idx >= child_agg_plan->group_bys_.size()) {
            needed_child_agg_plan_agg_indices.at(col_idx - child_agg_plan->group_bys_.size()) = true;
            old_agg_indices_cv_expr_mapping[cv_expr.GetColIdx()].emplace_back(this_expr);
          }
        }
      }
      std::vector<AbstractExpressionRef> optimized_aggregates;
      std::vector<AggregationType> optimized_aggregate_types;
      std::vector<Column> optimized_aggregate_output_columns;
      for (size_t gi = 0; gi < child_agg_plan->group_bys_.size(); ++gi) {
        optimized_aggregate_output_columns.emplace_back(child_agg_plan->output_schema_->GetColumn(gi));
      }
      for (size_t i = 0; i < child_agg_plan->aggregates_.size(); ++i) {
        if (needed_child_agg_plan_agg_indices[i]) {
          optimized_aggregates.emplace_back(child_agg_plan->aggregates_.at(i));
          optimized_aggregate_types.emplace_back(child_agg_plan->agg_types_.at(i));
          optimized_aggregate_output_columns.emplace_back(
              child_agg_plan->output_schema_->GetColumn(i + child_agg_plan->group_bys_.size()));
          for (auto &expr : old_agg_indices_cv_expr_mapping[i]) {
            auto &cv_expr = dynamic_cast<ColumnValueExpression &>(*expr);
            expr = std::make_shared<ColumnValueExpression>(
                cv_expr.GetTupleIdx(), child_agg_plan->group_bys_.size() + optimized_aggregates.size() - 1,
                cv_expr.GetReturnType());
          }
        }
      }
      auto optimized_child_agg_plan = std::make_shared<AggregationPlanNode>(
          std::make_unique<Schema>(optimized_aggregate_output_columns), child_agg_plan->children_.front(),
          child_agg_plan->group_bys_, optimized_aggregates, optimized_aggregate_types);
      optimized_plan = std::make_unique<ProjectionPlanNode>(optimized_plan->output_schema_,
                                                            this_proj_plan->expressions_, optimized_child_agg_plan);
    }
  }
  return optimized_plan;
}

auto Optimizer::OptimizeExpressionElimination(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // Optimization for Q3: Column Pruning
  // For common expression elimination, necessary computation, transform always false expression to dummy scan
  auto children = plan->children_;
  for (auto &child_plan : children) {
    child_plan = OptimizeExpressionElimination(child_plan);
  }
  AbstractPlanNodeRef optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Projection) {
    if (auto agg_plan = optimized_plan->children_.front();
        optimized_plan->children_.front()->GetType() == PlanType::Aggregation) {
      auto [new_agg_plan, agg_old_new_columns_mapping] = SimplifyProjectionAggregation(agg_plan);
      const auto &proj_plan = dynamic_cast<const ProjectionPlanNode &>(*optimized_plan);
      auto expressions = proj_plan.expressions_;
      for (auto &proj_expr : expressions) {
        ChangeProjectionExpressionColumns(proj_expr, agg_old_new_columns_mapping);
      }
      optimized_plan =
          std::make_shared<ProjectionPlanNode>(proj_plan.output_schema_, std::move(expressions), new_agg_plan);
    }
  }
  return optimized_plan;
}

auto Optimizer::SimplifyProjectionAggregation(AbstractPlanNodeRef &agg_plan)
    -> std::pair<AbstractPlanNodeRef, std::vector<size_t>> {
  const auto &aggregation_plan = dynamic_cast<const AggregationPlanNode &>(*agg_plan);
  const auto aggregations_count = aggregation_plan.aggregates_.size();
  const auto group_by_count = aggregation_plan.group_bys_.size();
  std::unordered_map<std::string, size_t> new_aggregation_to_new_column_idx_mapping;
  std::vector<AbstractExpressionRef> new_aggregates;
  std::vector<AggregationType> new_types;
  std::vector<size_t> column_old_to_new_mapping;
  std::vector<Column> new_schema_columns;
  for (size_t g = 0; g < group_by_count; ++g) {
    column_old_to_new_mapping.emplace_back(g);
    new_schema_columns.emplace_back(aggregation_plan.output_schema_->GetColumns().at(g));
  }
  for (size_t i = 0; i < aggregations_count; ++i) {
    auto &this_column_value = aggregation_plan.aggregates_[i];
    auto &this_type = aggregation_plan.agg_types_[i];
    auto this_record = fmt::format("{}{}", this_type, this_column_value);
    if (new_aggregation_to_new_column_idx_mapping.count(this_record) == 0) {
      new_aggregation_to_new_column_idx_mapping.emplace(this_record, new_aggregates.size() + group_by_count);
      new_aggregates.emplace_back(this_column_value);
      new_types.emplace_back(this_type);
      new_schema_columns.emplace_back(aggregation_plan.output_schema_->GetColumns().at(i + group_by_count));
    }
    column_old_to_new_mapping.emplace_back(new_aggregation_to_new_column_idx_mapping[this_record]);
  }
  auto new_schema = std::make_shared<Schema>(std::move(new_schema_columns));
  return {std::make_shared<AggregationPlanNode>(new_schema, aggregation_plan.children_[0], aggregation_plan.group_bys_,
                                                new_aggregates, new_types),
          column_old_to_new_mapping};
}

void Optimizer::ChangeProjectionExpressionColumns(AbstractExpressionRef &expression,
                                                  const std::vector<size_t> &mapping) {
  if (auto *cv_expr = dynamic_cast<ColumnValueExpression *>(expression.get()); cv_expr != nullptr) {
    BUSTUB_ENSURE(cv_expr->GetColIdx() < mapping.size(), "unexpected mapping");
    expression = std::make_shared<ColumnValueExpression>(cv_expr->GetTupleIdx(), mapping[cv_expr->GetColIdx()],
                                                         cv_expr->GetReturnType());
    return;
  }
  for (auto &expr : expression->children_) {
    ChangeProjectionExpressionColumns(expr, mapping);
  }
}

auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto p = plan;
  p = OptimizeMergeProjection(p);
  p = OptimizeAlwaysFalseExpressionToDummyScan(p);
  p = OptimizeExpressionElimination(p);
  p = OptimizeMergeEqualFilterNLJ(p);
  p = OptimizeMergeFilterNLJ(p);
  p = OptimizeReorderingJoin(p);
  p = OptimizeNLJAsIndexJoin(p);
  p = OptimizeNLJAsHashJoin(p);
  p = OptimizeOrderByAsIndexScan(p);
  p = OptimizeSortLimitAsTopN(p);
  p = OptimizeRemoveUnnecessaryComputation(p);
  return p;
}

}  // namespace bustub
