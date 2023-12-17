#pragma once

#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "concurrency/transaction.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"

#define BUSTUB_OPTIMIZER_HACK_REMOVE_AFTER_2022_FALL

namespace bustub {

/**
 * The optimizer takes an `AbstractPlanNode` and outputs an optimized
 * `AbstractPlanNode`.
 */
class Optimizer {
 public:
  explicit Optimizer(const Catalog &catalog, bool force_starter_rule)
      : catalog_(catalog), force_starter_rule_(force_starter_rule) {}

  auto Optimize(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

 private:
  /**
   * @brief merge projections that do identical project.
   * Identical projection might be produced when there's `SELECT *`,
   * aggregation, or when we need to rename the columns in the planner. We merge
   * these projections so as to make execution faster.
   */
  auto OptimizeMergeProjection(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief merge filter condition into nested loop join.
   * In planner, we plan cross join + filter with cross product (done with
   * nested loop join) and a filter plan node. We can merge the filter condition
   * into nested loop join to achieve better efficiency.
   */
  auto OptimizeMergeFilterNLJ(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief optimize nested loop join into hash join.
   * In the starter code, we will check NLJs with exactly one equal condition.
   * You can further support optimizing joins with multiple eq conditions.
   */
  auto OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief optimize filter with nlj into filter with hash join
   */
  auto OptimizeMergeEqualFilterNLJ(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto ExtractEqiExpressionHelper(bool &extracted, AbstractExpressionRef &eqi_expression,
                                  AbstractExpressionRef &expression) -> int32_t;
  /**
   * @brief Extract the eqi expression from `expression`.
   *
   * @param expression The expression from which the equ expression will be extracted.
   * @return True with the extracted eqi expression if found one eqi expression.Otherwise, false.
   */
  auto ExtractEqiExpression(AbstractExpressionRef &expression) -> std::pair<bool, AbstractExpressionRef>;

  [[maybe_unused]] auto OptimizeReorderingJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  [[maybe_unused]] void ResetNLJChildren(AbstractPlanNode &plan, const AbstractPlanNodeRef &left, uint32_t left_key_idx,
                                         TypeId left_return_type, const AbstractPlanNodeRef &right,
                                         uint32_t right_key_idx, TypeId right_return_type);

  [[maybe_unused]] auto GetScanNodeTableName(const AbstractPlanNode &scan_plan) -> std::string;

  /**
   * @brief prunes some unnecessary computation involving some columns.
   */
  auto OptimizeExpressionElimination(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto SimplifyProjectionAggregation(AbstractPlanNodeRef &agg_plan)
      -> std::pair<AbstractPlanNodeRef, std::vector<size_t>>;

  void ChangeProjectionExpressionColumns(AbstractExpressionRef &expression, const std::vector<size_t> &mapping);

  auto OptimizeRemoveUnnecessaryComputation(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief Merge two projections if the upper one does not need all the output columns of the downer one.
   * In this case:
   *  Projection [#0.1 #0.3]
   *  Projection [#0.0, #0.1, #0.2,#0.3]
   *  Merged projection: [#0.0, #0.1]
   */
  auto MergeTwoProjections(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief Get all column values expressions in an expression.
   */
  void GetAllColumnValueExpressions(const AbstractExpressionRef &root_expr, std::vector<AbstractExpressionRef> &result);

  auto SimplifyAggregationBelowProjection(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizeAlwaysFalseExpressionToDummyScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief optimize nested loop join into index join.
   */
  auto OptimizeNLJAsIndexJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief eliminate always true filter
   */
  auto OptimizeEliminateTrueFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief merge filter into filter_predicate of seq scan plan node
   */
  auto OptimizeMergeFilterScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief rewrite expression to be used in nested loop joins. e.g., if we have
   * `SELECT * FROM a, b WHERE a.x = b.y`, we will have `#0.x = #0.y` in the
   * filter plan node. We will need to figure out where does `0.x` and `0.y`
   * belong in NLJ (left table or right table?), and rewrite it as `#0.x =
   * #1.y`.
   *
   * @param expr the filter expression
   * @param left_column_cnt number of columns in the left size of the NLJ
   * @param right_column_cnt number of columns in the left size of the NLJ
   */
  auto RewriteExpressionForJoin(const AbstractExpressionRef &expr, size_t left_column_cnt, size_t right_column_cnt)
      -> AbstractExpressionRef;

  /** @brief check if the predicate is true::boolean */
  auto IsPredicateTrue(const AbstractExpression &expr) -> bool;

  /**
   * @brief optimize order by as index scan if there's an index on a table
   */
  auto OptimizeOrderByAsIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /** @brief check if the index can be matched */
  auto MatchIndex(const std::string &table_name, uint32_t index_key_idx)
      -> std::optional<std::tuple<index_oid_t, std::string>>;

  /**
   * @brief optimize sort + limit as top N
   */
  auto OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief get the estimated cardinality for a table based on the table name.
   * Useful when join reordering. BusTub doesn't support statistics for now, so
   * it's the only way for you to get the table size :(
   *
   * @param table_name
   * @return std::optional<size_t>
   */
  auto EstimatedCardinality(const std::string &table_name) -> std::optional<size_t>;

  /** Catalog will be used during the planning process. USERS SHOULD ENSURE IT
   * OUTLIVES OPTIMIZER, otherwise it's a dangling reference.
   */
  const Catalog &catalog_;

  const bool force_starter_rule_;
};

}  // namespace bustub
