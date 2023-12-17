//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace std {

/** Implements std::hash on Value */
template <>
struct hash<bustub::Value> {
  auto operator()(const bustub::Value &val) const -> std::size_t {
    size_t curr_hash = 0;
    {
      if (!val.IsNull()) {
        curr_hash = bustub::HashUtil::HashValue(&val);
      }
    }
    return curr_hash;
  }
};
}  // namespace std

namespace bustub {

auto operator==(const bustub::Value &x, const bustub::Value &y) -> bool;
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side
   * of join
   * @param right_child The child executor that produces tuples for the right
   * side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more
   * tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

  void Build();

 private:
  class HashTable {
   public:
    HashTable() = default;

    ~HashTable() = default;

    void Insert(const Value &value, const Tuple &tuple, RID rid) {
      if (table_indices_.count(value) == 0) {
        table_indices_[value] = 0;
      }
      table_[value].emplace_back(tuple, rid);
    }

    auto Find(const Value &value, Tuple &tuple, RID &rid) -> bool {
      if (table_indices_.count(value) > 0) {
        auto &index = table_indices_[value];
        if (index >= table_[value].size()) {
          return false;
        }
        tuple = table_[value][index].first;
        rid = table_[value][index].second;
        ++index;
        return true;
      }
      return false;
    }

    auto Contain(const Value &value) -> bool { return table_indices_.count(value) > 0; }

    void Reset(const Value &value) { table_indices_[value] = 0; }

   private:
    std::unordered_map<Value, std::vector<std::pair<Tuple, RID>>> table_;
    std::unordered_map<Value, size_t> table_indices_;
  };

  void AddTupleValuesTo(std::vector<Value> &values, bustub::Tuple *tuple, const bustub::Schema &schema);

  bool done_;
  bool result_generated_;
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;
  HashTable hash_table_;

  Tuple current_left_tuple_;
  RID current_left_rid_;
};

}  // namespace bustub
