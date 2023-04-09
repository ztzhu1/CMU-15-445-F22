//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The topn plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the topn */
  void Init() override;

  /**
   * Yield the next tuple from the topn.
   * @param[out] tuple The next tuple produced by the topn
   * @param[out] rid The next tuple RID produced by the topn
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the topn */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  struct TupleWrapper {
    TupleWrapper(Tuple &tuple, std::vector<std::pair<OrderByType, AbstractExpressionRef>> *order_bys,
                 const Schema *schema)
        : inner_(tuple), order_bys_(order_bys), schema_(schema) {}
    friend auto operator<(const TupleWrapper &a, const TupleWrapper &b) -> bool {
      for (const auto &order_by : *a.order_bys_) {
        auto &expr = order_by.second;
        auto result = expr->Evaluate(&a.inner_, *a.schema_).CompareEquals(expr->Evaluate(&b.inner_, *b.schema_));
        if (result == CmpBool::CmpFalse) {
          if (order_by.first == OrderByType::DESC) {
            return static_cast<bool>(
                expr->Evaluate(&a.inner_, *b.schema_).CompareGreaterThan(expr->Evaluate(&b.inner_, *b.schema_)));
          }
          return static_cast<bool>(
              expr->Evaluate(&a.inner_, *b.schema_).CompareLessThan(expr->Evaluate(&b.inner_, *b.schema_)));
        }
      }
      return true;
    }
    Tuple inner_;
    std::vector<std::pair<OrderByType, AbstractExpressionRef>> *order_bys_{};
    const Schema *schema_;
  };
  /** The topn plan node to be executed */
  const TopNPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::set<TupleWrapper> tuples_{};
};
}  // namespace bustub
