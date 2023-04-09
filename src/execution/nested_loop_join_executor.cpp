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
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple;
  RID rid;
  while (left_executor_->Next(&tuple, &rid)) {
    left_tuples_.push_back(tuple);
  }
  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples_.push_back(tuple);
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const auto &predicate = plan_->Predicate();
  const auto &left_s = left_executor_->GetOutputSchema();
  const auto &right_s = right_executor_->GetOutputSchema();
  if (right_tuples_.empty() && left_index_ < left_tuples_.size()) {
    std::vector<Value> values;
    const auto &left_tuple = left_tuples_[left_index_];
    for (uint32_t i = 0; i < left_s.GetColumnCount(); ++i) {
      values.push_back(left_tuple.GetValue(&left_s, i));
    }
    for (uint32_t i = 0; i < right_s.GetColumnCount(); ++i) {
      values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
    }
    *tuple = Tuple(values, &plan_->OutputSchema());
    ++left_index_;
    return true;
  }

  while (left_index_ < left_tuples_.size() && right_index_ < right_tuples_.size()) {
    bool last = false;
    const auto &left_tuple = left_tuples_[left_index_];
    const auto &right_tuple = right_tuples_[right_index_];
    auto v = predicate.EvaluateJoin(&left_tuple, left_s, &right_tuple, right_s);
    bool success = v.IsNull() || v.GetAs<bool>();
    if (++right_index_ == right_tuples_.size()) {
      ++left_index_;
      right_index_ = 0;
      if (!success && !returned_) {
        need_return_null_ = true;
      }
      returned_ = false;
      last = true;
    }
    if (success) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_s.GetColumnCount(); ++i) {
        values.push_back(left_tuple.GetValue(&left_s, i));
      }
      for (uint32_t i = 0; i < right_s.GetColumnCount(); ++i) {
        values.push_back(right_tuple.GetValue(&right_s, i));
      }
      *tuple = Tuple(values, &plan_->OutputSchema());
      need_return_null_ = false;
      if (!last) {
        returned_ = true;
      }
      return true;
    }
    if (plan_->GetJoinType() == JoinType::LEFT && need_return_null_) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_s.GetColumnCount(); ++i) {
        values.push_back(left_tuple.GetValue(&left_s, i));
      }
      for (uint32_t i = 0; i < right_s.GetColumnCount(); ++i) {
        values.push_back(ValueFactory::GetNullValueByType(right_tuple.GetValue(&right_s, i).GetTypeId()));
      }
      *tuple = Tuple(values, &plan_->OutputSchema());
      need_return_null_ = false;
      return true;
    }
    need_return_null_ = false;
  }
  return false;
}

}  // namespace bustub
