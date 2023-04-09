//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    left_tuples_.push_back(tuple);
  }
  auto oid = plan_->GetInnerTableOid();
  auto *inner_table_info = exec_ctx_->GetCatalog()->GetTable(oid);
  auto *index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  std::vector<RID> result;
  for (size_t i = 0; i < left_tuples_.size(); ++i) {
    auto v = plan_->KeyPredicate()->Evaluate(&left_tuples_[i], child_executor_->GetOutputSchema());
    Schema s({Column("key", v.GetTypeId())});
    index_info->index_->ScanKey(Tuple({v}, &s), &result, exec_ctx_->GetTransaction());
    Tuple t;
    if (!result.empty()) {
      inner_table_info->table_->GetTuple(result[0], &t, exec_ctx_->GetTransaction());
      right_tuples_[i] = t;
      result.clear();
    }
  }
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (index_ < left_tuples_.size()) {
    const auto &left_s = child_executor_->GetOutputSchema();
    const auto oid = plan_->GetInnerTableOid();
    const auto &right_s = exec_ctx_->GetCatalog()->GetTable(oid)->schema_;
    const auto &left_tuple = left_tuples_[index_];
    auto it = right_tuples_.find(index_);
    if (it != right_tuples_.end()) {
      const auto &right_tuple = it->second;
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_s.GetColumnCount(); ++i) {
        values.push_back(left_tuple.GetValue(&left_s, i));
      }
      for (uint32_t i = 0; i < right_s.GetColumnCount(); ++i) {
        values.push_back(right_tuple.GetValue(&right_s, i));
      }
      *tuple = Tuple(values, &plan_->OutputSchema());
      ++index_;
      return true;
    }
    if (plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      auto *inner_table_info = exec_ctx_->GetCatalog()->GetTable(oid);
      for (uint32_t i = 0; i < left_s.GetColumnCount(); ++i) {
        values.push_back(left_tuple.GetValue(&left_s, i));
      }
      for (uint32_t i = 0; i < right_s.GetColumnCount(); ++i) {
        values.push_back(ValueFactory::GetNullValueByType(inner_table_info->schema_.GetColumn(i).GetType()));
      }
      *tuple = Tuple(values, &plan_->OutputSchema());
      ++index_;
      return true;
    }
    ++index_;
  }
  return false;
}

}  // namespace bustub
