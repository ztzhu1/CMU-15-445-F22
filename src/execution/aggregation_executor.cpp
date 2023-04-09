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
      aht_iterator_(aht_.End()) {}

void AggregationExecutor::Init() {
  child_->Init();
  if (GetChildExecutor() != nullptr) {
    Tuple tuple;
    RID rid;
    while (child_->Next(&tuple, &rid)) {
      AggregateKey key = MakeAggregateKey(&tuple);
      AggregateValue value = MakeAggregateValue(&tuple);
      aht_.InsertCombine(key, value);
      empty_ = false;
    }
  }
  aht_iterator_ = static_cast<SimpleAggregationHashTable::Iterator>(aht_.Begin());
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    if (empty_) {
      empty_ = false;
      if (plan_->GetGroupBys().empty()) {
        std::vector<Value> values;
        for (const auto &v : aht_.GenerateInitialAggregateValue().aggregates_) {
          values.push_back(v);
        }
        *tuple = Tuple(values, &plan_->OutputSchema());
        *rid = tuple->GetRid();
        return true;
      }
    }
    return false;
  }
  std::vector<Value> values;
  for (const auto &v : aht_iterator_.Key().group_bys_) {
    values.push_back(v);
  }
  for (const auto &v : aht_iterator_.Val().aggregates_) {
    values.push_back(v);
  }
  *tuple = Tuple(values, &plan_->OutputSchema());
  *rid = tuple->GetRid();
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
