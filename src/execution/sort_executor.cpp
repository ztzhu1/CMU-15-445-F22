#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.push_back(tuple);
  }
  auto order_bys = plan_->GetOrderBy();
  std::sort(tuples_.begin(), tuples_.end(), Comparator(&order_bys, &plan_->OutputSchema()));
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_ >= tuples_.size()) {
    return false;
  }
  *tuple = tuples_[index_++];
  *rid = tuple->GetRid();
  return true;
}

}  // namespace bustub
