#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  auto order_bys = plan_->GetOrderBy();
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.emplace(tuple, &order_bys, &plan_->OutputSchema());
    if (tuples_.size() > plan_->GetN()) {
      tuples_.erase(std::prev(tuples_.end()));
    }
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tuples_.empty()) {
    return false;
  }
  *tuple = tuples_.begin()->inner_;
  *rid = tuple->GetRid();
  tuples_.erase(tuples_.begin());
  return true;
}

}  // namespace bustub
