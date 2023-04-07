//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
  plan_ = plan;
}

void SeqScanExecutor::Init() {
  auto table_oid = plan_->GetTableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid);
  it_ = std::make_unique<TableIterator>(table_info_->table_->Begin(exec_ctx_->GetTransaction()));
  end_ = std::make_unique<TableIterator>(table_info_->table_->End());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (*it_ == *end_) {
    return false;
  }
  *tuple = **it_;
  ++(*it_);

  return true;
}

}  // namespace bustub
