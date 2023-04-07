//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
}

void IndexScanExecutor::Init() {
  auto index_oid = plan_->GetIndexOid();
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(index_oid);
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
  it_ = std::make_unique<BPlusTreeIndexIteratorForOneIntegerColumn>(tree_->GetBeginIterator());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_->IsEnd()) {
    return false;
  }
  RID id = (**it_).second;
  table_info_->table_->GetTuple(id, tuple, exec_ctx_->GetTransaction());
  ++(*it_);
  return true;
}
}  // namespace bustub
