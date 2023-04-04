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
    : AbstractExecutor(exec_ctx), plan_(plan) {
  auto catalog = exec_ctx->GetCatalog();
  auto index_oid = plan->GetIndexOid();
  index_info_ = catalog->GetIndex(index_oid);
  table_info_ = catalog->GetTable(index_info_->table_name_);
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
}

void IndexScanExecutor::Init() { it_ = std::make_unique<ITERATOR_TYPE>(tree_->GetBeginIterator()); }

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_->IsEnd()) {
    return false;
  }

  *rid = (**it_).second;
  printf("----%d\n", rid->GetPageId());
  assert(table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction()));
  ++(*it_);

  return true;
}

}  // namespace bustub
