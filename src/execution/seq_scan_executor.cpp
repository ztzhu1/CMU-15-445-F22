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

#include <fmt/color.h>

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  auto catalog = exec_ctx->GetCatalog();
  auto table_oid = plan->GetTableOid();
  auto table_info = catalog->GetTable(table_oid);
  table_info_ = table_info;
}

void SeqScanExecutor::Init() {
  auto txn = exec_ctx_->GetTransaction();
  table_it_ = std::make_unique<TableIterator>(table_info_->table_->Begin(txn));
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (*table_it_ == table_info_->table_->End()) {
    return false;
  }
  *tuple = **table_it_;
  *rid = tuple->GetRid();
  ++(*table_it_);
  return true;
}
}  // namespace bustub
