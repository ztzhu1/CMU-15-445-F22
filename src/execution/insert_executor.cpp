//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  auto catalog = exec_ctx->GetCatalog();
  auto table_oid = plan->TableOid();
  table_info_ = catalog->GetTable(table_oid);
  indexes_ = catalog->GetTableIndexes(table_info_->name_);
  child_executor_ = std::move(child_executor);
}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (inserted) {
    return false;
  }
  Tuple child_tuple;
  RID child_rid;
  auto txn = exec_ctx_->GetTransaction();
  int cnt = 0;

  while (child_executor_->Next(&child_tuple, &child_rid)) {
    auto success = table_info_->table_->InsertTuple(child_tuple, &child_rid, txn);
    if (success) {
      ++cnt;
    }
    for (auto index : indexes_) {
      index->index_->InsertEntry(child_tuple, child_rid, txn);
    }
  }

  Schema schema(std::vector{Column{"count", INTEGER}});
  *tuple = Tuple{std::vector{Value(INTEGER, cnt)}, &schema};

  inserted = true;
  return true;
}

}  // namespace bustub
