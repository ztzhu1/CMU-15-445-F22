//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  auto catalog = exec_ctx->GetCatalog();
  auto table_oid = plan->TableOid();
  table_info_ = catalog->GetTable(table_oid);
  indexes_ = catalog->GetTableIndexes(table_info_->name_);
  child_executor_ = std::move(child_executor);
}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (deleted) {
    return false;
  }
  Tuple child_tuple;
  RID child_rid;
  auto txn = exec_ctx_->GetTransaction();
  Schema *table_schema = &table_info_->schema_;
  int cnt = 0;

  while (child_executor_->Next(&child_tuple, &child_rid)) {
    auto success = table_info_->table_->MarkDelete(child_rid, txn);
    if (success) {
      ++cnt;
    }
    for (auto index : indexes_) {
      auto col = index->index_->GetKeyAttrs()[0];
      auto index_schema = index->index_->GetKeySchema();
      auto t = Tuple{std::vector{child_tuple.GetValue(table_schema, col)}, index_schema};
      index->index_->DeleteEntry(t, child_rid, txn);
    }
  }

  Schema schema(std::vector{Column{"count", INTEGER}});
  *tuple = Tuple{std::vector{Value(INTEGER, cnt)}, &schema};

  deleted = true;
  return true;
}

}  // namespace bustub
