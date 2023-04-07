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
    : AbstractExecutor(exec_ctx) {
  child_executor_ = std::move(child_executor);
  plan_ = plan;
}

void DeleteExecutor::Init() {
  auto table_oid = plan_->TableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid);
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (deleted_) {
    *tuple = Tuple();
    return false;
  }
  int count = 0;
  Tuple next_tuple;
  RID next_rid;
  while (child_executor_->Next(&next_tuple, &next_rid)) {
    table_info_->table_->MarkDelete(next_tuple.GetRid(), exec_ctx_->GetTransaction());
    for (auto &index : indexes_) {
      auto i = index->index_->GetKeyAttrs()[0];
      auto v = next_tuple.GetValue(&table_info_->schema_, i);
      Tuple t(std::vector<Value>{v}, index->index_->GetKeySchema());
      index->index_->DeleteEntry(t, next_rid, exec_ctx_->GetTransaction());
    }
    ++count;
  }
  Schema schema(std::vector<Column>{Column("num", INTEGER)});
  *tuple = Tuple(std::vector<Value>{Value(INTEGER, count)}, &schema);
  deleted_ = true;
  return true;
}

}  // namespace bustub
