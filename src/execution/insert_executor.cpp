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
    : AbstractExecutor(exec_ctx) {
  child_executor_ = std::move(child_executor);
  plan_ = plan;
}

void InsertExecutor::Init() {
  auto table_oid = plan_->TableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid);
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (inserted_) {
    *tuple = Tuple();
    return false;
  }
  int count = 0;
  Tuple next_tuple;
  RID next_rid;
  // Schema next_schema(std::vector<Column>{Column("data", INTEGER)});
  // *tuple = Tuple(std::vector<Value>{Value(INTEGER, count)}, &schema);
  while (child_executor_->Next(&next_tuple, &next_rid)) {
    table_info_->table_->InsertTuple(next_tuple, &next_rid, exec_ctx_->GetTransaction());
    for (auto &index : indexes_) {
      auto i = index->index_->GetKeyAttrs()[0];
      auto v = next_tuple.GetValue(&table_info_->schema_, i);
      Tuple t(std::vector<Value>{v}, index->index_->GetKeySchema());
      index->index_->InsertEntry(t, next_rid, exec_ctx_->GetTransaction());
    }
    ++count;
  }
  Schema schema(std::vector<Column>{Column("num", INTEGER)});
  *tuple = Tuple(std::vector<Value>{Value(INTEGER, count)}, &schema);
  inserted_ = true;
  return true;
}

}  // namespace bustub
