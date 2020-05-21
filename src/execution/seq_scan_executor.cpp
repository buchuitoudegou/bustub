//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  if (!is_init_) {
    SimpleCatalog* catalog = exec_ctx_->GetCatalog();
    table_oid_t table_oid = plan_->GetTableOid();
    table_iter = std::make_unique<TableIterator>(catalog->GetTable(table_oid)->table_->Begin(exec_ctx_->GetTransaction()));
    is_init_ = true;
  }
}

bool SeqScanExecutor::Next(Tuple *tuple) {
  bool get_val = false;
  SimpleCatalog* catalog = exec_ctx_->GetCatalog();
  const AbstractExpression* predicate = plan_->GetPredicate();
  while (!get_val) {
    if (*(table_iter.get()) == catalog->GetTable(plan_->GetTableOid())->table_->End()) {
      break;
    }
    catalog->GetTable(plan_->GetTableOid())->table_->GetTuple((*(table_iter.get()))->GetRid(), tuple, exec_ctx_->GetTransaction());
    (*(table_iter.get())) ++;
    if (predicate && predicate->Evaluate(tuple, GetOutputSchema()).GetAs<bool>()) {
      get_val = true;
    } else if (!predicate) {
      get_val = true;
    }
  }
  return get_val;
}

}  // namespace bustub
