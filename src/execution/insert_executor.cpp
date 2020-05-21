//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child(std::move(child_executor)) {}

const Schema *InsertExecutor::GetOutputSchema() { return plan_->OutputSchema(); }

void InsertExecutor::Init() {
    is_init_ = true;
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple) {
    bool is_success = true;
    Schema* schema = &exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->schema_;
    SimpleCatalog* catalog = exec_ctx_->GetCatalog();
    Tuple insert_tuple;
    RID rid;
    if (plan_->IsRawInsert()) {
        auto all_vecs = plan_->RawValues();
        for (size_t i = 0; i < all_vecs.size(); ++i) {
            insert_tuple = Tuple(all_vecs[i], schema);
            bool result = catalog->GetTable(plan_->TableOid())->table_->InsertTuple(insert_tuple, &rid, exec_ctx_->GetTransaction());
            if (!result) {
                is_success = false;
            }
        }
        return is_success;
    } else {
        const AbstractPlanNode* child_plan = plan_->GetChildPlan();
        switch (child_plan->GetType()) {
            case PlanType::SeqScan: {
                SeqScanExecutor* scan_executor1_ptr = dynamic_cast<SeqScanExecutor*>(child.get());
                child.release();
                std::unique_ptr<SeqScanExecutor> scan_executor1(scan_executor1_ptr);
                scan_executor1->Init();
                while (scan_executor1->Next(&insert_tuple)) {
                    bool result = catalog->GetTable(plan_->TableOid())->table_->InsertTuple(insert_tuple, &rid, exec_ctx_->GetTransaction());
                    if (!result) {
                        is_success = false;
                    }
                }
                break;                
            }
            case PlanType::HashJoin: {
                HashJoinExecutor* join_executor1_ptr = dynamic_cast<HashJoinExecutor*>(child.get());
                child.release();
                std::unique_ptr<HashJoinExecutor> join_executor1(join_executor1_ptr);
                join_executor1->Init();
                while (join_executor1->Next(&insert_tuple)) {
                    bool result = catalog->GetTable(plan_->TableOid())->table_->InsertTuple(insert_tuple, &rid, exec_ctx_->GetTransaction());
                    if (!result) {
                        is_success = false;
                    }
                }
                break; 
            }
            case PlanType::Aggregation: {
                AggregationExecutor* aggr_executor1_ptr = dynamic_cast<AggregationExecutor*>(child.get());
                child.release();
                std::unique_ptr<AggregationExecutor> aggr_executor1(aggr_executor1_ptr);
                aggr_executor1->Init();
                while (aggr_executor1->Next(&insert_tuple)) {
                    bool result = catalog->GetTable(plan_->TableOid())->table_->InsertTuple(insert_tuple, &rid, exec_ctx_->GetTransaction());
                    if (!result) {
                        is_success = false;
                    }
                }
                break; 
            }
            default: return false;
        }
    }
    return is_success;
}

}  // namespace bustub
