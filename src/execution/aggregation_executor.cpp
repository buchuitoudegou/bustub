//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)), aht_(plan->GetAggregates(), plan->GetAggregateTypes()),aht_iterator_(aht_.Begin()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

const Schema *AggregationExecutor::GetOutputSchema() { return plan_->OutputSchema(); }

void AggregationExecutor::Init() {
    if (is_init) {
        return;
    }
    auto child_schema = child_->GetOutputSchema();
    switch (plan_->GetChildPlan()->GetType()) {
        case PlanType::SeqScan: {
            SeqScanExecutor* scan_executor = dynamic_cast<SeqScanExecutor*>(child_.get());
            scan_executor->Init();
            Tuple child_tuple;
            // 1. get one tuple
            // 2. convert it into AggregateKey by group_bys exprs (elements in group_bys_ corresponding to expr in exprs)
            // 3. convert it into AggregateValue by aggregates exprs (elements in aggregates_ vector corresponding to expr in exprs)
            // 4. insert AggregateKey and AggregateValue into aht_
            while (scan_executor->Next(&child_tuple)) {
                AggregateKey agg_key;
                for (auto group_by_expr: plan_->GetGroupBys()) {
                    Value val_of_key = group_by_expr->Evaluate(&child_tuple, child_schema);
                    agg_key.group_bys_.push_back(val_of_key);
                }
                AggregateValue agg_val;
                for (auto agg_expr: plan_->GetAggregates()) {
                    Value val_of_agg = agg_expr->Evaluate(&child_tuple, child_schema);
                    agg_val.aggregates_.push_back(val_of_agg);
                }
                aht_.InsertCombine(agg_key, agg_val);
            }
            break;
        }
        case PlanType::HashJoin: {
            HashJoinExecutor* hash_executor = dynamic_cast<HashJoinExecutor*>(child_.get());
            hash_executor->Init();
            Tuple child_tuple;
            while (hash_executor->Next(&child_tuple)) {
                AggregateKey agg_key;
                for (auto group_by_expr: plan_->GetGroupBys()) {
                    Value val_of_key = group_by_expr->Evaluate(&child_tuple, child_schema);
                    agg_key.group_bys_.push_back(val_of_key);
                }
                AggregateValue agg_val;
                for (auto agg_expr: plan_->GetAggregates()) {
                    Value val_of_agg = agg_expr->Evaluate(&child_tuple, child_schema);
                    agg_val.aggregates_.push_back(val_of_agg);
                }
                aht_.InsertCombine(agg_key, agg_val);
            }
            break;
        }
        default: break;
    }
    aht_iterator_ = aht_.Begin();
    is_init = true;
}

bool AggregationExecutor::Next(Tuple *tuple) {
    if (aht_iterator_ == aht_.End()) {
        return false;
    }
    auto out_schema = GetOutputSchema();
    AggregateValue cur_val = aht_iterator_.Val();
    AggregateKey cur_key = aht_iterator_.Key();
    bool is_success = true;
    if (plan_->GetHaving()) {
        is_success = plan_->GetHaving()->EvaluateAggregate(cur_key.group_bys_, cur_val.aggregates_).GetAs<bool>();
    }
    if (is_success) {
        std::vector<Value> raw_values;
        for (size_t i = 0; i <  out_schema->GetColumns().size(); ++i) {
            auto col = out_schema->GetColumns()[i];
            auto agg_expr = col.GetExpr();
            raw_values.push_back(agg_expr->EvaluateAggregate(cur_key.group_bys_, cur_val.aggregates_));
        }
        *tuple = Tuple(raw_values, GetOutputSchema());
        ++ aht_iterator_;
        return true;
    } else {
        ++ aht_iterator_;
        return Next(tuple);
    }
}

}  // namespace bustub
