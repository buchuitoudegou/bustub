//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left, std::unique_ptr<AbstractExecutor> &&right)
    : AbstractExecutor(exec_ctx), plan_(plan), jht_(std::string("simple_hash_join"), exec_ctx_->GetBufferPoolManager(), jht_comp_, jht_num_buckets_, jht_hash_fn_)
    , left(std::move(left)), right(std::move(right)) {}

/** @return the JHT in use. Do not modify this function, otherwise you will get a zero. */
// const HT *GetJHT() const { return &jht_; }

void HashJoinExecutor::Init() {
    if (is_init_) {
        return;
    }
    switch (plan_->GetLeftPlan()->GetType()) {
        case PlanType::SeqScan: {
            SeqScanExecutor* left_child_ptr = dynamic_cast<SeqScanExecutor*>(left.get());
            left.release();
            std::unique_ptr<SeqScanExecutor> left_child(left_child_ptr);
            Tuple tuple;
            auto left_keys = plan_->GetLeftKeys();
            auto left_schema =  plan_->GetLeftPlan()->OutputSchema();
            left_child->Init();
            while (left_child->Next(&tuple)) {
                const Tuple* cur_tuple = &tuple;
                hash_t hash_val = HashValues(cur_tuple, left_schema, left_keys);
                jht_.Insert(exec_ctx_->GetTransaction(), hash_val, tuple);
            }
            break;
        }
        case PlanType::Aggregation: {
            AggregationExecutor* left_child_ptr = dynamic_cast<AggregationExecutor*>(left.get());
            left.release();
            std::unique_ptr<AggregationExecutor> left_child(left_child_ptr);
            Tuple tuple;
            auto left_keys = plan_->GetLeftKeys();
            auto left_schema =  plan_->GetLeftPlan()->OutputSchema();
            left_child->Init();
            while (left_child->Next(&tuple)) {
                const Tuple* cur_tuple = &tuple;
                hash_t hash_val = HashValues(cur_tuple, left_schema, left_keys);
                jht_.Insert(exec_ctx_->GetTransaction(), hash_val, tuple);
            }
            break;
        }
        default: break;
    }
    is_init_ = true;
}

bool HashJoinExecutor::Next(Tuple *tuple) {
    auto right_keys = plan_->GetRightKeys();
    auto right_schema = plan_->GetRightPlan()->OutputSchema();
    auto left_schema = plan_->GetLeftPlan()->OutputSchema();
    auto out_schema = GetOutputSchema();
    switch (plan_->GetRightPlan()->GetType()) {
        case PlanType::SeqScan: {
            SeqScanExecutor* right_child = dynamic_cast<SeqScanExecutor*>(right.get());
            Tuple result_tuple;
            right_child->Init();
            if (right_child->Next(&result_tuple)) {
                const Tuple* cur_tuple = &result_tuple;
                hash_t hash_val = HashValues(cur_tuple, right_schema, right_keys);
                std::vector<Tuple> result;
                jht_.GetValue(exec_ctx_->GetTransaction(), hash_val, &result);
                for (auto probe_tuple: result) {
                    if (plan_->Predicate()->EvaluateJoin(&probe_tuple, left_schema, cur_tuple, right_schema).GetAs<bool>()) {
                        std::vector<Value> new_vals;
                        for (unsigned int i = 0; i < left_schema->GetColumnCount(); ++i) {
                            new_vals.push_back(probe_tuple.GetValue(left_schema, i));
                        }
                        for (unsigned int i = 0; i < right_schema->GetColumnCount(); ++i) {
                            new_vals.push_back(result_tuple.GetValue(right_schema, i));
                        }
                        buffer_.push(Tuple(new_vals, out_schema));
                    }
                }
            }
            break;
        }
        case PlanType::Aggregation: {
            AggregationExecutor* right_child = dynamic_cast<AggregationExecutor*>(right.get());
            Tuple result_tuple;
            right_child->Init();
            if (right_child->Next(&result_tuple)) {
                const Tuple* cur_tuple = &result_tuple;
                hash_t hash_val = HashValues(cur_tuple, right_schema, right_keys);
                std::vector<Tuple> result;
                jht_.GetValue(exec_ctx_->GetTransaction(), hash_val, &result);
                for (auto probe_tuple: result) {
                    if (plan_->Predicate()->EvaluateJoin(&probe_tuple, left_schema, cur_tuple, right_schema).GetAs<bool>()) {
                        std::vector<Value> new_vals;
                        for (unsigned int i = 0; i < left_schema->GetColumnCount(); ++i) {
                            new_vals.push_back(probe_tuple.GetValue(left_schema, i));
                        }
                        for (unsigned int i = 0; i < right_schema->GetColumnCount(); ++i) {
                            new_vals.push_back(result_tuple.GetValue(right_schema, i));
                        }
                        buffer_.push(Tuple(new_vals, out_schema));
                    }
                }
            }
            break;
        }
        default: break;
    }
    if (!buffer_.empty()) {
        *tuple = buffer_.front();
        buffer_.pop();
        return true;
    } else {
        return false;
    }
}
}  // namespace bustub