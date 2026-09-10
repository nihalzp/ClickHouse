#pragma once

#include <Interpreters/Aggregator.h>

namespace DB
{

/// Keeps a block's columns and instructions alive while its staged chunks await admission.
/// The processor transports the outbox and resumes pending checks; only `Aggregator` reads
/// or changes the suspended block state.
struct AdaptiveAggregationExecution
{
    explicit AdaptiveAggregationExecution(AdaptiveAggregationProducer & producer_) : producer(producer_)
    {
    }

    /// Reports whether admission must be followed by `Aggregator::resumeAdaptiveBlock`.
    bool hasPendingBlock() const { return next_step != Aggregator::PostBlockStep::None; }

    /// Prepared chunks awaiting transport and the allocation context to use for their admission.
    std::vector<StagedChunkPtr> ready_chunks;
    bool use_own_memory_tracker = false;

private:
    friend class Aggregator;

    /// The producer outlives its suspended execution state, including cancellation cleanup.
    AdaptiveAggregationProducer & producer;
    Aggregator::PostBlockStep next_step = Aggregator::PostBlockStep::None;
    size_t input_rows = 0;
    Aggregator::PostBlockSnapshot snapshot;

    /// These owners remain alive until all post-block checks finish. The resume method releases
    /// prepared storage under the aggregation tracker and input columns under the caller's tracker.
    Columns columns;
    Columns materialized_columns;
    Aggregator::NestedColumnsHolder nested_columns_holder;
    Aggregator::AggregateFunctionInstructions instructions;
};

}
