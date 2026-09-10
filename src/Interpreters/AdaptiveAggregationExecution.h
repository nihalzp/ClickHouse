#pragma once

#include <Interpreters/Aggregator.h>

namespace DB
{

/// Storage owned by an aggregating processor while prepared chunks cross its admission port.
/// The input owners survive post-block checks, but no thread-local tracker or hashing state
/// survives a scheduler yield.
struct AdaptiveAggregationExecution
{
    explicit AdaptiveAggregationExecution(AdaptiveAggregationProducer & producer_) : producer(producer_)
    {
    }

    /// The producer outlives its execution storage, including cancellation cleanup.
    AdaptiveAggregationProducer & producer;

    enum class Continuation
    {
        None,
        BeforeMemoryCheck,
        FrozenPressureDrain,
        BaselinePressureDrain,
    };

    enum class Finish
    {
        NotStarted,
        AfterFinalFlush,
        Complete,
    };

    std::vector<StagedChunkPtr> ready_chunks;
    size_t next_chunk = 0;
    Continuation continuation = Continuation::None;
    Finish finish = Finish::NotStarted;
    bool use_own_memory_tracker = false;
    size_t input_rows = 0;
    Aggregator::PostBlockSnapshot snapshot;

    Columns columns;
    Columns materialized_columns;
    Aggregator::NestedColumnsHolder nested_columns_holder;
    Aggregator::AggregateFunctionInstructions instructions;
};

}
