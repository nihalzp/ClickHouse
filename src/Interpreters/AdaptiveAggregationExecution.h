#pragma once

#include <Interpreters/Aggregator.h>

namespace DB
{

/// Storage owned by an aggregating processor while prepared chunks cross its admission port.
/// The input owners survive post-block checks, but no thread-local tracker or hashing state
/// survives a scheduler yield.
struct AdaptiveAggregationExecution
{
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
    size_t result_size = 0;
    Int64 current_memory_usage = 0;
    Int64 result_size_bytes = 0;

    Columns columns;
    Columns materialized_columns;
    Aggregator::NestedColumnsHolder nested_columns_holder;
    Aggregator::AggregateFunctionInstructions instructions;
};

}
