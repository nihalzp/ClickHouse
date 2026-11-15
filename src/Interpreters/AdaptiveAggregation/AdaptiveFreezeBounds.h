#pragma once

#include <base/defines.h>
#include <base/extended_types.h>
#include <base/types.h>
#include <Interpreters/AdaptiveAggregation/AdaptiveAggregationImpl.h>

namespace DB
{

/// The number of keys at which each of `producers` learning tables freezes early (see `AdaptiveFreezeBounds`), out of
/// a freeze threshold of `threshold` keys. Every producer learns the keys of its table from the first rows it sees,
/// so the producers together learn their number times the threshold in keys, whatever the size of the aggregation.
/// With 192 producers and 16,384 keys each that is 3.1 million keys, and when a filter leaves 13 million rows, a
/// quarter of them go through the learning tables. A key that many producers hold has a state in each of them for
/// the merge to combine, which costs most where the states are heavy, as those of `uniqExact` are. So above
/// `adaptive_full_freeze_threshold_producers` producers the tables share that many thresholds: 192 producers take
/// 5,461 keys each.
inline size_t adaptiveEarlyFreezeThreshold(size_t threshold, size_t producers)
{
    if (producers <= adaptive_full_freeze_threshold_producers)
        return threshold;
    return threshold * adaptive_full_freeze_threshold_producers / producers;
}

/// Whether a learning table that took `rows` rows and holds `keys` keys ends its learning because its keys do not
/// repeat: past the trial of `adaptive_learning_trial_rows` rows, when fewer than one row in
/// `adaptive_bypass_hit_rate_inverse` repeated a key of the table.
inline bool adaptiveLearningFindsNoRepeats(size_t rows, size_t keys)
{
    if (rows < adaptive_learning_trial_rows)
        return false;
    const size_t repeats = rows > keys ? rows - keys : 0;
    return repeats * adaptive_bypass_hit_rate_inverse < rows;
}

/// Whether an early freeze pays for its fixed cost: the producer, which received `received_rows` rows of the
/// `expected_rows` it expects in all, expects at least `adaptive_early_freeze_rows_per_partition` more rows per
/// partition of the layout. For example, with 2,048 partitions a producer that received 10,000 rows freezes early
/// only if it expects 42,768 rows or more.
inline bool adaptiveEarlyFreezePays(size_t expected_rows, size_t received_rows, size_t partitions)
{
    return expected_rows >= received_rows + adaptive_early_freeze_rows_per_partition * partitions;
}

/// Whether a learning table is on track to `threshold` keys, at which it freezes whatever follows, so that an early
/// freeze adds no freeze to the aggregation. The table holds `keys` keys after it learned from `learned_rows` rows,
/// and its producer received `received_rows` rows of the `expected_rows` it expects in all. The rows to come bring
/// new keys at the rate of the rows learned from, at most: keys repeat more as a table fills. For example, a table
/// that holds 5,600 keys after 8,000 rows and expects 30,000 more rows is on track for
/// 5,600 + 30,000 * 5,600 / 8,000 = 26,600 keys, past a threshold of 16,384.
inline bool adaptiveLearningReachesThreshold(
    size_t keys, size_t learned_rows, size_t expected_rows, size_t received_rows, size_t threshold)
{
    chassert(learned_rows >= keys && keys > 0);
    const size_t remaining_rows = expected_rows > received_rows ? expected_rows - received_rows : 0;
    return keys + static_cast<size_t>(static_cast<UInt128>(remaining_rows) * keys / learned_rows) >= threshold;
}

}
