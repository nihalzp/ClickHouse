#pragma once

#include <algorithm>
#include <optional>

#include <base/defines.h>
#include <base/extended_types.h>
#include <base/types.h>
#include <Interpreters/AdaptiveAggregation/AdaptiveAggregationImpl.h>

namespace DB
{

/// The rules by which the learning table of an adaptive producer freezes. The functions give the rules of an
/// early freeze, and `AdaptiveFreezeBounds` applies them to one block of a producer. The constants that tune
/// the rules are defined with the other tuning constants of the adaptive aggregation (see
/// `AdaptiveAggregationImpl.h`).

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

/// The key counts at which the learning table of a producer freezes, as they stand while the producer takes one
/// block: they follow from the rows that the producer received and expects, which are read once per block. The
/// slicer of a block and the check between blocks ask the same bounds, so a table freezes by the same rules wherever
/// a bound is noticed. Inside a block, the rows of the block that the table has not taken yet already count as
/// received, so the rows to come are estimated on the low side there, which only delays an early freeze to the end
/// of the block.
///
/// A table freezes at `adaptive_aggregator_freeze_threshold` keys whatever follows. When the query tells how many
/// rows to expect, it can freeze early. At the key count of an early freeze (see `adaptiveEarlyFreezeThreshold`) it
/// does while the freeze pays for itself (see `adaptiveEarlyFreezePays`), or while the table is on track to the whole
/// threshold anyway (see `adaptiveLearningReachesThreshold`), so that its freeze is only moved forward. At the end of
/// a trial in which its keys did not repeat (see `adaptiveLearningFindsNoRepeats`) it does only while the freeze
/// pays for itself: keys that do not repeat gain from staging only in bulk.
class AdaptiveFreezeBounds
{
public:
    /// `threshold` is `adaptive_aggregator_freeze_threshold` and `early_threshold` the key count of an early freeze.
    /// The producer received `received_rows` rows, the current block included, and expects `expected_rows` in all, or
    /// nothing when the query does not tell. `partitions` are those of the staging layout.
    AdaptiveFreezeBounds(
        size_t threshold_, size_t early_threshold_, std::optional<size_t> expected_rows_, size_t received_rows_, size_t partitions_)
        : threshold(threshold_)
        , early_threshold(early_threshold_)
        , expected_rows(expected_rows_)
        , received_rows(received_rows_)
        , early_freeze_pays(expected_rows_ && adaptiveEarlyFreezePays(*expected_rows_, received_rows_, partitions_))
    {
    }

    /// Whether a table that holds `keys` keys after it learned from `learned_rows` rows freezes.
    bool reached(size_t keys, size_t learned_rows) const
    {
        if (keys >= threshold)
            return true;
        if (!expected_rows)
            return false;
        if (keys >= early_threshold)
            return early_freeze_pays
                || adaptiveLearningReachesThreshold(keys, learned_rows, *expected_rows, received_rows, threshold);
        return early_freeze_pays && adaptiveLearningFindsNoRepeats(learned_rows, keys);
    }

    /// The rows that such a table, which has not reached a bound, takes before the bounds are asked again: as many
    /// as the keys it lacks to the next key count that can freeze it, so that it cannot pass that count unnoticed,
    /// but at least `adaptive_learning_slice_rows`, and no further than the end of a trial that can freeze it.
    size_t rowsToNextCheck(size_t keys, size_t learned_rows) const
    {
        size_t rows = std::max(nextKeyBound(keys) - keys, adaptive_learning_slice_rows);
        if (trialCanFreeze(learned_rows))
            rows = std::min(rows, adaptive_learning_trial_rows - learned_rows);
        return rows;
    }

    /// Whether a block of `rows` rows can take such a table to a bound before the block ends, so that the block has
    /// to be taken in slices: it can pass the next key count that can freeze the table, or the trial ends in it.
    bool reachableWithin(size_t keys, size_t learned_rows, size_t rows) const
    {
        return keys + rows > nextKeyBound(keys) || trialCanFreeze(learned_rows);
    }

private:
    /// The next key count at which the table can freeze: that of an early freeze while the table is below it and the
    /// query tells how many rows to expect.
    size_t nextKeyBound(size_t keys) const { return expected_rows && keys < early_threshold ? early_threshold : threshold; }

    /// Whether the table is still in a trial whose end can freeze it.
    bool trialCanFreeze(size_t learned_rows) const { return early_freeze_pays && learned_rows < adaptive_learning_trial_rows; }

    const size_t threshold;
    const size_t early_threshold;
    const std::optional<size_t> expected_rows;
    const size_t received_rows;
    const bool early_freeze_pays;
};

}
