/// The method-specialized frozen consume kernels and their staging orchestration. Conversion
/// templates are defined in AdaptiveAggregationStagingImpl.h; bucket application and pressure
/// policy have separate translation units.

#include <algorithm>
#include <bit>
#include <limits>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Arena.h>
#include <Common/CurrentThread.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/ThreadStatus.h>
#include <Common/memcpySmall.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <base/arithmeticOverflow.h>
#include <base/memcmpSmall.h>
#include <base/unaligned.h>
#include <Interpreters/AdaptiveAggregationImpl.h>
#include <Interpreters/AdaptiveAggregationStagingImpl.h>
#include <Interpreters/AggregationUtils.h>

namespace ProfileEvents
{
    extern const Event AdaptiveAggregationProbeBypasses;
    extern const Event AdaptiveAggregationStagedBytes;
    extern const Event AdaptiveAggregationStagedRecords;
    extern const Event AdaptiveAggregationStagedRecordsMerged;
    extern const Event AggregationOptimizedEqualRangesOfKeys;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
    extern const int LOGICAL_ERROR;
}

}

namespace
{
    /// Whether the string views the state's key holders hand out point into storage that
    /// outlives the row loop (a batch-serialized buffer or the key column itself) rather than
    /// into per-row scratch that dies with the holder. Only the serialized methods can go
    /// either way, and they expose the choice as `use_batch_serialize`; the run tracking in
    /// the count kernel may only remember a previous key's view when this holds.
    template <typename State>
    bool ALWAYS_INLINE adaptiveKeyViewsAreBlockStable(const State & state)
    {
        if constexpr (requires { state.use_batch_serialize; })
            return state.use_batch_serialize;
        else
            return true;
    }
}

namespace DB
{

void Aggregator::executeFrozen(
    const Columns & columns,
    size_t row_begin,
    size_t row_end,
    AggregatedDataVariants & result,
    ColumnRawPtrs & key_columns,
    AggregateFunctionInstruction * aggregate_instructions,
    AdaptiveAggregationProducer & adaptive,
    bool all_keys_are_const) const
{
#define M(NAME) \
    else if (result.type == AggregatedDataVariants::Type::NAME) \
        executeFrozenImpl( \
            *result.NAME, \
            std::type_identity<std::decay_t<decltype(*result.NAME##_two_level)>>{}, \
            result.aggregates_pool, \
            columns, \
            row_begin, \
            row_end, \
            key_columns, \
            aggregate_instructions, \
            adaptive, \
            all_keys_are_const);

    if (false) {} // NOLINT
    APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)
#undef M
    else
        throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant in the adaptive frozen path.");
}

/// The set counterpart of the frozen kernel. A `GROUP BY` without aggregate functions has no places to
/// record and no states to advance, so a hit costs nothing beyond the probe and a miss stages the key
/// alone - which is all a set has to carry.
template <typename LocalMethod, typename SharedMethod>
requires SetAggregationMethod<LocalMethod>
void NO_INLINE Aggregator::executeFrozenImpl(
    LocalMethod & local_method,
    std::type_identity<SharedMethod>,
    Arena *,
    const Columns & columns,
    size_t row_begin,
    size_t row_end,
    ColumnRawPtrs & key_columns,
    AggregateFunctionInstruction *,
    AdaptiveAggregationProducer & adaptive,
    bool all_keys_are_const) const
{
    Arena scratch_pool;

    typename LocalMethod::StateNoCache local_find_state(key_columns, key_sizes, aggregation_state_cache);

    /// The kernel runs only while the producer is frozen, and phase transitions happen between
    /// blocks, so the reference stays valid for the whole block.
    auto & frozen = std::get<AdaptiveAggregationProducer::FrozenState>(adaptive.phase);
    const bool bypass_local_probe = frozen.bypass_local_probe;

    auto stage_miss = [&]([[maybe_unused]] const auto & key, UInt64 hash, size_t row)
    {
        adaptive.converter.miss_source_rows.push_back(static_cast<UInt32>(row));
        adaptive.converter.miss_hashes.push_back(hash);
        adaptive.converter.miss_buckets.push_back(static_cast<UInt8>(SharedMethod::Data::getBucketFromHash(hash)));
        if constexpr (adaptive_key_stages_bytes<typename SharedMethod::Key>)
            adaptive.converter.miss_key_sizes.push_back(adaptiveStagedKeyBytes(key).size());
    };

    if (all_keys_are_const)
    {
        auto && key_holder = local_find_state.getKeyHolder(0, scratch_pool);
        const auto & key = keyHolderGetKey(key_holder);
        const UInt64 hash = local_method.data.hash(key);

        /// The whole range carries one key and a set stores a key once, so a single record stands
        /// for it however many rows it spans.
        if (!local_method.data.find(key, hash))
        {
            stage_miss(key, hash, row_begin);
            publishDelayedRecords<typename SharedMethod::Key>(
                columns, row_end, adaptive, local_find_state, scratch_pool, /*counts_only=*/false, /*key_row_override=*/0);
        }
        keyHolderDiscardKey(key_holder);
        return;
    }

    size_t hits = 0;
    for (size_t i = row_begin; i < row_end; ++i)
    {
        auto && key_holder = local_find_state.getKeyHolder(i, scratch_pool);
        const auto & key = keyHolderGetKey(key_holder);
        const UInt64 hash = local_method.data.hash(key);

        if (!bypass_local_probe && local_method.data.find(key, hash))
            ++hits;
        else
            stage_miss(key, hash, i);

        keyHolderDiscardKey(key_holder);
    }

    if (!frozen.bypass_local_probe)
    {
        frozen.sampled_hits += hits;
        frozen.sampled_rows += row_end - row_begin;
        if (frozen.sampled_rows >= adaptive_bypass_sample_rows
            && frozen.sampled_hits * adaptive_bypass_hit_rate_inverse < frozen.sampled_rows)
        {
            frozen.bypass_local_probe = true;
            ProfileEvents::increment(ProfileEvents::AdaptiveAggregationProbeBypasses);
        }
    }

    publishDelayedRecords<typename SharedMethod::Key>(
        columns, row_end, adaptive, local_find_state, scratch_pool, /*counts_only=*/false);
}

template <typename LocalMethod, typename SharedMethod>
requires MapAggregationMethod<LocalMethod>
void NO_INLINE Aggregator::executeFrozenImpl(
    LocalMethod & local_method,
    std::type_identity<SharedMethod>,
    Arena * aggregates_pool,
    const Columns & columns,
    size_t row_begin,
    size_t row_end,
    ColumnRawPtrs & key_columns,
    AggregateFunctionInstruction * aggregate_instructions,
    AdaptiveAggregationProducer & adaptive,
    bool all_keys_are_const) const
{
    Arena scratch_pool;

    typename LocalMethod::StateNoCache local_find_state(key_columns, key_sizes, aggregation_state_cache);
    /// Routing needs only the two-level twin's TYPE: the hash is the local table's canonical
    /// hash (identical to the twin's by construction - the pairing in `executeFrozen` binds a
    /// method to its own two-level form, which keeps the hash function), and the bucket
    /// mapping is static. Borrowing the shared table's method instance here would race with a
    /// pressure spill re-initializing it; the mutable early-drain table must not double as a
    /// hash-policy object.

    /// The kernel runs only while the producer is frozen, and phase transitions happen between
    /// blocks, so the reference stays valid for the whole block.
    auto & frozen = std::get<AdaptiveAggregationProducer::FrozenState>(adaptive.phase);
    auto update_bypass_sampling = [&](size_t hits, size_t rows)
    {
        if (frozen.bypass_local_probe)
            return;
        frozen.sampled_hits += hits;
        frozen.sampled_rows += rows;
        if (frozen.sampled_rows >= adaptive_bypass_sample_rows
            && frozen.sampled_hits * adaptive_bypass_hit_rate_inverse < frozen.sampled_rows)
        {
            frozen.bypass_local_probe = true;
            ProfileEvents::increment(ProfileEvents::AdaptiveAggregationProbeBypasses);
        }
    };
    const bool bypass_local_probe = frozen.bypass_local_probe;

    if (all_keys_are_const)
    {
        auto && key_holder = local_find_state.getKeyHolder(0, scratch_pool);
        const auto & key = keyHolderGetKey(key_holder);
        const UInt64 hash = local_method.data.hash(key);

        bool found = false;
        AggregateDataPtr found_place = nullptr;
        if (auto it = local_method.data.find(key, hash))
        {
            found = true;
            if (is_simple_count)
                getInlineCountState(it->getMapped()) += row_end - row_begin;
            else
                found_place = it->getMapped();
        }

        if (found)
        {
            if (!is_simple_count && params.aggregates_size)
            {
                /// Apply the whole range to the single place, mirroring the ordinary
                /// all-keys-are-const handling.
                for (size_t i = 0; i < aggregate_functions.size(); ++i)
                {
                    AggregateFunctionInstruction * inst = aggregate_instructions + i;
                    ProfileEvents::increment(ProfileEvents::AggregationOptimizedEqualRangesOfKeys);
                    addBatchSinglePlace(row_begin, row_end, inst, found_place + inst->state_offset, aggregates_pool);
                }
            }
        }
        else
        {
            const auto bucket = static_cast<UInt8>(SharedMethod::Data::getBucketFromHash(hash));

            if (is_simple_count)
            {
                adaptive.converter.miss_hashes.push_back(hash);
                adaptive.converter.miss_multiplicities.push_back(static_cast<UInt32>(row_end - row_begin));
                if constexpr (adaptive_key_stages_bytes<typename SharedMethod::Key>)
                    adaptive.converter.miss_key_sizes.push_back(adaptiveStagedKeyBytes(key).size());
                adaptive.converter.miss_buckets.push_back(bucket);
            }
            else
            {
                for (size_t i = row_begin; i < row_end; ++i)
                {
                    adaptive.converter.miss_source_rows.push_back(static_cast<UInt32>(i));
                    adaptive.converter.miss_hashes.push_back(hash);
                    adaptive.converter.miss_buckets.push_back(bucket);
                    if constexpr (adaptive_key_stages_bytes<typename SharedMethod::Key>)
                        adaptive.converter.miss_key_sizes.push_back(adaptiveStagedKeyBytes(key).size());
                }
            }
            publishDelayedRecords<typename SharedMethod::Key>(
                columns, row_end, adaptive, local_find_state, scratch_pool, /*counts_only=*/is_simple_count, /*key_row_override=*/0);
        }
        keyHolderDiscardKey(key_holder);
        return;
    }

    if (is_simple_count)
    {
        size_t hits = 0;
        typename SharedMethod::Key last_staged_key{};
        [[maybe_unused]] const bool stable_key_views = adaptiveKeyViewsAreBlockStable(local_find_state);
        for (size_t i = row_begin; i < row_end; ++i)
        {
            auto && key_holder = local_find_state.getKeyHolder(i, scratch_pool);
            const auto & key = keyHolderGetKey(key_holder);
            const UInt64 hash = local_method.data.hash(key);

            if (!bypass_local_probe)
            {
                if (auto it = local_method.data.find(key, hash))
                {
                    ++hits;
                    ++getInlineCountState(it->getMapped());
                    keyHolderDiscardKey(key_holder);
                    continue;
                }
            }

            const typename SharedMethod::Key staged_key = key;

            bool run_continues = !adaptive.converter.miss_hashes.empty() && adaptive.converter.miss_hashes.back() == hash;
            if constexpr (std::is_same_v<typename SharedMethod::Key, std::string_view>)
                run_continues = run_continues && stable_key_views && staged_key == last_staged_key;
            else
                run_continues = run_continues && staged_key == last_staged_key;

            if (run_continues)
            {
                ++adaptive.converter.miss_multiplicities.back();
            }
            else
            {
                adaptive.converter.miss_hashes.push_back(hash);
                adaptive.converter.miss_multiplicities.push_back(1);
                /// Fixed-size keys stage no size: it is a compile-time constant the publish
                /// substitutes, so the hot staging loop skips a dead store per record.
                if constexpr (adaptive_key_stages_bytes<typename SharedMethod::Key>)
                    adaptive.converter.miss_key_sizes.push_back(adaptiveStagedKeyBytes(staged_key).size());

                /// A serialized key view points into the reused scratch arena and can only seed
                /// the run tracking when the views are block-stable; every other key type is
                /// either a self-contained value or, for a packed reference, points into the
                /// block's key column, whose bytes outlive the block.
                if constexpr (std::is_same_v<typename SharedMethod::Key, std::string_view>)
                {
                    if (stable_key_views)
                        last_staged_key = staged_key;
                }
                else
                {
                    last_staged_key = staged_key;
                }
                adaptive.converter.miss_source_rows.push_back(static_cast<UInt32>(i));
                adaptive.converter.miss_buckets.push_back(static_cast<UInt8>(SharedMethod::Data::getBucketFromHash(hash)));
            }
            keyHolderDiscardKey(key_holder);
        }
        update_bypass_sampling(hits, row_end - row_begin);
        publishDelayedRecords<typename SharedMethod::Key>(columns, row_end, adaptive, local_find_state, scratch_pool, /*counts_only=*/true);
        return;
    }

    /// The probe/staging loop, shared by the with-places and the zero-aggregates shapes: a
    /// keyed GROUP BY without aggregate functions needs no places at all (the baseline
    /// specializes the same way), so it skips the allocation and the per-row stores.
    auto probe_rows = [&]<bool record_places>(AggregateDataPtr * places_data) -> size_t
    {
        size_t hits = 0;
        for (size_t i = row_begin; i < row_end; ++i)
        {
            auto && key_holder = local_find_state.getKeyHolder(i, scratch_pool);
            const auto & key = keyHolderGetKey(key_holder);
            const UInt64 hash = local_method.data.hash(key);

            if (!bypass_local_probe)
            {
                if (auto it = local_method.data.find(key, hash))
                {
                    ++hits;
                    if constexpr (record_places)
                        places_data[i] = it->getMapped();
                    keyHolderDiscardKey(key_holder);
                    continue;
                }
            }

            if constexpr (record_places)
                places_data[i] = nullptr;
            adaptive.converter.miss_source_rows.push_back(static_cast<UInt32>(i));
            adaptive.converter.miss_hashes.push_back(hash);
            adaptive.converter.miss_buckets.push_back(static_cast<UInt8>(SharedMethod::Data::getBucketFromHash(hash)));

            if constexpr (adaptive_key_stages_bytes<typename SharedMethod::Key>)
                adaptive.converter.miss_key_sizes.push_back(adaptiveStagedKeyBytes(key).size());
            keyHolderDiscardKey(key_holder);
        }
        return hits;
    };

    if (params.aggregates_size == 0)
    {
        const size_t hits = probe_rows.template operator()<false>(nullptr);
        update_bypass_sampling(hits, row_end - row_begin);
        publishDelayedRecords<typename SharedMethod::Key>(columns, row_end, adaptive, local_find_state, scratch_pool, /*counts_only=*/false);
        return;
    }

    AllocatorWithMemoryTracking<AggregateDataPtr> allocator;
    const size_t places_size = row_end;
    auto places_deleter = [&allocator, &places_size](auto * ptr)
    {
        if (ptr) [[likely]]
            allocator.deallocate(ptr, places_size);
    };
    std::unique_ptr<AggregateDataPtr[], decltype(places_deleter)> places(allocator.allocate(places_size), places_deleter);

    const size_t hits = probe_rows.template operator()<true>(places.get());
    update_bypass_sampling(hits, row_end - row_begin);
    publishDelayedRecords<typename SharedMethod::Key>(columns, row_end, adaptive, local_find_state, scratch_pool, /*counts_only=*/false);

    /// With no local hits every place is null and the batch pass would only skip rows; the
    /// staged records carry the block's whole contribution. Bypassed blocks are always all-miss.
    if (hits != 0)
        executeAggregateInstructions(
            aggregates_pool,
            row_begin,
            row_end,
            aggregate_instructions,
            places.get(),
            /*key_start=*/row_begin,
            /*has_only_one_value_since_last_reset=*/false,
            /*all_keys_are_const=*/false,
            /*use_compiled_functions=*/false);
}

template <typename SharedKey, typename State>
void NO_INLINE Aggregator::publishDelayedRecords(
    const Columns & columns,
    size_t num_rows,
    AdaptiveAggregationProducer & adaptive,
    State & local_find_state,
    Arena & scratch_pool,
    bool counts_only,
    std::optional<UInt32> key_row_override) const
{
    const size_t total = adaptive.converter.miss_hashes.size();
    if (!total)
        return;

    if (num_rows > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Adaptive aggregation got a block of {} rows; row numbers are 32-bit.", num_rows);

    auto & shared = *adaptive.session;

    auto block = adaptive.converter.build<SharedKey>(
        columns, aggregates_positions, params.aggregates_size, local_find_state, scratch_pool, counts_only, key_row_override);
    auto & keys = block->keys;

    size_t batch_bytes = 0;
    if constexpr (adaptive_key_stages_bytes<SharedKey>)
        for (const auto size : adaptive.converter.miss_key_sizes)
            batch_bytes += size;
    else
        batch_bytes = total * sizeof(SharedKey);

    if (counts_only)
        batch_bytes += total * sizeof(UInt32);
    else
        for (const auto & column : std::get<StagedChunk::AggregatePayload>(block->payload).argument_columns)
            if (column && !column->valuesHaveFixedSize())
                batch_bytes += column->byteSize();
    batch_bytes += total * (sizeof(UInt64) + (adaptive_key_stages_bytes<SharedKey> ? sizeof(UInt64) : 0));

    observeAdaptiveStagedRecords(shared, adaptive.converter.miss_hashes, batch_bytes);

    adaptive.converter.clearMisses();

    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationStagedRecords, total);
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationStagedRecordsMerged, total - keys.size());
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationStagedBytes, keys.key_bytes.size());

    size_t estimated_payload_bytes
        = keys.key_bytes.size() + keys.key_offsets.size() * sizeof(UInt64) + keys.routing_hashes.size() * sizeof(UInt64);
    if (const auto * counts = std::get_if<StagedChunk::CountPayload>(&block->payload))
        estimated_payload_bytes += counts->multiplicities.size() * sizeof(UInt32);
    else
        for (const auto & column : std::get<StagedChunk::AggregatePayload>(block->payload).argument_columns)
            if (column)
                estimated_payload_bytes += column->byteSize();

    if (auto ready = adaptive.converter.stage(std::move(block), estimated_payload_bytes, aggregates_positions))
        publishStagedChunk(*adaptive.session, std::move(ready));
}

}
