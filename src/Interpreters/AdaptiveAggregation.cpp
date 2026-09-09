#include <unordered_set>

#include <Columns/IColumn.h>
#include <Common/Arena.h>
#include <Common/MemoryTrackerSwitcher.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Interpreters/AdaptiveAggregationImpl.h>

namespace ProfileEvents
{
    extern const Event AdaptiveAggregationBucketsRetired;
    extern const Event AdaptiveAggregationStagedChunkSplits;
    extern const Event AdaptiveAggregationThaws;
}

namespace DB
{

void Aggregator::prepareStagedChunk(StagedChunk & block) const
{
    auto & payload = std::get<StagedChunk::AggregatePayload>(block.payload);

    auto prep = std::make_unique<StagedChunkPreparation>();
    prep->aggregate_columns.resize(params.aggregates_size);
    prep->instructions.resize(params.aggregates_size + 1);
    prep->instructions[params.aggregates_size].that = nullptr;

    /// The payload columns are already in the drain's form - the seal normalized them at the
    /// gather - so the instructions wire the columns directly and only the combinator
    /// unwrapping remains. Nothing dense is materialized here, and a staged payload is never
    /// sparse.
    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        prep->aggregate_columns[i].resize(params.aggregates[i].argument_names.size());
        for (size_t j = 0; j < prep->aggregate_columns[i].size(); ++j)
            prep->aggregate_columns[i][j] = payload.argument_columns[aggregates_positions[i][j]].get();
        buildAggregateFunctionInstruction(
            i, /*has_sparse_arguments=*/false, prep->aggregate_columns, prep->instructions, prep->nested_columns_holder);
    }

    payload.prepared = std::move(prep);
}

void Aggregator::initAdaptiveSession(AggregatedDataVariants & local_result, AdaptiveAggregationSession & shared) const
{
    auto early_drain_variants = std::make_shared<AggregatedDataVariants>();
    early_drain_variants->aggregator = this;
    early_drain_variants->keys_size = params.keys_size;
    early_drain_variants->key_sizes = key_sizes;
    early_drain_variants->init(convertToTwoLevelTypeIfPossible(local_result.type));

    shared.drain_type = early_drain_variants->type;
    shared.early_drain_variants = std::move(early_drain_variants);
    shared.initialized.store(true, std::memory_order_release);
}

void Aggregator::prepareStagedChunks(
    const AdaptiveAggregationSession & shared, MutableStagedChunkPtr block, std::vector<StagedChunkPtr> & ready_chunks) const
{
    chassert(block->wellFormed());

    /// The drains claim chunks whole, so a chunk is never let into the backlogs larger than
    /// the part its claim is bounded by (see `splitStagedChunkAtPartBound`).
    auto pieces = splitStagedChunkAtPartBound(shared, *block);
    if (pieces.empty())
    {
        appendPreparedStagedChunk(std::move(block), ready_chunks);
        return;
    }

    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationStagedChunkSplits);
    LOG_TRACE(
        log,
        "Adaptive aggregation: split a staged chunk of {} records into {} pieces at the part bound",
        block->keys.size(),
        pieces.size());
    block.reset();
    for (auto & piece : pieces)
    {
        chassert(piece->wellFormed());
        appendPreparedStagedChunk(std::move(piece), ready_chunks);
    }
}

void Aggregator::appendPreparedStagedChunk(MutableStagedChunkPtr block, std::vector<StagedChunkPtr> & ready_chunks) const
{
    /// Prepared here, on the publishing thread, so the chunk is immutable once any bucket can
    /// see it.
    if (std::holds_alternative<StagedChunk::AggregatePayload>(block->payload))
        prepareStagedChunk(*block);

    ready_chunks.push_back(std::move(block));
}

void Aggregator::admitStagedChunk(
    AdaptiveAggregationSession & shared, const StagedChunkPtr & chunk, bool use_own_memory_tracker) const
{
    std::optional<MemoryTrackerSwitcher> memory_tracker_switcher;
    if (use_own_memory_tracker)
        memory_tracker_switcher.emplace(memory_tracker.get());
    shared.backlog.publish(chunk);
}

void AdaptiveAggregationSession::StagedBacklog::publish(const StagedChunkPtr & chunk)
{
    undrained_records.fetch_add(chunk->keys.size(), std::memory_order_relaxed);
    registerChunk(chunk);
}

void AdaptiveAggregationSession::StagedBacklog::registerChunk(const StagedChunkPtr & chunk)
{
    std::shared_lock registry_lock(registry_mutex);
    for (size_t b = 0; b < ADAPTIVE_AGGREGATION_NUM_BUCKETS; ++b)
    {
        if (!chunk->keys.recordsForBucket(b))
            continue;

        auto & bucket = buckets[b];
        std::lock_guard lock(bucket.mutex);
        bucket.backlog.push_back(chunk);
    }
}

void AdaptiveAggregationSession::StagedBacklog::releaseMergedBucket(size_t bucket)
{
    std::shared_lock registry_lock(registry_mutex);
    auto & b = buckets[bucket];
    std::lock_guard lock(b.mutex);
    b.backlog = {};
}

std::vector<StagedChunkPtr> AdaptiveAggregationSession::StagedBacklog::takeAllForPressureDrain()
{
    std::vector<StagedChunkPtr> chunks;
    std::unique_lock registry_lock(registry_mutex);
    /// A chunk is registered with every bucket it has records for, so the swap-out sees it
    /// once per such bucket and keeps the first appearance.
    std::unordered_set<const void *> seen;
    for (auto & bucket : buckets)
    {
        std::vector<StagedChunkPtr> claimed;
        {
            std::lock_guard bucket_lock(bucket.mutex);
            claimed.swap(bucket.backlog);
        }
        for (auto & chunk : claimed)
            if (seen.insert(chunk.get()).second)
                chunks.push_back(std::move(chunk));
    }
    return chunks;
}

void Aggregator::retireAdaptiveMergedBucket(AggregatedDataVariants & dest, AdaptiveAggregationSession & shared, size_t bucket) const
{
    dest.adaptive_merge_bucket_arenas[bucket].reset();
    shared.backlog.releaseMergedBucket(bucket);
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationBucketsRetired);
}

/// Thawing is the adaptive aggregation standing down globally: when the staged stream
/// proves to keep repeating the same missing keys instead of bringing rare ones, every
/// thread returns to ordinary insertion for good. A frozen table thaws; a thread still
/// learning stops trying to freeze. Staging such a stream re-copies a repeated key's
/// bytes on every occurrence, while an unfrozen table would absorb the repeats as cheap
/// in-place updates.
///
/// The verdict is evaluated over totals shared by all threads; the tuning constants
/// hold the calibration:
///
///     wasted bytes per distinct key = (repeat - 1) * bytes per record
///                                   > adaptive_thaw_wasted_bytes_per_key
///
/// Here repeat = thaw_sampled_records / distinct_sampled_hashes, and bytes per record =
/// staged_bytes / staged_records. A key's first record is the price of storing it once;
/// each repeat wastes one record's bytes, so heavy records tolerate few repeats and tiny
/// ones many. Until the verdict fires, every publish folds its batch into the shared
/// evidence and re-evaluates, so the thread whose batch tips the totals over the bound
/// fires for everyone by setting `thaw_all`, once `staged_records` has reached the
/// `adaptive_thaw_min_staged_records` evidence floor. A publish updates:
///
/// - `staged_records` grows by the batch's record count.
/// - `staged_bytes` grows by the batch's estimated footprint, computed below as
///   `batch_bytes`. It counts the key bytes as the kernel staged them, the variable-width
///   aggregate arguments at their gathered sizes (the sealed chunk's columns hold exactly
///   the staged rows, so a wide tail behind a narrow frequent head is charged its real
///   width rather than the block's average), and the per-record bookkeeping the chunk
///   stores (the eight-byte routing hash, plus an eight-byte key offset only for
///   byte-staged keys; fixed keys have no offsets). A column read by several aggregates is staged
///   once, so it is counted once; a count batch stages a four-byte run length instead of
///   arguments.
///   The estimate is taken before the count deduplication (which merges a batch's
///   repeats of one key into a single record with a run length), so it charges every
///   staged record. That is deliberate: `staged_records` also counts the records before
///   deduplication, and the verdict's bytes per record is `staged_bytes` divided by
///   `staged_records`, so the two counters must describe the same set of records for
///   the ratio to mean anything.
///   Variable-width arguments count in full because staging such a value pays real work
///   at every step. The seal gathers it out of the block into the staged column (a copy),
///   the staged chunk pins that memory until the merge drains it, and updating the
///   aggregate state from it copies the value once more (a string min keeps its own copy
///   of the winning value). A repeated key pays all of that on every occurrence, where
///   an unfrozen table would have paid a single in-place state update, so each repeat of
///   a heavy value is genuine waste.
///   Fixed-width arguments are deliberately not counted because their staging copy is a
///   few bytes and the drain consumes the staged batch with the same vectorized batch
///   executor the scan would have used on the original block. Deferring such values
///   moves the work without multiplying it, so their staging costs about what their
///   consumption saves. Charging them would fire the thaw on streams where staging is in
///   fact profitable. The measured anchor is a stream of five UInt64 arguments at repeat
///   10: it stays a clear adaptive win, and counting its forty fixed bytes per record
///   would have thawed it.
/// - The sampler receives the batch's routing hashes matching `hash & 0xFF == 0`, about
///   total / 256 of them, collected outside the lock. `thaw_sampled_records` counts
///   every sampled occurrence; `distinct_sampled_hashes` collapses a key's repeats onto
///   one entry across all threads, so their ratio estimates the stream's repeat factor
///   independently of how the keys spread over the threads.
///
/// The verdict lands at each thread's next between-blocks check; a learning thread about
/// to freeze also checks it at the crossing, so no table freezes against it. The current
/// records are still published: their rows were deferred by the frozen kernel and only
/// the drain will aggregate them.
void Aggregator::observeAdaptiveStagedRecords(
    AdaptiveAggregationSession & shared, const PaddedPODArray<UInt64> & hashes, size_t batch_bytes) const
{
    if (!shared.thaw_all.load(std::memory_order_relaxed))
    {
        PaddedPODArray<UInt64> sampled_hashes;
        for (const auto hash : hashes)
            if ((hash & adaptive_thaw_sample_mask) == 0)
                sampled_hashes.push_back(hash);

        std::lock_guard lock(shared.thaw_sample_mutex);
        shared.staged_records += hashes.size();
        shared.staged_bytes += batch_bytes;
        shared.thaw_sampled_records += sampled_hashes.size();
        for (const auto hash : sampled_hashes)
            shared.distinct_sampled_hashes.insert(hash);
        /// Re-checked under the lock: a thread that sampled while another was firing would
        /// otherwise fire a second time. The verdict compares the wasted staged bytes per
        /// distinct key, (repeat - 1) * bytes per record, against the bound. It is rearranged
        /// onto a common denominator so the arithmetic stays integral:
        /// (sampled - distinct) * staged_bytes > bound * distinct * staged_records.
        /// The products are widened to 128 bits: a giant near-unique stream (billions of
        /// staged records times their bytes) overflows 64, and a wrapped product could thaw
        /// a healthy stream.
        const size_t distinct = shared.distinct_sampled_hashes.size();
        if (!shared.thaw_all.load(std::memory_order_relaxed)
            && shared.staged_records >= adaptive_thaw_min_staged_records
            && shared.thaw_sampled_records > distinct
            && static_cast<UInt128>(shared.thaw_sampled_records - distinct) * shared.staged_bytes
                > static_cast<UInt128>(adaptive_thaw_wasted_bytes_per_key) * distinct * shared.staged_records)
        {
            shared.thaw_all.store(true, std::memory_order_relaxed);
            ProfileEvents::increment(ProfileEvents::AdaptiveAggregationThaws);
            const double repeat = static_cast<double>(shared.thaw_sampled_records) / static_cast<double>(distinct);
            LOG_TRACE(
                log,
                "Adaptive aggregation: thawing the local tables after {} staged records ({} bytes, repeat factor {:.2f}, {} wasted bytes per key)",
                shared.staged_records,
                shared.staged_bytes,
                repeat,
                static_cast<size_t>((repeat - 1.0) * (static_cast<double>(shared.staged_bytes) / static_cast<double>(shared.staged_records))));
        }
    }

}

void Aggregator::flushPendingChunks(AdaptiveAggregationProducer & adaptive, std::vector<StagedChunkPtr> & ready_chunks) const
{
    if (auto chunk = adaptive.converter.flush(aggregates_positions))
        prepareStagedChunks(*adaptive.session, std::move(chunk), ready_chunks);
}

/// The flushed variants' sizes are meaningless by the time the external path finishes, so a
/// stored entry keeps its sizes: only the verdict is written, and only when the session staged
/// enough records to trust the thaw sampler. Runs without a measurement leave the entry alone.
void Aggregator::recordAdaptiveStagingVerdict(AdaptiveAggregationSession & shared) const
{
    const auto & stats_params = params.stats_collecting_params;
    if (!stats_params.isCollectionAndUseEnabled())
        return;

    bool measured = false;
    bool repeat_dominated = false;
    {
        std::lock_guard lock(shared.thaw_sample_mutex);
        measured = shared.staged_records >= adaptive_thaw_min_staged_records;
        repeat_dominated = shared.thaw_all.load(std::memory_order_relaxed);
    }
    if (!measured)
        return;

    auto & stats = getHashTablesStatistics<AggregationEntry>();
    AggregationEntry entry{.sum_of_sizes = 0, .median_size = 0, .adaptive_staging_repeat_dominated = repeat_dominated};
    if (const auto prev = stats.getSizeHint(stats_params))
    {
        entry.sum_of_sizes = prev->sum_of_sizes;
        entry.median_size = prev->median_size;
    }
    stats.update(entry, stats_params);
}

}
