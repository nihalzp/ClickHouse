#include <Interpreters/AdaptiveAggregationImpl.h>
#include <Interpreters/AdaptiveAggregationStagingImpl.h>
#include <Processors/Merges/Algorithms/PartitionedChunkCoalescing.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
    extern const Event AdaptiveAggregationStagedRecordsMerged;
    extern const Event AdaptiveAggregationSealedChunks;
}

namespace DB
{

bool StagedChunk::wellFormed() const
{
    const size_t records = keys.size();
    if (keys.bucket_offsets.back() != records)
        return false;
    if (keys.fixed_key_size)
    {
        if (!keys.key_offsets.empty() || keys.key_bytes.size() != records * keys.fixed_key_size)
            return false;
    }
    else
    {
        if (keys.key_offsets.size() != records + 1 || keys.key_offsets.back() != keys.key_bytes.size())
            return false;
        for (size_t i = 0; i < records; ++i)
            if (keys.key_offsets[i] > keys.key_offsets[i + 1])
                return false;
    }
    for (size_t b = 0; b < ADAPTIVE_AGGREGATION_NUM_BUCKETS; ++b)
        if (keys.bucket_offsets[b] > keys.bucket_offsets[b + 1])
            return false;
    if (const auto * counts = std::get_if<CountPayload>(&payload))
        return counts->multiplicities.size() == records;
    for (const auto & column : std::get<AggregatePayload>(payload).argument_columns)
        if (column && column->size() != records)
            return false;
    return true;
}

StagedChunk::AggregatePayload::AggregatePayload() = default;
StagedChunk::AggregatePayload::AggregatePayload(AggregatePayload &&) noexcept = default;
StagedChunk::AggregatePayload & StagedChunk::AggregatePayload::operator=(AggregatePayload &&) noexcept
    = default;
StagedChunk::AggregatePayload::~AggregatePayload() = default;

void StagedChunkConverter::clearMisses()
{
    miss_source_rows.clear();
    miss_hashes.clear();
    miss_buckets.clear();
    miss_key_sizes.clear();
    miss_multiplicities.clear();
}

namespace
{

auto stagedOffsets(const std::vector<MutableStagedChunkPtr> & minis)
{
    return [&minis](size_t source, size_t partition) { return minis[source]->keys.bucket_offsets[partition]; };
}

/// Concatenates the minis' bucket-grouped keys into `keys`: bucket b's records are the
/// concatenation of the minis' b-slices in buffer order. A caller's payload concatenation must
/// walk the same (bucket, mini) order, so a record keeps one position across the key, hash,
/// and payload arrays.
void concatenateStagedKeys(StagedChunk::StagedKeys & keys, const std::vector<MutableStagedChunkPtr> & minis)
{
    constexpr size_t num_buckets = ADAPTIVE_AGGREGATION_NUM_BUCKETS;

    size_t total = 0;
    for (size_t b = 0; b < num_buckets; ++b)
    {
        keys.bucket_offsets[b] = static_cast<UInt32>(total);
        for (const auto & mini : minis)
        {
            chassert(mini->countsOnly() == minis.front()->countsOnly());
            total += mini->keys.recordsForBucket(b);
        }
    }
    keys.bucket_offsets[num_buckets] = static_cast<UInt32>(total);

    UInt64 total_key_bytes = 0;
    for (const auto & mini : minis)
        total_key_bytes += mini->keys.key_bytes.size();

    keys.routing_hashes.resize(total);
    forEachPartitionedChunkRange(
        num_buckets, minis.size(), stagedOffsets(minis),
        [&](size_t source, size_t begin, size_t length, size_t destination)
        {
            memcpy(&keys.routing_hashes[destination], &minis[source]->keys.routing_hashes[begin], length * sizeof(UInt64));
        });

    keys.fixed_key_size = minis.front()->keys.fixed_key_size;
    if (!keys.fixed_key_size)
        keys.key_offsets.resize(total + 1);
    keys.key_bytes.resize(total_key_bytes);
    {
        UInt64 byte_pos = 0;
        forEachPartitionedChunkRange(
            num_buckets, minis.size(), stagedOffsets(minis),
            [&](size_t source, size_t begin, size_t length, size_t destination)
            {
                const auto & mini = minis[source];
                const UInt64 src_begin = mini->keys.keyByteOffsetAt(begin);
                const UInt64 slice_bytes = mini->keys.keyByteOffsetAt(begin + length) - src_begin;
                memcpy(keys.key_bytes.data() + byte_pos, mini->keys.key_bytes.data() + src_begin, slice_bytes);
                if (!keys.fixed_key_size)
                    for (size_t j = 0; j < length; ++j)
                        keys.key_offsets[destination + j] = byte_pos + (mini->keys.key_offsets[begin + j] - src_begin);
                byte_pos += slice_bytes;
            });
        if (!keys.fixed_key_size)
            keys.key_offsets[total] = byte_pos;
    }
}

/// The bypassed count seal: a straight concatenation with no cross-mini dedup. Duplicate count
/// records are legal - the drain merges them at its emplace - so a stale bypass costs staged
/// memory until the next resample, never results.
void sealValueStagedChunkConcatenated(const std::vector<MutableStagedChunkPtr> & minis, StagedChunk & chunk)
{
    concatenateStagedKeys(chunk.keys, minis);

    auto & multiplicities = chunk.payload.emplace<StagedChunk::CountPayload>().multiplicities;
    multiplicities.resize(chunk.keys.size());
    forEachPartitionedChunkRange(
        ADAPTIVE_AGGREGATION_NUM_BUCKETS, minis.size(), stagedOffsets(minis),
        [&](size_t source, size_t begin, size_t length, size_t destination)
        {
            const auto & mini_multiplicities = std::get<StagedChunk::CountPayload>(minis[source]->payload).multiplicities;
            memcpy(&multiplicities[destination], &mini_multiplicities[begin], length * sizeof(UInt32));
        });
}

}

MutableStagedChunkPtr StagedChunkConverter::stage(
    MutableStagedChunkPtr block, size_t estimated_payload_bytes, const ColumnNumbersList & aggregates_positions)
{
    /// Coalescing pays in proportion to how many batches merge into one chunk. A batch of at
    /// least half the seal target could only ever merge with one neighbor, gaining almost
    /// nothing for a full extra copy of its data, so it is enqueued as-is.
    if (estimated_payload_bytes * 2 >= adaptive_seal_target_bytes)
    {
        return block;
    }

    pending_chunks.push_back(std::move(block));
    pending_staged_bytes += estimated_payload_bytes;

    if (pending_staged_bytes >= adaptive_seal_target_bytes)
        return sealPendingChunks(aggregates_positions);
    return {};
}

MutableStagedChunkPtr StagedChunkConverter::flush(const ColumnNumbersList & aggregates_positions)
{
    if (!pending_chunks.empty())
        return sealPendingChunks(aggregates_positions);
    return {};
}

MutableStagedChunkPtr StagedChunkConverter::sealPendingChunks(const ColumnNumbersList & aggregates_positions)
{
    auto & minis = pending_chunks;
    const size_t num_minis = minis.size();

    if (num_minis == 1)
    {
        auto chunk = std::move(minis.front());
        minis.clear();
        pending_staged_bytes = 0;
        return chunk;
    }

    auto chunk = std::make_shared<StagedChunk>();
    auto & keys = chunk->keys;
    const bool counts_only = minis.front()->countsOnly();

    if (counts_only)
    {
        /// The cross-mini dedup merges keys repeating across the buffered batches, which the
        /// per-block publish dedup cannot see. On a distinct stream it merges nothing; the
        /// productivity tracker then degrades the seal to a straight concatenation.
        if (seal_dedup.shouldDedup())
        {
            size_t input_records = 0;
            for (const auto & mini : minis)
                input_records += mini->keys.size();
            sealValueStagedChunkDeduplicated(minis, *chunk);
            seal_dedup.record(input_records, chunk->keys.size());
        }
        else
            sealValueStagedChunkConcatenated(minis, *chunk);
    }
    else
    {
        concatenateStagedKeys(keys, minis);

        auto columns_of = [](const StagedChunk & mini) -> const Columns &
        { return std::get<StagedChunk::AggregatePayload>(mini.payload).argument_columns; };

        auto & argument_columns = chunk->payload.emplace<StagedChunk::AggregatePayload>().argument_columns;
        argument_columns.assign(columns_of(*minis.front()).size(), nullptr);
        for (const auto & argument_positions : aggregates_positions)
            for (const auto position : argument_positions)
            {
                if (argument_columns[position])
                    continue;

                /// The seal normalized every batch's payload columns to the dense form the
                /// drain consumes, so the buffered batches always agree at a position and the
                /// coalescing is a plain concatenation.
                VectorWithMemoryTracking<ColumnPtr> sources;
                sources.reserve(num_minis);
                for (const auto & mini : minis)
                    sources.push_back(columns_of(*mini)[position]);

                argument_columns[position] = coalescePartitionedColumn(
                    sources, ADAPTIVE_AGGREGATION_NUM_BUCKETS, stagedOffsets(minis));
            }
    }

    size_t batch_records = 0;
    for (const auto & mini : minis)
        batch_records += mini->keys.size();
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationSealedChunks);
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationStagedRecordsMerged, batch_records - keys.size());

    static const auto log = getLogger("Aggregator");
    LOG_TRACE(
        log,
        "Adaptive aggregation: sealed {} staged batches into one chunk of {} records",
        num_minis,
        keys.size());

    minis.clear();
    pending_staged_bytes = 0;
    return chunk;
}

void StagedChunkConverter::sealValueStagedChunkDeduplicated(
    const std::vector<MutableStagedChunkPtr> & minis,
    StagedChunk & chunk)
{
    constexpr size_t num_buckets = ADAPTIVE_AGGREGATION_NUM_BUCKETS;

    auto multiplicities_of = [](const StagedChunk & mini) -> const PaddedPODArray<UInt32> &
    { return std::get<StagedChunk::CountPayload>(mini.payload).multiplicities; };

    size_t total = 0;
    UInt64 total_key_bytes = 0;
    for (const auto & mini : minis)
    {
        chassert(mini->countsOnly());
        total += mini->keys.size();
        total_key_bytes += mini->keys.key_bytes.size();
    }

    auto & keys = chunk.keys;
    auto & multiplicities = chunk.payload.emplace<StagedChunk::CountPayload>().multiplicities;
    keys.fixed_key_size = minis.front()->keys.fixed_key_size;
    keys.routing_hashes.resize(total);
    multiplicities.resize(total);
    if (!keys.fixed_key_size)
        keys.key_offsets.resize(total + 1);
    keys.key_bytes.resize(total_key_bytes);

    /// The publish dedup only sees one block; keys repeating across the buffered batches are
    /// merged here, while the seal copies the records anyway. Same scheme as the publish walk:
    /// group a bucket's records by a few hash bits so a duplicate can only be one of its
    /// group's survivors, then compare within the group.
    struct StagedRef
    {
        UInt64 hash;
        UInt32 mini;
        UInt32 index;
    };
    std::vector<StagedRef> refs;
    std::vector<StagedRef> grouped;

    size_t out = 0;
    UInt64 byte_pos = 0;
    for (size_t b = 0; b < num_buckets; ++b)
    {
        keys.bucket_offsets[b] = static_cast<UInt32>(out);

        constexpr size_t num_groups = 256;
        std::array<UInt32, num_groups + 1> group_offsets{};

        /// One pass collects the bucket's records and their group histogram together; the
        /// records are then scattered whole into group order, so the dedup pass reads them
        /// sequentially instead of gathering through an index vector.
        refs.clear();
        for (size_t m = 0; m < minis.size(); ++m)
        {
            const auto & mini = *minis[m];
            for (size_t j = mini.keys.bucket_offsets[b]; j < mini.keys.bucket_offsets[b + 1]; ++j)
            {
                refs.push_back({mini.keys.routing_hashes[j], static_cast<UInt32>(m), static_cast<UInt32>(j)});
                ++group_offsets[((mini.keys.routing_hashes[j] >> 10) & 0xFF) + 1];
            }
        }
        if (refs.empty())
            continue;

        for (size_t g = 0; g < num_groups; ++g)
            group_offsets[g + 1] += group_offsets[g];
        std::array<UInt32, num_groups> group_cursor{};
        for (size_t g = 0; g < num_groups; ++g)
            group_cursor[g] = group_offsets[g];
        grouped.resize(refs.size());
        for (const auto & ref : refs)
            grouped[group_cursor[(ref.hash >> 10) & 0xFF]++] = ref;

        for (size_t g = 0; g < num_groups; ++g)
        {
            const size_t group_out_begin = out;
            for (size_t i = group_offsets[g]; i < group_offsets[g + 1]; ++i)
            {
                const auto & ref = grouped[i];
                const auto & mini = *minis[ref.mini];

                /// Batch key bytes live in the minis' padded staged arrays.
                const KeyBytesRef key{mini.keys.keyBytesAt(ref.index), ReadablePadding::AtLeast15Bytes};
                mergeOrAppendStagedCount(
                    keys, multiplicities, ref.hash, key, multiplicities_of(mini)[ref.index], group_out_begin, out, byte_pos);
            }
        }
    }

    keys.bucket_offsets[num_buckets] = static_cast<UInt32>(out);
    if (!keys.fixed_key_size)
    {
        keys.key_offsets[out] = byte_pos;
        keys.key_offsets.resize(out + 1);
    }

    keys.routing_hashes.resize(out);
    multiplicities.resize(out);
    keys.key_bytes.resize(byte_pos);
}

/// The records [begin, end) of a chunk as a chunk of their own. The records are laid out bucket
/// by bucket, so any record range is a contiguous slice of every staged array and the piece is a
/// plain copy of that slice with the offsets rebased; the buckets outside the range come out
/// empty, and a bucket the range starts or ends inside keeps the records that fell in it. The
/// drains read a bucket's records from the piece's own offsets, so a bucket that spans two
/// pieces is drained in two goes, into the same table if the same claim takes both pieces, and
/// otherwise into two parts the external merge folds together.
MutableStagedChunkPtr sliceStagedChunk(const StagedChunk & source, size_t begin, size_t end)
{
    const auto & src = source.keys;
    const size_t records = end - begin;

    /// Reserved exactly: the claim charges a chunk by its allocation (`estimateStagedBytesWithKeyCopy`), and
    /// the pieces were sized by their bytes, so a power-of-two rounding of the arrays would make
    /// a piece look up to twice its size to the claim and stop it a piece early.
    auto piece = std::make_shared<StagedChunk>();
    auto & keys = piece->keys;
    keys.fixed_key_size = src.fixed_key_size;
    keys.routing_hashes.reserve_exact(records);
    keys.routing_hashes.insert(src.routing_hashes.begin() + begin, src.routing_hashes.begin() + end);

    const size_t byte_begin = src.keyByteOffsetAt(begin);
    const size_t byte_end = src.keyByteOffsetAt(end);
    keys.key_bytes.reserve_exact(byte_end - byte_begin);
    keys.key_bytes.insert(src.key_bytes.begin() + byte_begin, src.key_bytes.begin() + byte_end);
    if (!src.fixed_key_size)
    {
        keys.key_offsets.reserve_exact(records + 1);
        for (size_t i = begin; i <= end; ++i)
            keys.key_offsets.push_back(src.key_offsets[i] - byte_begin);
    }

    for (size_t b = 0; b <= ADAPTIVE_AGGREGATION_NUM_BUCKETS; ++b)
        keys.bucket_offsets[b] = static_cast<UInt32>(std::clamp<size_t>(src.bucket_offsets[b], begin, end) - begin);

    if (const auto * counts = std::get_if<StagedChunk::CountPayload>(&source.payload))
    {
        auto & multiplicities = piece->payload.emplace<StagedChunk::CountPayload>().multiplicities;
        multiplicities.reserve_exact(records);
        multiplicities.insert(counts->multiplicities.begin() + begin, counts->multiplicities.begin() + end);
    }
    else
    {
        const auto & columns = std::get<StagedChunk::AggregatePayload>(source.payload).argument_columns;
        auto & argument_columns = piece->payload.emplace<StagedChunk::AggregatePayload>().argument_columns;
        argument_columns.reserve(columns.size());
        for (const auto & column : columns)
            argument_columns.push_back(column ? column->cut(begin, records) : nullptr);
    }
    return piece;
}

}
