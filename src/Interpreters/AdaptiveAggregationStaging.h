#pragma once

#include <array>
#include <optional>
#include <variant>
#include <vector>

#include <Columns/IColumn_fwd.h>
#include <Core/ColumnNumbers.h>
#include <Common/PODArray.h>
#include <Interpreters/AdaptiveAggregation.h>
#include <base/PackedStringRef.h>

namespace DB
{

class Arena;

/// A count-record dedup pass (the publish's within-block pass or the seal's cross-mini pass)
/// is bypassed after this many consecutive passes whose records almost all survived it - the
/// pass costs a per-record candidate scan and finds nothing on a distinct stream. While
/// bypassed, every `adaptive_dedup_resample_interval`-th pass runs the dedup anyway, so a
/// stream whose distribution turns repetitive gets its dedup back.
constexpr size_t adaptive_dedup_unproductive_passes_to_bypass = 4;
constexpr size_t adaptive_dedup_resample_interval = 64;
/// Staged batches smaller than this are coalesced into one bucket-grouped chunk before they
/// reach the backlogs, so the merge-time drain processes a few large contiguous slices per
/// bucket instead of one tiny slice per consumed block; a batch of at least half the target
/// is enqueued as-is. Also bounds the coalescing buffer per thread.
constexpr size_t adaptive_seal_target_bytes = 4 << 20;

/// String-like keys stage their bytes: a packed reference copied as a plain value would
/// carry a pointer into the source block, which dies when the publish compacts the
/// arguments and releases it. The staged form of both string kinds is the raw characters,
/// and the drain rebuilds the table's key from them (the pressure-time drain additionally
/// persists the bytes into its arena; the merge-time drain borrows them).
template <typename Key>
constexpr bool adaptive_key_stages_bytes = std::is_same_v<Key, std::string_view> || std::is_same_v<Key, PackedStringRef>;


/// The staged records route by the two-level bucket of their key's hash, so the backlogs and
/// the routing structures come in the same 256 buckets as the two-level hash tables.
inline constexpr size_t ADAPTIVE_AGGREGATION_NUM_BUCKETS = 256;

/// All delayed records of one consumed block, grouped by bucket. One record batch per
/// consumed block, rather than one per (block, bucket); a thread's small batches are
/// further coalesced into one larger chunk of the same shape before they reach the
/// backlogs.
struct StagedChunk
{
    /// The bucket-grouped key side of a staged chunk, shared by both payload modes: record i's
    /// routing hash (reused by the drain's emplace) is `routing_hashes[i]` and its key bytes
    /// occupy `key_bytes[keyByteOffsetAt(i), keyByteOffsetAt(i) + keySizeAt(i))`; bucket b owns
    /// the record range [bucket_offsets[b], bucket_offsets[b + 1]). The key bytes are staged so
    /// that the drain emplaces without constructing a hashing state per (chunk, bucket) slice.
    struct StagedKeys
    {
        PaddedPODArray<UInt64> routing_hashes;
        PaddedPODArray<char> key_bytes;
        /// Byte offsets of the records' keys, populated only for variable-size (string-kind)
        /// keys. A fixed-size-key chunk carries no offsets: every position derives from
        /// `fixed_key_size`, which saves eight bytes per staged record.
        PaddedPODArray<UInt64> key_offsets;
        /// The staged width of a fixed-size key - `sizeof` of the shared method's key type,
        /// the exact width the kernels stage - or zero for variable-size keys.
        UInt64 fixed_key_size = 0;
        std::array<UInt32, ADAPTIVE_AGGREGATION_NUM_BUCKETS + 1> bucket_offsets{};

        size_t size() const { return routing_hashes.size(); }
        size_t recordsForBucket(size_t bucket) const { return bucket_offsets[bucket + 1] - bucket_offsets[bucket]; }
        UInt64 keyByteOffsetAt(size_t i) const { return fixed_key_size ? i * fixed_key_size : key_offsets[i]; }
        size_t keySizeAt(size_t i) const { return fixed_key_size ? fixed_key_size : key_offsets[i + 1] - key_offsets[i]; }
        std::string_view keyBytesAt(size_t i) const { return {key_bytes.data() + keyByteOffsetAt(i), keySizeAt(i)}; }
    };

    /// Value staging (simple-count aggregation only): the record is the key itself plus a run
    /// length - `multiplicities[i]` is how many source rows record i represents (repeats of a
    /// key collapse into one record at staging time).
    struct CountPayload
    {
        PaddedPODArray<UInt32> multiplicities;
    };

    /// Row-reference staging (general aggregates): record i reads its aggregate arguments from
    /// row i of `argument_columns`, which hold the records' values gathered at publish in the
    /// same bucket-grouped order, so a bucket's slice is a contiguous row range. Only the
    /// aggregate-argument positions are filled, kept at their original indexes so that the
    /// instruction preparation can index the vector; sparse arguments are materialized by the
    /// gather, so the staged columns are always dense.
    struct AggregatePayload
    {
        Columns argument_columns;

        /// The aggregate-function instructions over `argument_columns`, built in the chunk's
        /// own stable storage when the chunk is published (see `prepareStagedChunk`), so a
        /// published chunk is immutable and the drains read it without coordination.
        std::unique_ptr<const StagedChunkPreparation> prepared;

        AggregatePayload();
        AggregatePayload(AggregatePayload &&) noexcept;
        AggregatePayload & operator=(AggregatePayload &&) noexcept;
        ~AggregatePayload();
    };

    StagedKeys keys;
    std::variant<CountPayload, AggregatePayload> payload;

    bool countsOnly() const { return std::holds_alternative<CountPayload>(payload); }

    /// Debug-only structural invariants, checked at publication.
    bool wellFormed() const;

};

/// Converts recorded frozen-table misses into owned, bucket-grouped chunks. The producer
/// supplies aggregation metadata for each synchronous call and publishes returned chunks.
class StagedChunkConverter
{
public:
    /// The current block's misses, one entry per delayed record, in staging order.
    PaddedPODArray<UInt32> miss_source_rows;
    PaddedPODArray<UInt64> miss_hashes;
    PaddedPODArray<UInt8> miss_buckets;
    PaddedPODArray<UInt64> miss_key_sizes;
    PaddedPODArray<UInt32> miss_multiplicities;


    /// Builds a candidate without clearing misses, which the producer still needs for thaw sampling.
    template <typename SharedKey, typename State>
    MutableStagedChunkPtr build(
        const Columns & columns,
        const ColumnNumbersList & aggregates_positions,
        size_t aggregates_size,
        State & local_find_state,
        Arena & scratch_pool,
        bool counts_only,
        std::optional<UInt32> key_row_override);

    /// Returns a large candidate immediately, or buffers small candidates until the seal target.
    MutableStagedChunkPtr stage(
        MutableStagedChunkPtr chunk, size_t estimated_payload_bytes, const ColumnNumbersList & aggregates_positions);

    /// Returns the coalesced pending candidates, or null when there are none.
    MutableStagedChunkPtr flush(const ColumnNumbersList & aggregates_positions);

    void clearMisses();

private:
    template <typename SharedKey, typename State>
    void buildDeduplicatedCountChunk(
        StagedChunk & block, State & local_find_state, Arena & scratch_pool, std::optional<UInt32> key_row_override);

    template <typename SharedKey, typename State>
    void buildBucketGroupedAggregateChunk(
        StagedChunk & block, const Columns & columns, const ColumnNumbersList & aggregates_positions,
        size_t aggregates_size, State & local_find_state, Arena & scratch_pool, std::optional<UInt32> key_row_override);

    MutableStagedChunkPtr sealPendingChunks(const ColumnNumbersList & aggregates_positions);
    static void sealValueStagedChunkDeduplicated(const std::vector<MutableStagedChunkPtr> & minis, StagedChunk & chunk);

    /// Scratch for the value-staged publish grouping: the records' staging indexes in group
    /// order (the hashes stay in `miss_hashes`, so the entries are four bytes, not sixteen).
    std::vector<UInt32> grouped_index_scratch;
    std::vector<UInt32> group_offsets_scratch;
    std::vector<UInt32> group_cursor_scratch;

    /// Productivity tracking of a count-record dedup pass. The publish's within-block pass and
    /// the seal's cross-mini pass are tracked separately, because a stream can be distinct
    /// within blocks yet repetitive across them. Bypassing is always correct - duplicate count
    /// records merge at the drain's emplace - so a stale decision costs staged memory until
    /// the next resample, never results.
    struct DedupProductivity
    {
        size_t consecutive_unproductive = 0;
        size_t passes_since_resample = 0;
        bool bypassed = false;

        /// Whether the next pass should dedup: always while engaged, and periodically as a
        /// resample while bypassed, so a distribution change re-engages the dedup.
        bool shouldDedup()
        {
            if (!bypassed)
                return true;
            if (++passes_since_resample < adaptive_dedup_resample_interval)
                return false;
            passes_since_resample = 0;
            return true;
        }

        /// Feeds back a pass that ran the dedup: one productive pass re-engages, enough
        /// consecutive passes that merged almost nothing (less than 1/64 of the records)
        /// bypass.
        void record(size_t input_records, size_t surviving_records)
        {
            if (input_records == 0)
                return;
            if (surviving_records * 64 > input_records * 63)
            {
                if (++consecutive_unproductive >= adaptive_dedup_unproductive_passes_to_bypass)
                    bypassed = true;
            }
            else
            {
                consecutive_unproductive = 0;
                bypassed = false;
            }
        }
    };

    DedupProductivity publish_dedup;
    DedupProductivity seal_dedup;

    /// Small per-block staging batches buffered for coalescing: they are merged into one
    /// bucket-grouped chunk before they reach the backlogs (see `stage`), so the
    /// merge-time drain gets a few large contiguous slices per bucket instead of one tiny
    /// slice per consumed block. Flushed by `flush` when the input ends.
    std::vector<MutableStagedChunkPtr> pending_chunks;
    size_t pending_staged_bytes = 0;
};

/// Copies a contiguous record range and rebases its bucket and key offsets. Preparation is rebuilt by the caller.
MutableStagedChunkPtr sliceStagedChunk(const StagedChunk & source, size_t begin, size_t end);

}
