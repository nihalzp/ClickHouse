#pragma once

#include <Interpreters/AdaptiveAggregation.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>

namespace DB
{

struct AggregatingTransformParams;

/// An owned, prepared payload carried only by a producer's dedicated admission port.
class StagedChunkInfo final : public ChunkInfoCloneable<StagedChunkInfo>
{
public:
    StagedChunkInfo(StagedChunkPtr chunk_, bool use_own_memory_tracker_)
        : chunk(std::move(chunk_)), use_own_memory_tracker(use_own_memory_tracker_)
    {
    }

    const StagedChunkPtr chunk;
    const bool use_own_memory_tracker;
};

/// Admits one producer's chunks independently. Its output carries completion only; renewed
/// input demand acknowledges that the pulled chunk has been registered and released locally.
class AdaptiveAggregationAdmissionTransform final : public IProcessor
{
public:
    AdaptiveAggregationAdmissionTransform(
        SharedHeader header, std::shared_ptr<AggregatingTransformParams> params_, AdaptiveAggregationSessionPtr session_);

    String getName() const override { return "AdaptiveAggregationAdmissionTransform"; }
    Status prepare() override;
    void work() override;
    void onCancel() noexcept override;

private:
    std::shared_ptr<AggregatingTransformParams> params;
    AdaptiveAggregationSessionPtr session;
    Chunk current_chunk;
    bool has_current_chunk = false;
};

}
