#include <Processors/Transforms/AdaptiveAggregationAdmissionTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Interpreters/AdaptiveAggregationImpl.h>

namespace DB
{

AdaptiveAggregationAdmissionTransform::AdaptiveAggregationAdmissionTransform(
    SharedHeader header, std::shared_ptr<AggregatingTransformParams> params_, AdaptiveAggregationSessionPtr session_)
    : IProcessor({header}, {header})
    , params(std::move(params_))
    , session(std::move(session_))
{
}

IProcessor::Status AdaptiveAggregationAdmissionTransform::prepare()
{
    auto & input = inputs.front();
    auto & output = outputs.front();
    if (isCancelled() || output.isFinished())
    {
        session->cancel();
        input.close();
        if (input.hasData())
            input.pullData(/*set_not_needed=*/true);
        current_chunk.clear();
        has_current_chunk = false;
        return Status::Finished;
    }

    if (has_current_chunk)
        return Status::Ready;

    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    /// Completion carries no data and needs no output demand. Do not renew input demand while
    /// a pulled chunk is waiting for work: the producer uses that demand as its acknowledgement.
    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    current_chunk = input.pull(/*set_not_needed=*/true);
    has_current_chunk = true;
    return Status::Ready;
}

void AdaptiveAggregationAdmissionTransform::work()
{
    const auto info = current_chunk.getChunkInfos().get<StagedChunkInfo>();
    chassert(info);
    params->aggregator.admitStagedChunk(*session, info->chunk, info->use_own_memory_tracker);
    current_chunk.clear();
    has_current_chunk = false;
}

void AdaptiveAggregationAdmissionTransform::onCancel() noexcept
{
    session->cancel();
}

}
