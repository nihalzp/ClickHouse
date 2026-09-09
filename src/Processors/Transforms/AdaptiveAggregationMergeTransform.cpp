#include <Processors/Transforms/AdaptiveAggregationMergeTransform.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Interpreters/AdaptiveAggregationImpl.h>

namespace DB
{

AdaptiveAggregationMergeTransform::AdaptiveAggregationMergeTransform(
    AggregatingTransformParamsPtr params_, ManyAggregatedDataPtr many_data_,
    size_t max_threads_, size_t temporary_data_merge_threads_, RuntimeDataflowStatisticsCacheUpdaterPtr updater_)
    : IProcessor({}, {params_->getHeader()})
    , params(std::move(params_))
    , many_data(std::move(many_data_))
    , session(many_data->adaptive_session)
    , max_threads(std::min(many_data->num_producers, max_threads_))
    , temporary_data_merge_threads(temporary_data_merge_threads_)
    , updater(std::move(updater_))
{
    auto header = std::make_shared<const Block>(params->getHeader());
    for (size_t i = 0; i < many_data->num_producers; ++i)
        inputs.emplace_back(*header, this);
}

IProcessor::Status AdaptiveAggregationMergeTransform::prepare(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts &)
{
    auto & output = outputs.front();
    if (isCancelled() || output.isFinished())
    {
        session->cancel();
        for (auto & input : inputs)
            input.close();
        many_data.reset();
        return Status::Finished;
    }

    if (!merge_initialized)
    {
        if (!inputs_initialized)
        {
            for (auto & input : inputs)
            {
                if (!input.isFinished())
                {
                    input.setNeeded();
                    unfinished_inputs.insert(&input);
                }
            }
            inputs_initialized = true;
        }
        else
        {
            for (const auto * input : updated_inputs)
                if (input->isFinished())
                    unfinished_inputs.erase(input);
        }
        return unfinished_inputs.empty() ? Status::Ready : Status::NeedData;
    }

    if (!pipeline_created)
    {
        if (!processors.empty())
            return Status::UpdatePipeline;
        output.finish();
        many_data.reset();
        return Status::Finished;
    }

    auto & input = inputs.back();
    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }
    if (input.isFinished())
    {
        output.finish();
        many_data.reset();
        return Status::Finished;
    }
    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;
    output.push(input.pull());
    return Status::PortFull;
}

void AdaptiveAggregationMergeTransform::work()
{
    processors = createAggregationMergePipeline(
        params, many_data, max_threads, temporary_data_merge_threads,
        /*should_produce_results_in_order_of_bucket_number=*/false, /*skip_merging=*/false, updater, tmp_files);
    merge_initialized = true;
}

IProcessor::PipelineUpdate AdaptiveAggregationMergeTransform::updatePipeline()
{
    auto & output = processors.back()->getOutputs().front();
    inputs.emplace_back(output.getHeader(), this);
    connect(output, inputs.back());
    pipeline_created = true;
    for (auto & processor : processors)
        processor->inheritQueryPlanStepFromParent(*this, static_cast<size_t>(AggregatingStep::AggregatingStage::FinalAggregation));
    return PipelineUpdate{.to_add = std::move(processors), .to_remove = {}};
}

void AdaptiveAggregationMergeTransform::onCancel() noexcept
{
    session->cancel();
}

}
