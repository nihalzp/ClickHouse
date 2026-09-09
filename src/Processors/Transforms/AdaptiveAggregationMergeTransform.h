#pragma once

#include <Processors/Transforms/AggregatingTransform.h>

namespace DB
{

/// Waits for every producer's admission stream to finish, then owns the ordinary aggregation
/// merge pipeline. Completion ports never carry staged payloads or per-chunk notifications.
class AdaptiveAggregationMergeTransform final : public IProcessor
{
public:
    AdaptiveAggregationMergeTransform(
        AggregatingTransformParamsPtr params_, ManyAggregatedDataPtr many_data_,
        size_t max_threads_, size_t temporary_data_merge_threads_, RuntimeDataflowStatisticsCacheUpdaterPtr updater_);

    String getName() const override { return "AdaptiveAggregationMergeTransform"; }
    Status prepare() override;
    void work() override;
    PipelineUpdate updatePipeline() override;
    void onCancel() noexcept override;

private:
    AggregatingTransformParamsPtr params;
    ManyAggregatedDataPtr many_data;
    AdaptiveAggregationSessionPtr session;
    size_t max_threads;
    size_t temporary_data_merge_threads;
    RuntimeDataflowStatisticsCacheUpdaterPtr updater;
    Processors processors;
    std::list<TemporaryBlockStreamHolder> tmp_files;
    bool merge_initialized = false;
    bool pipeline_created = false;
};

}
