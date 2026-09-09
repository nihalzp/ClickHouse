#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/AdaptiveAggregationImpl.h>
#include <Processors/Executors/Runtime/PipelineExecutor.h>
#include <Processors/ISink.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Transforms/AdaptiveAggregationAdmissionTransform.h>
#include <Processors/Transforms/AdaptiveAggregationMergeTransform.h>

using namespace DB;

namespace
{

SharedHeader makeHeader()
{
    return std::make_shared<const Block>(
        Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "key")});
}

AggregatingTransformParamsPtr makeParams(const SharedHeader & header)
{
    Aggregator::Params params(
        Names{"key"}, AggregateDescriptions{}, /*overflow_row_=*/false, /*max_threads_=*/2,
        /*max_block_size_=*/65536, /*min_hit_rate_to_use_consecutive_keys_optimization_=*/0.5f,
        /*serialize_string_with_zero_byte_=*/false, /*enable_packed_string_keys_=*/true);
    params.only_merge = false;
    params.enable_adaptive_aggregator = true;
    params.adaptive_aggregator_freeze_threshold = 64;
    return std::make_shared<AggregatingTransformParams>(header, params, /*final_=*/true);
}

MutableStagedChunkPtr makeStagedChunk()
{
    auto chunk = std::make_shared<StagedChunk>();
    chunk->keys.fixed_key_size = sizeof(UInt64);
    chunk->keys.routing_hashes.push_back(0);
    chunk->keys.key_bytes.resize_fill(sizeof(UInt64), 0);
    chunk->keys.bucket_offsets.fill(1);
    chunk->keys.bucket_offsets[0] = 0;
    std::get<StagedChunk::CountPayload>(chunk->payload).multiplicities.push_back(1);
    return chunk;
}

Chunk envelope(const SharedHeader & header, StagedChunkPtr chunk)
{
    Chunk result(header->getColumns(), 0);
    result.getChunkInfos().add(std::make_shared<StagedChunkInfo>(std::move(chunk), false));
    return result;
}

/// Manual port driving can stop exactly between pulling an envelope and registering its payload.
struct Admission
{
    AdaptiveAggregationAdmissionTransform processor;
    OutputPort producer;
    InputPort completion;

    Admission(const SharedHeader & header, const AggregatingTransformParamsPtr & params, const AdaptiveAggregationSessionPtr & session)
        : processor(header, params, session), producer(header), completion(header)
    {
        connect(producer, processor.getInputs().front());
        connect(processor.getOutputs().front(), completion);
    }
};

Chunk keyRange(UInt64 begin, size_t rows)
{
    auto column = ColumnUInt64::create();
    auto & values = column->getData();
    values.resize(rows);
    for (size_t i = 0; i < rows; ++i)
        values[i] = begin + i;
    Columns columns;
    columns.push_back(std::move(column));
    return Chunk(std::move(columns), rows);
}

class KeySink final : public ISink
{
public:
    explicit KeySink(SharedHeader header) : ISink(std::move(header)) {}
    String getName() const override { return "KeySink"; }
    size_t rows = 0;
    UInt64 sum = 0;

private:
    void consume(Chunk chunk) override
    {
        rows += chunk.getNumRows();
        for (const auto value : assert_cast<const ColumnUInt64 &>(*chunk.getColumns().front()).getData())
            sum += value;
    }
};

}

TEST(AdaptiveAggregationPipeline, AcknowledgementFollowsIndependentRegistration)
{
    auto header = makeHeader();
    auto params = makeParams(header);
    auto session = std::make_shared<AdaptiveAggregationSession>();
    Admission first(header, params, session);
    Admission second(header, params, session);

    /// Admission does not require demand on its completion-only output.
    EXPECT_EQ(first.processor.prepare(), IProcessor::Status::NeedData);
    EXPECT_EQ(second.processor.prepare(), IProcessor::Status::NeedData);
    first.producer.push(envelope(header, makeStagedChunk()));
    EXPECT_EQ(first.processor.prepare(), IProcessor::Status::Ready);
    EXPECT_FALSE(first.producer.canPush());
    EXPECT_EQ(session->backlog.undrainedRecords(), 0);

    /// One receiver can register and acknowledge while the other has only pulled its envelope.
    second.producer.push(envelope(header, makeStagedChunk()));
    EXPECT_EQ(second.processor.prepare(), IProcessor::Status::Ready);
    second.processor.work();
    EXPECT_FALSE(second.producer.canPush());
    EXPECT_EQ(second.processor.prepare(), IProcessor::Status::NeedData);
    EXPECT_TRUE(second.producer.canPush());
    EXPECT_FALSE(first.producer.canPush());
    EXPECT_EQ(session->backlog.undrainedRecords(), 1);
    EXPECT_EQ(session->backlog.forMergeBucket(0).front().use_count(), 1);

    first.processor.work();
    EXPECT_FALSE(first.producer.canPush());
    EXPECT_EQ(first.processor.prepare(), IProcessor::Status::NeedData);
    EXPECT_TRUE(first.producer.canPush());
    EXPECT_EQ(session->backlog.undrainedRecords(), 2);
    EXPECT_FALSE(first.completion.hasData());
    EXPECT_FALSE(second.completion.hasData());
    EXPECT_FALSE(first.completion.isFinished());
    EXPECT_FALSE(second.completion.isFinished());
}

TEST(AdaptiveAggregationPipeline, CompletionWaitsForPulledPayload)
{
    auto header = makeHeader();
    auto params = makeParams(header);
    auto many_data = std::make_shared<ManyAggregatedData>(2);
    many_data->adaptive_session = std::make_shared<AdaptiveAggregationSession>();
    AdaptiveAggregationMergeTransform merge(params, many_data, 2, 2, nullptr);
    AdaptiveAggregationAdmissionTransform first(header, params, many_data->adaptive_session);
    AdaptiveAggregationAdmissionTransform second(header, params, many_data->adaptive_session);
    OutputPort first_producer(header);
    OutputPort second_producer(header);
    InputPort result(header);
    connect(first_producer, first.getInputs().front());
    connect(second_producer, second.getInputs().front());
    auto completion = merge.getInputs().begin();
    connect(first.getOutputs().front(), *completion++);
    connect(second.getOutputs().front(), *completion);
    connect(merge.getOutputs().front(), result);

    EXPECT_EQ(merge.prepare({&merge.getInputs().front(), &merge.getInputs().back()}, {}), IProcessor::Status::NeedData);
    EXPECT_EQ(second.prepare(), IProcessor::Status::NeedData);
    second_producer.push(envelope(header, makeStagedChunk()));
    second_producer.finish();
    EXPECT_EQ(second.prepare(), IProcessor::Status::Ready);
    first_producer.finish();
    EXPECT_EQ(first.prepare(), IProcessor::Status::Finished);
    EXPECT_EQ(merge.prepare({&merge.getInputs().front(), &merge.getInputs().back()}, {}), IProcessor::Status::NeedData);
    EXPECT_EQ(second.prepare(), IProcessor::Status::Ready);

    second.work();
    EXPECT_EQ(second.prepare(), IProcessor::Status::Finished);
    /// The coordinator can finish consumption even without downstream result demand.
    EXPECT_EQ(merge.prepare({&merge.getInputs().front(), &merge.getInputs().back()}, {}), IProcessor::Status::Ready);
    EXPECT_EQ(many_data->adaptive_session->backlog.undrainedRecords(), 1);
}

TEST(AdaptiveAggregationPipeline, CancellationReleasesQueuedAndPulledPayloads)
{
    for (const bool pulled : {false, true})
    {
        for (const bool cancelled : {false, true})
        {
            SCOPED_TRACE(pulled);
            SCOPED_TRACE(cancelled);
            auto header = makeHeader();
            auto params = makeParams(header);
            auto session = std::make_shared<AdaptiveAggregationSession>();
            Admission admission(header, params, session);
            ASSERT_EQ(admission.processor.prepare(), IProcessor::Status::NeedData);
            auto chunk = makeStagedChunk();
            std::weak_ptr<const StagedChunk> weak = chunk;
            admission.producer.push(envelope(header, std::move(chunk)));
            if (pulled)
                ASSERT_EQ(admission.processor.prepare(), IProcessor::Status::Ready);
            EXPECT_FALSE(weak.expired());
            if (cancelled)
                admission.processor.cancel();
            else
                admission.completion.close();
            EXPECT_EQ(admission.processor.prepare(), IProcessor::Status::Finished);
            EXPECT_TRUE(weak.expired());
            EXPECT_TRUE(session->cancelled.load());
            EXPECT_TRUE(admission.producer.isFinished());
            EXPECT_FALSE(admission.processor.getInputs().front().hasData());
            EXPECT_EQ(session->backlog.undrainedRecords(), 0);
        }
    }
}

TEST(AdaptiveAggregationPipeline, CompletesAtSingleAndMultipleExecutorThreads)
{
    for (const size_t threads : {1, 4})
    {
        SCOPED_TRACE(threads);
        auto header = makeHeader();
        auto params = makeParams(header);
        auto many_data = std::make_shared<ManyAggregatedData>(2);
        many_data->adaptive_session = std::make_shared<AdaptiveAggregationSession>();
        auto merge = std::make_shared<AdaptiveAggregationMergeTransform>(params, many_data, 2, 2, nullptr);
        auto processors = std::make_shared<Processors>();
        auto completion = merge->getInputs().begin();
        for (size_t producer_index = 0; producer_index < 2; ++producer_index)
        {
            const UInt64 begin = producer_index * 70000;
            Chunks chunks;
            /// A small candidate remains buffered when the next candidate passes through.
            /// Final flushing must deliver that remaining candidate before merge assembly.
            chunks.push_back(keyRange(begin, 8192));
            chunks.push_back(keyRange(begin + 8192, 140000));
            auto source = std::make_shared<SourceFromChunks>(header, std::move(chunks));
            auto producer = std::make_shared<AggregatingTransform>(
                header, params, many_data, producer_index, 2, 2, false, false, nullptr);
            auto admission = std::make_shared<AdaptiveAggregationAdmissionTransform>(header, params, many_data->adaptive_session);
            connect(source->getPort(), producer->getInputs().front());
            connect(producer->getOutputs().front(), admission->getInputs().front());
            connect(admission->getOutputs().front(), *completion++);
            processors->insert(processors->end(), {source, producer, admission});
        }
        auto sink = std::make_shared<KeySink>(header);
        connect(merge->getOutputs().front(), sink->getPort());
        processors->insert(processors->end(), {merge, sink});
        PipelineExecutor executor(processors, QueryStatusPtr{});
        executor.execute(threads, false);
        constexpr UInt64 expected_rows = 70000 + 8192 + 140000;
        EXPECT_EQ(sink->rows, expected_rows);
        EXPECT_EQ(sink->sum, expected_rows * (expected_rows - 1) / 2);
    }
}

TEST(AdaptiveAggregationPipeline, CompletionUsesUpdatedPortsAndCountsEachClosureOnce)
{
    auto header = makeHeader();
    auto params = makeParams(header);
    auto many_data = std::make_shared<ManyAggregatedData>(3);
    many_data->adaptive_session = std::make_shared<AdaptiveAggregationSession>();
    AdaptiveAggregationMergeTransform merge(params, many_data, 3, 3, nullptr);
    OutputPort first(header);
    OutputPort second(header);
    OutputPort third(header);
    InputPort result(header);
    auto input = merge.getInputs().begin();
    auto * first_input = &*input++;
    auto * second_input = &*input++;
    auto * third_input = &*input;
    connect(first, *first_input);
    connect(second, *second_input);
    connect(third, *third_input);
    connect(merge.getOutputs().front(), result);

    first.finish();
    EXPECT_EQ(merge.prepare({}, {}), IProcessor::Status::NeedData);
    second.finish();
    EXPECT_EQ(merge.prepare({first_input, second_input, second_input}, {}), IProcessor::Status::NeedData);
    EXPECT_EQ(merge.prepare({second_input}, {}), IProcessor::Status::NeedData);
    third.finish();
    EXPECT_EQ(merge.prepare({third_input}, {}), IProcessor::Status::Ready);
}

TEST(AdaptiveAggregationPipeline, CancellationReleasesProducerOutboxAndSuspendedInput)
{
    for (const bool published : {false, true})
    {
        SCOPED_TRACE(published);
        auto header = makeHeader();
        auto params = makeParams(header);
        auto many_data = std::make_shared<ManyAggregatedData>(2);
        auto session = std::make_shared<AdaptiveAggregationSession>();
        many_data->adaptive_session = session;
        AggregatingTransform producer(header, params, many_data, 0, 2, 2, false, false, nullptr);
        AdaptiveAggregationAdmissionTransform admission(header, params, session);
        OutputPort source(header);
        InputPort completion(header);
        connect(source, producer.getInputs().front());
        connect(producer.getOutputs().front(), admission.getInputs().front());
        connect(admission.getOutputs().front(), completion);
        ASSERT_EQ(admission.prepare(), IProcessor::Status::NeedData);
        ASSERT_EQ(producer.prepare(), IProcessor::Status::NeedData);
        auto input = keyRange(0, 150000);
        auto owner = input.getColumns().front();
        source.push(std::move(input));
        ASSERT_EQ(producer.prepare(), IProcessor::Status::Ready);
        producer.work();
        EXPECT_GT(owner->use_count(), 1);
        if (published)
            ASSERT_EQ(producer.prepare(), IProcessor::Status::PortFull);
        completion.close();
        ASSERT_EQ(admission.prepare(), IProcessor::Status::Finished);
        ASSERT_EQ(producer.prepare(), IProcessor::Status::Finished);
        EXPECT_EQ(owner->use_count(), 1);
        EXPECT_TRUE(source.isFinished());
        EXPECT_EQ(session->backlog.undrainedRecords(), 0);
    }
}
