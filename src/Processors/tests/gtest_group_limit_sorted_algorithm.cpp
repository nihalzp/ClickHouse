#include <gtest/gtest.h>

#include <limits>

#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Merges/Algorithms/GroupLimitSortedAlgorithm.h>

using namespace DB;

namespace
{

SharedHeader makeHeader(const DataTypePtr & seen_rows_type = std::make_shared<DataTypeUInt8>())
{
    return std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "key"),
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "payload"),
        ColumnWithTypeAndName(seen_rows_type, "seen_rows")});
}

SortDescription runDescription()
{
    SortDescription description;
    description.emplace_back("key", 1, 1);
    description.emplace_back("seen_rows", -1, 1);
    return description;
}

Chunk makeChunk(const Block & header, const Array & keys, UInt64 first_payload, UInt64 seen_rows = 0)
{
    auto columns = header.cloneEmptyColumns();
    const size_t key_pos = header.getPositionByName("key");
    const size_t payload_pos = header.getPositionByName("payload");
    const size_t seen_rows_pos = header.getPositionByName("seen_rows");
    for (const auto & key : keys)
    {
        columns[key_pos]->insert(key);
        columns[payload_pos]->insert(first_payload++);
        columns[seen_rows_pos]->insert(seen_rows);
    }
    return Chunk(std::move(columns), keys.size());
}

}

TEST(GroupLimitSortedAlgorithm, SeenOnlyProgressIsBounded)
{
    /// Consumed seen rows count toward the block size, in the window of `DISTINCT` and in ranked windows.
    /// The ranks `[0, 1)` of `DISTINCT` and `LIMIT 1 BY` use the distinct merge path; any other window ranks
    /// rows one by one. `LIMIT BY` counts every row before spilling, so its seen counts are 64-bit.
    const auto limit_by_header = makeHeader(std::make_shared<DataTypeUInt64>());
    struct Case
    {
        SharedHeader header;
        UInt64 offset;
        UInt64 end;
        UInt64 seen_rows;
    };
    const std::vector<Case> cases{
        {makeHeader(), 0, 1, 1}, {limit_by_header, 0, 1, 7}, {limit_by_header, 1, 3, 2},
        {limit_by_header, 5, 7, std::numeric_limits<UInt64>::max()}};
    for (const auto & [header, offset, end, seen_rows] : cases)
    {
        SCOPED_TRACE(::testing::Message() << "window=[" << offset << ", " << end << "), seen_rows=" << seen_rows);
        Block output_header = *header;
        output_header.erase("seen_rows");
        GroupLimitSortedAlgorithm algorithm(
            {header}, std::make_shared<const Block>(output_header), runDescription(), offset, end, 2);
        IMergingAlgorithm::Inputs inputs(1);
        inputs[0].chunk = makeChunk(*header, {1u, 1u, 2u, 3u, 4u}, 0, seen_rows);
        algorithm.initialize(std::move(inputs));
        for (size_t step = 0; step < 2; ++step)
        {
            auto status = algorithm.merge();
            EXPECT_FALSE(status.is_finished);
            EXPECT_EQ(status.required_source, -1);
            EXPECT_EQ(status.chunk.getNumRows(), 0);
            EXPECT_EQ(status.chunk.getNumColumns(), 2);
        }
        auto refill = algorithm.merge();
        EXPECT_EQ(refill.required_source, 0);
        EXPECT_TRUE(algorithm.merge().is_finished);
    }
}

TEST(GroupLimitSortedAlgorithm, WholeChunkOfDistinctKeysForwardsColumns)
{
    const auto header = makeHeader();
    Block output_header = *header;
    output_header.erase("seen_rows");
    GroupLimitSortedAlgorithm algorithm(
        {header}, std::make_shared<const Block>(output_header), runDescription(), /*group_offset_=*/ 0, /*group_limit_end_=*/ 1, 64);
    IMergingAlgorithm::Inputs inputs(1);
    inputs[0].chunk = makeChunk(*header, {1u, 2u, 3u}, 100);
    const auto * payload = inputs[0].chunk.getColumns()[1].get();
    algorithm.initialize(std::move(inputs));
    auto status = algorithm.merge();
    EXPECT_EQ(status.required_source, 0);
    EXPECT_EQ(status.chunk.getNumRows(), 3);
    EXPECT_EQ(status.chunk.getNumColumns(), 2);
    EXPECT_EQ(status.chunk.getColumns()[1].get(), payload);
    EXPECT_TRUE(algorithm.merge().is_finished);
}
