#include <gtest/gtest.h>

#include <limits>
#include <numeric>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/sortBlock.h>
#include <Common/typeid_cast.h>

using namespace DB;

/// With stable sorting, `sortBlockAndLimitGroupRows` retains the first `group_limit` input rows of each sort
/// key. The `arrival` payload records input positions so the tests can identify the retained rows.
namespace
{

ColumnWithTypeAndName numbers(const String & name, const std::vector<UInt64> & values)
{
    auto column = ColumnUInt64::create();
    for (const auto value : values)
        column->insertValue(value);
    return {std::move(column), std::make_shared<DataTypeUInt64>(), name};
}

ColumnWithTypeAndName strings(const String & name, const std::vector<String> & values)
{
    auto column = ColumnString::create();
    for (const auto & value : values)
        column->insertData(value.data(), value.size());
    return {std::move(column), std::make_shared<DataTypeString>(), name};
}

ColumnWithTypeAndName arrivals(size_t rows)
{
    std::vector<UInt64> values(rows);
    std::iota(values.begin(), values.end(), 0);
    return numbers("arrival", values);
}

/// `left` and `right` select "zero", "one" or "two" through the same `indexes`, while `k` is constant.
Block blockWithSharedIndexes(const std::vector<UInt64> & indexes)
{
    const auto indexes_column = numbers("indexes", indexes).column;
    auto left = strings("left", {"zero", "one", "two"});
    auto right = strings("right", {"0", "1", "2"});
    left.column = ColumnReplicated::create(left.column, indexes_column);
    right.column = ColumnReplicated::create(right.column, indexes_column);
    ColumnWithTypeAndName key{
        ColumnConst::create(ColumnUInt64::create(1, 7), indexes.size()), std::make_shared<DataTypeUInt64>(), "k"};
    return {std::move(key), std::move(left), std::move(right), arrivals(indexes.size())};
}

void checkSharedIndexes(const Block & block)
{
    const auto * left = typeid_cast<const ColumnReplicated *>(block.getByName("left").column.get());
    const auto * right = typeid_cast<const ColumnReplicated *>(block.getByName("right").column.get());
    ASSERT_NE(left, nullptr);
    ASSERT_NE(right, nullptr);
    EXPECT_EQ(left->getIndexesColumn(), right->getIndexesColumn());
}

SortDescription ascending(const String & name)
{
    SortDescription description;
    description.emplace_back(name, 1, 1);
    return description;
}

}

TEST(SortBlockAndLimitGroupRows, GroupLimitPreservesSharedIndexes)
{
    /// The indexes select "two" three times, so a limit of two drops one of its rows when the block is sorted
    /// by the replicated column, and a constant key cuts the block to the limit. The results keep sharing the
    /// indexes.
    const std::vector<UInt64> indexes{2, 0, 2, 1, 0, 2};
    const std::vector<String> left{"two", "zero", "two", "one", "zero", "two"};
    const std::vector<String> right{"2", "0", "2", "1", "0", "2"};
    struct Case
    {
        size_t group_limit;
        std::vector<UInt64> sorted_arrival;
        size_t constant_rows;
    };
    const std::vector<Case> cases{
        {1, {3, 0, 1}, 1}, {2, {3, 0, 2, 1, 4}, 2}, {std::numeric_limits<size_t>::max(), {3, 0, 2, 5, 1, 4}, 6}};
    auto expect_rows = [&](const Block & block, const std::vector<UInt64> & arrival)
    {
        ASSERT_EQ(block.rows(), arrival.size());
        for (size_t row = 0; row < arrival.size(); ++row)
        {
            EXPECT_EQ(block.getByName("arrival").column->getUInt(row), arrival[row]);
            EXPECT_EQ(block.getByName("left").column->getDataAt(row), left[arrival[row]]);
            EXPECT_EQ(block.getByName("right").column->getDataAt(row), right[arrival[row]]);
        }
        checkSharedIndexes(block);
    };
    for (const auto & [group_limit, sorted_arrival, constant_rows] : cases)
    {
        SCOPED_TRACE(group_limit);
        Block block = blockWithSharedIndexes(indexes);
        sortBlockAndLimitGroupRows(block, ascending("left"), group_limit, IColumn::PermutationSortStability::Stable);
        expect_rows(block, sorted_arrival);

        Block constant = blockWithSharedIndexes(indexes);
        sortBlockAndLimitGroupRows(constant, ascending("k"), group_limit, IColumn::PermutationSortStability::Stable);
        std::vector<UInt64> constant_arrival(constant_rows);
        std::iota(constant_arrival.begin(), constant_arrival.end(), 0);
        expect_rows(constant, constant_arrival);
    }
}
