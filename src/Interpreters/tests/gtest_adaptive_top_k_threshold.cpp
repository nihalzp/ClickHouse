#include <gtest/gtest.h>

#include <limits>
#include <optional>
#include <vector>

#include <Interpreters/AdaptiveAggregation/AdaptiveTopKThreshold.h>

using namespace DB;

namespace
{

void offer(AdaptiveTopKThreshold & threshold, UInt64 rank)
{
    const std::vector<UInt64> ranks{rank};
    threshold.offer(ranks);
}

}

/// A descending threshold without a known floor starts at 0, which drops nothing, and takes the worst of the three
/// best ranks once three are offered. With a limit this small every rank kept past it selects again.
TEST(AdaptiveTopKThreshold, DescendingWithoutKnownFloor)
{
    AdaptiveTopKThreshold threshold(3, /*ascending_=*/false, /*best_possible_rank_=*/std::nullopt);
    EXPECT_EQ(threshold.current(), 0);
    offer(threshold, 5);
    offer(threshold, 9);
    EXPECT_EQ(threshold.current(), 0);
    offer(threshold, 2);
    EXPECT_EQ(threshold.current(), 2);
    offer(threshold, 7);
    EXPECT_EQ(threshold.current(), 5);
    offer(threshold, 8);
    EXPECT_EQ(threshold.current(), 7);
    /// Ranks no better than the threshold are dropped.
    offer(threshold, 1);
    offer(threshold, 6);
    EXPECT_EQ(threshold.current(), 7);
    EXPECT_FALSE(threshold.settled());
}

/// A descending threshold starts at the known floor, a rank that three groups are known to reach. Offered ranks that
/// do not exceed the floor are dropped, and the threshold rises only once three ranks above the floor are offered.
TEST(AdaptiveTopKThreshold, DescendingStartsAtTheKnownFloor)
{
    AdaptiveTopKThreshold threshold(3, /*ascending_=*/false, /*best_possible_rank_=*/std::nullopt, /*known_floor_=*/4);
    EXPECT_EQ(threshold.current(), 4);
    offer(threshold, 5);
    offer(threshold, 9);
    /// Neither a rank below the floor nor a tie with it counts towards the three best.
    offer(threshold, 2);
    offer(threshold, 4);
    EXPECT_EQ(threshold.current(), 4);
    offer(threshold, 7);
    EXPECT_EQ(threshold.current(), 5);
    offer(threshold, 8);
    EXPECT_EQ(threshold.current(), 7);
}

/// When fewer than three ranks exceed the known floor, the floor stays the threshold: it is the third best rank
/// itself.
TEST(AdaptiveTopKThreshold, KnownFloorStaysWhenFewRanksExceedIt)
{
    AdaptiveTopKThreshold threshold(3, /*ascending_=*/false, /*best_possible_rank_=*/std::nullopt, /*known_floor_=*/100);
    const std::vector<UInt64> ranks{100, 100, 250, 99, 300, 1};
    threshold.offer(ranks);
    EXPECT_EQ(threshold.current(), 100);
}

/// An ascending threshold starts at the largest rank and settles once it reaches the best possible rank.
TEST(AdaptiveTopKThreshold, AscendingSettlesAtTheBestPossibleRank)
{
    AdaptiveTopKThreshold threshold(2, /*ascending_=*/true, /*best_possible_rank_=*/1);
    EXPECT_EQ(threshold.current(), std::numeric_limits<UInt64>::max());
    offer(threshold, 3);
    EXPECT_FALSE(threshold.settled());
    offer(threshold, 1);
    EXPECT_EQ(threshold.current(), 3);
    EXPECT_FALSE(threshold.settled());
    offer(threshold, 1);
    EXPECT_EQ(threshold.current(), 1);
    EXPECT_TRUE(threshold.settled());
}
