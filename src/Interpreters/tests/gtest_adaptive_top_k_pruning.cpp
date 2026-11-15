#include <gtest/gtest.h>

#include <array>
#include <initializer_list>
#include <limits>
#include <tuple>

#include <Interpreters/AdaptiveAggregation/AdaptiveTopKPruning.h>
#include <Interpreters/AggregatedDataVariants.h>

using namespace DB;

namespace
{

constexpr UInt64 unbounded = std::numeric_limits<UInt64>::max();

/// A hash of bin `bin` of bucket `bucket`: the bins sit on the hash bits from bit 14 up.
UInt64 hashOf(size_t bucket, size_t bin)
{
    return (bucket * adaptive_count_bins_per_bucket + bin) << 14;
}

/// The finished bins of a producer that counted `rows`, each a bucket, a bin of the bucket and a number of rows.
AdaptiveTopKPruning::ProducerBins producerBins(std::initializer_list<std::tuple<size_t, size_t, UInt64>> rows)
{
    AdaptiveTopKCountBins counts;
    counts.allocate();
    for (const auto & [bucket, bin, count] : rows)
        counts.add(hashOf(bucket, bin), count);
    return AdaptiveTopKPruning::finalizeProducerCounts(std::move(counts));
}

}

/// The bound of a bucket is the sum of the producers' largest bins in it, which may be different bins, so it is at
/// least the bound of every bin of the bucket.
TEST(AdaptiveTopKPruning, BucketBoundSumsTheLargestBins)
{
    AdaptiveTopKPruning pruning;
    pruning.addProducerBins(producerBins({{3, 5, 100}, {3, 9, 40}, {7, 1, 8}}));
    pruning.addProducerBins(producerBins({{3, 9, 70}, {7, 2, 6}}));

    EXPECT_EQ(pruning.bucketBound(3), 170);
    EXPECT_EQ(pruning.bucketBound(7), 14);
    EXPECT_EQ(pruning.bucketBound(0), 0);

    const auto bounds = pruning.boundsForBucket(3);
    EXPECT_EQ(bounds[5], 100);
    EXPECT_EQ(bounds[9], 110);
}

/// A bin that takes more rows than its counter holds saturates and bounds nothing, so its bucket has no bound either.
/// The other buckets keep theirs.
TEST(AdaptiveTopKPruning, SaturatedBinLeavesItsBucketUnbounded)
{
    AdaptiveTopKPruning pruning;
    pruning.addProducerBins(producerBins({{3, 5, 70000}, {7, 1, 8}}));
    pruning.addProducerBins(producerBins({{3, 5, 10}, {7, 1, 2}}));

    EXPECT_EQ(pruning.boundsForBucket(3)[5], unbounded);
    EXPECT_EQ(pruning.bucketBound(3), unbounded);
    EXPECT_EQ(pruning.bucketBound(7), 10);
}

/// A table that the merge counts adds its largest count of each bucket to the bucket's bound. A table that comes
/// without them leaves every bucket unbounded, because its groups are counted only while a bucket merges.
TEST(AdaptiveTopKPruning, TableWithoutLargestCountsLeavesEveryBucketUnbounded)
{
    AdaptiveTopKPruning pruning;
    pruning.addProducerBins(producerBins({{3, 5, 100}}));

    AggregatedDataVariants counted;
    std::array<UInt64, ADAPTIVE_AGGREGATION_NUM_BUCKETS> largest_counts{};
    largest_counts[3] = 500;
    pruning.addMergeCountedTable(counted, &largest_counts);
    EXPECT_EQ(pruning.bucketBound(3), 600);
    EXPECT_EQ(pruning.bucketBound(0), 0);

    AggregatedDataVariants uncounted;
    pruning.addMergeCountedTable(uncounted, nullptr);
    EXPECT_EQ(pruning.bucketBound(3), unbounded);
    EXPECT_EQ(pruning.bucketBound(0), unbounded);
}
