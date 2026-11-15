#include <Interpreters/AdaptiveAggregation/AdaptiveFreezeBounds.h>

#include <gtest/gtest.h>

#include <tuple>

using namespace DB;

/// Up to 64 producers every learning table takes the whole freeze threshold, and more producers share 64 thresholds:
/// with 16,384 keys, 65 producers take 16,384 * 64 / 65 = 16,131 keys each and 192 producers 5,461.
TEST(AdaptiveEarlyFreeze, ThresholdIsSharedAbove64Producers)
{
    for (const auto & [threshold, producers, expected] : {
             std::tuple<size_t, size_t, size_t>{16384, 1, 16384},
             {16384, 64, 16384},
             {16384, 65, 16131},
             {16384, 128, 8192},
             {16384, 192, 5461},
             {128, 192, 42},
             {0, 192, 0}})
        EXPECT_EQ(adaptiveEarlyFreezeThreshold(threshold, producers), expected) << threshold << " keys, " << producers << " producers";
}

/// The trial covers the first 4,096 rows of a table, and finds no repeats when fewer than one row in four repeated a
/// key. After 4,096 rows that is up to 1,023 repeats, so 3,073 keys or more.
TEST(AdaptiveEarlyFreeze, TrialFindsNoRepeats)
{
    for (const auto & [rows, keys, expected] : {
             std::tuple<size_t, size_t, bool>{4095, 4095, false},
             {4096, 4096, true},
             {4096, 3073, true},
             {4096, 3072, false},
             {4096, 100, false},
             {8192, 6145, true},
             {8192, 6144, false}})
        EXPECT_EQ(adaptiveLearningFindsNoRepeats(rows, keys), expected) << rows << " rows, " << keys << " keys";
}

/// An early freeze pays while the producer expects at least 16 more rows per partition: with 2,048 partitions that
/// is 32,768 rows beyond those it received.
TEST(AdaptiveEarlyFreeze, PaysWhileRowsPerPartitionFollow)
{
    for (const auto & [expected_rows, received_rows, partitions, expected] : {
             std::tuple<size_t, size_t, size_t, bool>{42768, 10000, 2048, true},
             {42767, 10000, 2048, false},
             {32768, 0, 2048, true},
             {5000, 10000, 2048, false},
             {8192, 0, 512, true},
             {8191, 0, 512, false}})
        EXPECT_EQ(adaptiveEarlyFreezePays(expected_rows, received_rows, partitions), expected)
            << expected_rows << " expected, " << received_rows << " received, " << partitions << " partitions";
}

/// A table is on track to the threshold when its keys plus those of the rows to come, at the rate of new keys per
/// row so far, reach it. A table of 5,600 keys after 8,000 rows gains 0.7 keys per row: 15,406 more rows bring it to
/// 16,384 keys and 15,405 leave it one key short. A producer that received every row it expects gains nothing.
TEST(AdaptiveEarlyFreeze, TableOnTrackToThreshold)
{
    for (const auto & [keys, learned_rows, expected_rows, received_rows, threshold, expected] : {
             std::tuple<size_t, size_t, size_t, size_t, size_t, bool>{5600, 8000, 38000, 8000, 16384, true},
             {5600, 8000, 23406, 8000, 16384, true},
             {5600, 8000, 23405, 8000, 16384, false},
             {5600, 8000, 8000, 8000, 16384, false},
             {5600, 8000, 4000, 8000, 16384, false},
             {16384, 20000, 20000, 20000, 16384, true},
             /// A table that learns again after a write to disk counts only the rows it learned from.
             {5600, 8000, 123406, 108000, 16384, true}})
        EXPECT_EQ(adaptiveLearningReachesThreshold(keys, learned_rows, expected_rows, received_rows, threshold), expected)
            << keys << " keys after " << learned_rows << " rows, " << expected_rows << " expected, " << received_rows << " received";
}
