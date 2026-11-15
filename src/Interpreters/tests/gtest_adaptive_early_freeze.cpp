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

/// Without a row estimate a table freezes only at the whole threshold, and only a block that can pass the threshold
/// is taken in slices, each as long as the keys that the table lacks, but at least 4,096 rows.
TEST(AdaptiveFreezeBounds, WithoutExpectedRowsOnlyTheThresholdFreezes)
{
    const AdaptiveFreezeBounds bounds(16384, 5461, /*expected_rows_=*/std::nullopt, /*received_rows_=*/8000, /*partitions_=*/2048);
    EXPECT_FALSE(bounds.reached(16383, 20000));
    EXPECT_TRUE(bounds.reached(16384, 20000));
    /// Neither the key count of an early freeze nor a trial without repeats freezes the table.
    EXPECT_FALSE(bounds.reached(6000, 8000));
    EXPECT_FALSE(bounds.reached(4096, 4096));

    EXPECT_FALSE(bounds.reachableWithin(6000, 8000, 10384));
    EXPECT_TRUE(bounds.reachableWithin(6000, 8000, 10385));
    EXPECT_FALSE(bounds.reachableWithin(0, 0, 4096));
    EXPECT_EQ(bounds.rowsToNextCheck(6000, 8000), 10384);
    EXPECT_EQ(bounds.rowsToNextCheck(16000, 30000), 4096);
}

/// A producer that expects 32,768 more rows than it received, 16 for each of 2,048 partitions, freezes early: at the
/// key count of an early freeze, and at the end of a trial whose keys did not repeat. The slices end at the trial and
/// at that key count.
TEST(AdaptiveFreezeBounds, EarlyFreezeThatPays)
{
    const AdaptiveFreezeBounds bounds(16384, 5461, /*expected_rows_=*/42768, /*received_rows_=*/10000, /*partitions_=*/2048);
    EXPECT_FALSE(bounds.reached(5460, 9000));
    EXPECT_TRUE(bounds.reached(5461, 9000));
    /// The trial: 4,096 rows with 3,073 keys or more.
    EXPECT_FALSE(bounds.reached(4000, 4095));
    EXPECT_TRUE(bounds.reached(3073, 4096));
    EXPECT_FALSE(bounds.reached(3072, 4096));

    /// The first block is taken in slices whatever its size, because the trial ends in it.
    EXPECT_TRUE(bounds.reachableWithin(0, 0, 100));
    EXPECT_EQ(bounds.rowsToNextCheck(0, 0), 4096);
    EXPECT_EQ(bounds.rowsToNextCheck(1000, 3000), 1096);
    /// Past the trial, a block is sliced only if it can pass the key count of an early freeze.
    EXPECT_FALSE(bounds.reachableWithin(3000, 6000, 2461));
    EXPECT_TRUE(bounds.reachableWithin(3000, 6000, 2462));
    EXPECT_EQ(bounds.rowsToNextCheck(3000, 6000), 4096);
    EXPECT_EQ(bounds.rowsToNextCheck(1000, 6000), 4461);
}

/// A producer that expects too few rows for an early freeze to pay still freezes at the key count of an early freeze
/// when its table is on track to the whole threshold: 5,600 keys after 8,000 rows gain 0.7 keys per row, so 15,406
/// more rows reach 16,384 keys and 15,405 do not. Its trial does not freeze it. A table past the key count of an early
/// freeze that did not freeze there is checked again at the whole threshold.
TEST(AdaptiveFreezeBounds, EarlyFreezeOnTrackToThreshold)
{
    const AdaptiveFreezeBounds on_track(16384, 5461, /*expected_rows_=*/23406, /*received_rows_=*/8000, /*partitions_=*/2048);
    EXPECT_TRUE(on_track.reached(5600, 8000));
    EXPECT_FALSE(on_track.reached(5460, 8000));
    EXPECT_FALSE(on_track.reached(4096, 4096));
    EXPECT_FALSE(on_track.reachableWithin(0, 0, 4096));
    EXPECT_TRUE(on_track.reachableWithin(0, 0, 5462));

    const AdaptiveFreezeBounds short_of_it(16384, 5461, /*expected_rows_=*/23405, /*received_rows_=*/8000, /*partitions_=*/2048);
    EXPECT_FALSE(short_of_it.reached(5600, 8000));
    EXPECT_TRUE(short_of_it.reached(16384, 20000));
    EXPECT_EQ(short_of_it.rowsToNextCheck(5600, 8000), 10784);
    EXPECT_EQ(short_of_it.rowsToNextCheck(5000, 8000), 4096);
}

/// Up to 64 producers the key count of an early freeze is the whole threshold, so only the trial freezes a table
/// early.
TEST(AdaptiveFreezeBounds, EarlyThresholdEqualToTheThreshold)
{
    const AdaptiveFreezeBounds bounds(16384, 16384, /*expected_rows_=*/100000, /*received_rows_=*/4096, /*partitions_=*/512);
    EXPECT_TRUE(bounds.reached(4096, 4096));
    EXPECT_FALSE(bounds.reached(16383, 30000));
    EXPECT_TRUE(bounds.reached(16384, 30000));
    EXPECT_EQ(bounds.rowsToNextCheck(8000, 10000), 8384);
}
