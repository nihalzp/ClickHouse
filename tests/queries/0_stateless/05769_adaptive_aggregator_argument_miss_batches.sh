#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The frozen kernel of the adaptive aggregator takes a block of argument records in slices of 8,192 rows and stages
# the misses of each slice. Each cell aggregates two streams of 300,000 rows in blocks of 65,536 rows, eight slices
# each, with a count among other aggregates, whose records carry arguments. The tables freeze at 128 keys, within
# their first block, so every later block goes through the frozen kernel. The result matches the same query with
# the feature off.
# `max_threads_min_free_memory_per_thread = 0` keeps both threads whatever the free memory, so each stream has a
# table of its own. The thresholds are pinned because the runner randomizes them, and every cell runs in its own
# clickhouse-local process, so the counters in `system.events` belong to it alone.
cell()
{
    local key=$1 fact=$2
    local query="SELECT count(), sum(c), sum(s), sum(cityHash64(k, c, s)) FROM (
        SELECT ${key} AS k, count() AS c, sum(number % 7) AS s
        FROM (SELECT number FROM numbers(300000) UNION ALL SELECT number + 10000000 FROM numbers(300000))
        GROUP BY k SETTINGS __SETTINGS__)"
    local off=${query//__SETTINGS__/enable_adaptive_aggregator = 0}
    local on=${query//__SETTINGS__/enable_adaptive_aggregator = 1}
    $CLICKHOUSE_LOCAL --query "
SET max_threads = 2, max_threads_min_free_memory_per_thread = 0, max_block_size = 65536;
SET adaptive_aggregator_freeze_threshold = 128, adaptive_aggregator_freeze_threshold_bytes = 0;
SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 0;
SET collect_hash_table_stats_during_aggregation = 0, optimize_aggregation_in_order = 0;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

SELECT 'matches the baseline', (${off}) = (${on});
WITH
    (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationStagedRecords') AS staged,
    (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationProbeBypasses') AS bypasses
SELECT ${fact};
"
}

# Keys that never repeat: no row hits a frozen table, so both tables stop probing after their first 65,536 frozen
# rows, and the later blocks take the kernel without the probe. Every row past the first 128 of a stream is staged.
cell 'number' "'slices without the probe', bypasses = 2 AND staged >= 2 * (300000 - 8192)"

# Every even row takes one of 50 keys, which the tables learn at once, and every odd row a key of its own. Half of
# the rows hit a frozen table, so the tables keep probing, and the odd rows past the freeze are staged.
cell 'if(number % 2 = 0, number % 100, number)' "'slices with the probe', bypasses = 0 AND staged >= 2 * (150000 - 8192)"
