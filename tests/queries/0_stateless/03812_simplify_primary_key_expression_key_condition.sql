-- Tags: no-replicated-database, no-parallel-replicas, no-parallel, no-random-merge-tree-settings
-- EXPLAIN output may differ

-- { echo }

DROP TABLE IF EXISTS pk_complex;

CREATE TABLE pk_complex
(
    a UInt64,
    b UInt64,
    c UInt64,
    d UInt64
)
ENGINE = MergeTree
ORDER BY ((a, b, (a, b, c, d), (cityHash64(a), c), (cityHash64(a), cityHash64(a))))
SETTINGS index_granularity = 1, flatten_and_deduplicate_primary_key_expressions = 1;

INSERT INTO pk_complex SELECT number % 2, number % 3, number % 4, number % 5 FROM numbers(1000);

EXPLAIN indexes = 1
SELECT * FROM pk_complex WHERE a == 1 AND b = 2 AND c = 3 AND d = 4;

SELECT
    name,
    primary_key,
    sorting_key,
    partition_key
FROM system.tables
WHERE database = currentDatabase() AND name = 'pk_complex'
FORMAT Vertical;


DROP TABLE IF EXISTS pk_tuple;

CREATE TABLE pk_tuple
(
    a UInt64,
    b UInt64,
    c UInt64,
    d UInt64
)
ENGINE = MergeTree
ORDER BY (a, (b, c))
SETTINGS index_granularity = 1, flatten_and_deduplicate_primary_key_expressions = 1;

INSERT INTO pk_tuple SELECT number % 2, number % 3, number % 4, number % 5 FROM numbers(1000);

EXPLAIN indexes = 1
SELECT * FROM pk_tuple WHERE a == 1 AND b = 2 AND c = 3 AND d = 4;

SELECT
    name,
    primary_key,
    sorting_key,
    partition_key
FROM system.tables
WHERE database = currentDatabase() AND name = 'pk_tuple'
FORMAT Vertical;
