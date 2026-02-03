DROP TABLE IF EXISTS test_explicit_pk_nested_tuple;

CREATE TABLE test_explicit_pk_nested_tuple
(
    a UInt32,
    b UInt32,
    c UInt32
)
ENGINE = MergeTree
PRIMARY KEY (a, (c, (b, a), (a, b)))
SETTINGS flatten_and_deduplicate_primary_key_expressions = 1;

SELECT 'PRIMARY KEY (a, (c, (b, a), (a, b)))';
SHOW CREATE TABLE test_explicit_pk_nested_tuple;

DROP TABLE IF EXISTS test_explicit_pk_tuple_col;

CREATE TABLE test_explicit_pk_tuple_col
(
    a Tuple(UInt64, UInt32),
    b UInt32,
    c UInt32
)
ENGINE = MergeTree
PRIMARY KEY (a, (c, (b, a), (a, b)))
SETTINGS flatten_and_deduplicate_primary_key_expressions = 1;

SELECT 'PRIMARY KEY (a, (c, (b, a), (a, b)))';
SHOW CREATE TABLE test_explicit_pk_tuple_col;


DROP TABLE IF EXISTS test_pk_and_order_by_dedup;

CREATE TABLE test_pk_and_order_by_dedup
(
    a Tuple(UInt64, UInt32),
    b UInt32,
    c UInt32
)
ENGINE = MergeTree
PRIMARY KEY (a, (b, a), (a, b))
ORDER BY (a, (b, a), (a, b), c)
SETTINGS flatten_and_deduplicate_primary_key_expressions = 1;

SELECT 'PRIMARY KEY (a, (b, a), (a, b))';
SELECT 'ORDER BY (a, (b, a), (a, b), c)';
SHOW CREATE TABLE test_pk_and_order_by_dedup;

DROP TABLE IF EXISTS test_order_by_tuple_func;

CREATE TABLE test_order_by_tuple_func
(
    a UInt32,
    b UInt32,
    c UInt32
)
ENGINE = MergeTree
ORDER BY tuple(a, tuple(c, b, a), a)
SETTINGS flatten_and_deduplicate_primary_key_expressions = 1;

SELECT 'ORDER BY tuple(a, tuple(c, b, a), a)';
SHOW CREATE TABLE test_order_by_tuple_func;


DROP TABLE IF EXISTS test_order_by_func_and_nested;

CREATE TABLE test_order_by_func_and_nested
(
    a UInt32,
    b UInt32,
    c UInt32
)
ENGINE = MergeTree
ORDER BY (sipHash64((a, b)), (a, (c, (b, a))))
SETTINGS flatten_and_deduplicate_primary_key_expressions = 1;

SELECT 'ORDER BY (sipHash64((a, b)), (a, (c, (b, a))))';
SHOW CREATE TABLE test_order_by_func_and_nested;


DROP TABLE IF EXISTS test_order_by_mixed_directions;

CREATE TABLE test_order_by_mixed_directions
(
    a UInt32,
    b UInt32,
    c UInt32
)
ENGINE = MergeTree
ORDER BY (a DESC, (b, a), c DESC)
SETTINGS allow_experimental_reverse_key = 1, flatten_and_deduplicate_primary_key_expressions = 1;

SELECT 'ORDER BY (a DESC, (b, a), c DESC)';
SHOW CREATE TABLE test_order_by_mixed_directions;

DROP TABLE IF EXISTS test_order_by_nested_direction;

CREATE TABLE test_order_by_nested_direction
(
    a UInt32,
    b UInt32,
    c UInt32
)
ENGINE = MergeTree
ORDER BY (a, (b, c) DESC)
SETTINGS allow_experimental_reverse_key = 1, flatten_and_deduplicate_primary_key_expressions = 1;

SELECT 'ORDER BY (a, (b, c) DESC)';
SHOW CREATE TABLE test_order_by_nested_direction;

DROP TABLE IF EXISTS test_order_by_deep_nested_direction;

CREATE TABLE test_order_by_deep_nested_direction
(
    a UInt32,
    b UInt32,
    c UInt32,
    d UInt32
)
ENGINE = MergeTree
ORDER BY (a, (b, (c, d)) DESC)
SETTINGS allow_experimental_reverse_key = 1, flatten_and_deduplicate_primary_key_expressions = 1;

SELECT 'ORDER BY (a, (b, (c, d)) DESC)';
SHOW CREATE TABLE test_order_by_deep_nested_direction;

DROP TABLE IF EXISTS test_order_by_outer_desc;

CREATE TABLE test_order_by_outer_desc
(
    a UInt32,
    b UInt32,
    c UInt32
)
ENGINE = MergeTree
ORDER BY (a, (b, a), c) DESC
SETTINGS allow_experimental_reverse_key = 1, flatten_and_deduplicate_primary_key_expressions = 1;

SELECT 'ORDER BY (a, (b, a), c) DESC';
SHOW CREATE TABLE test_order_by_outer_desc;

DROP TABLE IF EXISTS test_show_keys_system_tables;

CREATE TABLE test_show_keys_system_tables
(
    a UInt32,
    b UInt32,
    c UInt32
)
ENGINE = MergeTree
ORDER BY (a, (c, (b, a), (a, b)))
SETTINGS flatten_and_deduplicate_primary_key_expressions = 1;

SELECT 'ORDER BY (a, (c, (b, a), (a, b)))';
SHOW CREATE TABLE test_show_keys_system_tables;
SELECT sorting_key, primary_key FROM system.tables WHERE name = 'test_show_keys_system_tables' FORMAT TSVRaw;

DROP TABLE IF EXISTS test_attach_no_rewrite;

CREATE TABLE test_attach_no_rewrite
(
    a UInt32,
    b UInt32,
    c UInt32
)
ENGINE = MergeTree
ORDER BY (a, (c, (b, a), (a, b)));

SELECT 'ORDER BY (a, (c, (b, a), (a, b)))';
SHOW CREATE TABLE test_attach_no_rewrite;

ALTER TABLE test_attach_no_rewrite MODIFY SETTING flatten_and_deduplicate_primary_key_expressions = 1; -- { serverError READONLY_SETTING }

SELECT 'ORDER BY (a, (c, (b, a), (a, b)))';
SHOW CREATE TABLE test_attach_no_rewrite;

DETACH TABLE test_attach_no_rewrite;
ATTACH TABLE test_attach_no_rewrite;

SELECT 'ORDER BY (a, (c, (b, a), (a, b)))';
SHOW CREATE TABLE test_attach_no_rewrite;

DROP TABLE IF EXISTS test_bad_pk_not_prefix;

CREATE TABLE test_bad_pk_not_prefix
(
    a UInt32,
    b UInt32,
    c UInt32
)
ENGINE = MergeTree
PRIMARY KEY (a, (c, a))
ORDER BY (a, (b, a))
SETTINGS flatten_and_deduplicate_primary_key_expressions = 1; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS test_pk_dedup_only;

CREATE TABLE test_pk_dedup_only
(
    a UInt32,
    b UInt32,
    c UInt32
)
ENGINE = MergeTree
PRIMARY KEY (a, b, a, b, c)
SETTINGS flatten_and_deduplicate_primary_key_expressions = 1;

SELECT 'PRIMARY KEY (a, b, a, b, c)';
SHOW CREATE TABLE test_pk_dedup_only;


DROP TABLE IF EXISTS test_single_expr_pk;

CREATE TABLE test_single_expr_pk
(
    a UInt32
)
ENGINE = MergeTree
PRIMARY KEY (a, (a, a))
SETTINGS flatten_and_deduplicate_primary_key_expressions = 1;

SELECT 'PRIMARY KEY (a, (a, a))';
SHOW CREATE TABLE test_single_expr_pk;

DROP TABLE IF EXISTS test_alter_no_rewrite;

CREATE TABLE test_alter_no_rewrite
(
    a UInt32
)
ENGINE = MergeTree
ORDER BY a
SETTINGS flatten_and_deduplicate_primary_key_expressions = 1;

ALTER TABLE test_alter_no_rewrite
    ADD COLUMN d UInt32,
    MODIFY ORDER BY (a, (d, (d, d)));

SELECT 'ORDER BY (a, (d, (d, d)))';
SHOW CREATE TABLE test_alter_no_rewrite;
