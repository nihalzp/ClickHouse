-- The ratio enables external `LIMIT BY` by default, and older compatibility versions keep it disabled.
SET max_bytes_before_external_limit_by = 0;
SELECT getSetting('max_bytes_ratio_before_external_limit_by');
SELECT getSetting('max_bytes_ratio_before_external_limit_by') SETTINGS compatibility = '26.9';
SELECT getSetting('max_bytes_ratio_before_external_limit_by') SETTINGS compatibility = '26.10';

-- A server memory limit supplies the ratio's threshold, so the default selects the spilling transform.
SELECT count() FROM (EXPLAIN PIPELINE SELECT number % 10 AS k FROM numbers(100) LIMIT 2 BY k)
WHERE explain LIKE '%ExternalLimitByTransform%';
SELECT count() FROM (EXPLAIN PIPELINE SELECT number % 10 AS k FROM numbers(100) LIMIT 2 BY k SETTINGS compatibility = '26.9')
WHERE explain LIKE '%ExternalLimitByTransform%';
