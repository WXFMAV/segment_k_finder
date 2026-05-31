-- ============================================================================
-- dist_k_seg_finder_agg.sql  (MaxCompute SQL version)
--
-- Goal:
--   Given N integer-point trajectories, each with M (possibly duplicated)
--   integer points, find for every trajectory whether it contains a run of
--   K consecutive integers, and if so emit one qualifying run's start.
--
-- Input table:   raw_data (group_id BIGINT, x BIGINT)
-- Output table:  seg_result (group_id BIGINT, start_x BIGINT)
--
-- Algorithm: halo-encoded bucket aggregation, no sort, O(M) per group,
--            O(1) per-row state. Identical mathematics to the SCOPE version
--            in dist_k_seg_finder_agg.script; only the syntax differs.
--
-- Step 0. Fan out each raw point x to 5 indicator rows:
--           temp_x = x       I_raw  = 1   (a raw point sits at t)
--           temp_x = x + 1   I_d1   = 1   (a raw point sits at t - 1)
--           temp_x = x - 1   I_p1   = 1   (a raw point sits at t + 1)
--           temp_x = x - K   I_k    = 1   (a raw point sits at t + K)
--           temp_x = x - K-1 I_kp1  = 1   (a raw point sits at t + K + 1)
--         Then GROUP BY (group_id, temp_x) and MAX (= logical OR).
--
-- Step 1. Bucket rows by  bucket = floor(temp_x / K). Anchor M = B*K + K - 1
--         (last cell of bucket B's left half). Per bucket compute:
--           has_M_raw : whether temp_x = M is raw
--           L_M       : MAX(temp_x) where I_raw=1 AND I_d1=0 AND temp_x <= M
--           R_M       : MIN of qualifying right endpoints in the 2K window
--
--         Hit condition:
--           has_M_raw = 1 AND L_M IS NOT NULL
--             AND (R_M IS NULL OR R_M - L_M + 1 >= K)
--
--         Each long-run start s is reported by exactly one bucket:
--         floor(s / K). See README.md for the proof.
--
-- Step 2. Per-group dedup: keep MIN(start_x) per group_id.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Parameters. Adjust K to your minimum run length.
-- (MaxCompute supports session variables via "SET", but inlining the literal
--  is the most portable form. Replace 10 below to change K.)
-- ---------------------------------------------------------------------------
SET odps.sql.allow.fullscan = true;

-- The single source of truth for K. Change here only.
-- Embedded as a literal because MaxCompute does not allow scalar parameters
-- inside DML in the same way SCOPE's #DECLARE does.
--
-- If you prefer SET-based config, write a small wrapper that does:
--     SET k = 10;
--     ... and template ${k} into this script via your scheduler.
--
-- Throughout the rest of this file, the constant 10 stands for K.

-- ---------------------------------------------------------------------------
-- Optional: synthesize the input table (mirrors RandomTrajectoryGenerator
-- from the SCOPE version). Comment out if you already have raw_data populated.
-- ---------------------------------------------------------------------------
-- Parameters: N = 10000 groups, M = 100 points per group, x in [0, 300).
-- Requires MaxCompute 2.0+ (lateral view + posexplode + sequence helpers).

-- DROP TABLE IF EXISTS raw_data;
-- CREATE TABLE IF NOT EXISTS raw_data (group_id BIGINT, x BIGINT);
--
-- INSERT OVERWRITE TABLE raw_data
-- SELECT  g.group_id,
--         CAST(FLOOR(RAND(g.group_id * 1000003 + p.pos) * 300) AS BIGINT) AS x
-- FROM    (
--             SELECT  pos + 1 AS group_id
--             FROM    (SELECT POSEXPLODE(SPLIT(SPACE(9999), ' ')) AS (pos, val)) t
--         ) g
-- LATERAL VIEW POSEXPLODE(SPLIT(SPACE(99), ' ')) p AS pos, val;

-- ---------------------------------------------------------------------------
-- Step 0a: 5-way indicator fan-out.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS exploded_tmp;
CREATE TABLE IF NOT EXISTS exploded_tmp (
    group_id BIGINT,
    temp_x   BIGINT,
    i_raw    BIGINT,
    i_d1     BIGINT,
    i_p1     BIGINT,
    i_k      BIGINT,
    i_kp1    BIGINT
);

INSERT OVERWRITE TABLE exploded_tmp
SELECT group_id, x          AS temp_x, 1 AS i_raw, 0 AS i_d1, 0 AS i_p1, 0 AS i_k, 0 AS i_kp1 FROM raw_data
UNION ALL
SELECT group_id, x + 1      AS temp_x, 0 AS i_raw, 1 AS i_d1, 0 AS i_p1, 0 AS i_k, 0 AS i_kp1 FROM raw_data
UNION ALL
SELECT group_id, x - 1      AS temp_x, 0 AS i_raw, 0 AS i_d1, 1 AS i_p1, 0 AS i_k, 0 AS i_kp1 FROM raw_data
UNION ALL
SELECT group_id, x - 10     AS temp_x, 0 AS i_raw, 0 AS i_d1, 0 AS i_p1, 1 AS i_k, 0 AS i_kp1 FROM raw_data
UNION ALL
SELECT group_id, x - 10 - 1 AS temp_x, 0 AS i_raw, 0 AS i_d1, 0 AS i_p1, 0 AS i_k, 1 AS i_kp1 FROM raw_data;

-- ---------------------------------------------------------------------------
-- Step 0b: per-(group, temp_x) merge of indicators (MAX = OR).
-- bucket = floor(temp_x / K), expressed via Euclidean modulo so that
-- negative temp_x lands in the correct bucket.
--   floor(t / K) = (t - ((t % K + K) % K)) / K
--   M_pos        = bucket * K + K - 1
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS combined_tmp;
CREATE TABLE IF NOT EXISTS combined_tmp (
    group_id BIGINT,
    temp_x   BIGINT,
    bucket   BIGINT,
    m_pos    BIGINT,
    i_raw    BIGINT,
    i_d1     BIGINT,
    i_p1     BIGINT,
    i_k      BIGINT,
    i_kp1    BIGINT
);

INSERT OVERWRITE TABLE combined_tmp
SELECT  group_id,
        temp_x,
        (temp_x - ((temp_x % 10 + 10) % 10)) / 10           AS bucket,
        (temp_x - ((temp_x % 10 + 10) % 10)) + 10 - 1       AS m_pos,
        MAX(i_raw)  AS i_raw,
        MAX(i_d1)   AS i_d1,
        MAX(i_p1)   AS i_p1,
        MAX(i_k)    AS i_k,
        MAX(i_kp1)  AS i_kp1
FROM    exploded_tmp
GROUP BY group_id,
         temp_x,
         (temp_x - ((temp_x % 10 + 10) % 10)) / 10,
         (temp_x - ((temp_x % 10 + 10) % 10)) + 10 - 1;

-- ---------------------------------------------------------------------------
-- Step 1: bucket-level aggregation. Compute has_M_raw / L_M / R_M.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS bucket_agg_tmp;
CREATE TABLE IF NOT EXISTS bucket_agg_tmp (
    group_id  BIGINT,
    bucket    BIGINT,
    m_pos     BIGINT,
    has_m_raw BIGINT,
    l_m       BIGINT,
    r_m       BIGINT
);

INSERT OVERWRITE TABLE bucket_agg_tmp
SELECT  group_id,
        bucket,
        MAX(m_pos)                                                          AS m_pos,
        MAX(IF(i_raw = 1 AND temp_x = m_pos, 1, 0))                         AS has_m_raw,
        MAX(IF(i_raw = 1 AND i_d1 = 0, temp_x, NULL))                       AS l_m,
        MIN(
            CASE
                WHEN temp_x = m_pos AND i_raw = 1 AND i_p1  = 0 THEN temp_x
                WHEN i_k    = 1     AND i_kp1 = 0               THEN temp_x + 10
                ELSE NULL
            END
        )                                                                   AS r_m
FROM    combined_tmp
GROUP BY group_id, bucket;

-- ---------------------------------------------------------------------------
-- Step 2: hit filter + per-group dedup (MIN start).
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS seg_result;
CREATE TABLE IF NOT EXISTS seg_result (
    group_id BIGINT,
    start_x  BIGINT
);

INSERT OVERWRITE TABLE seg_result
SELECT  group_id,
        MIN(l_m) AS start_x
FROM    bucket_agg_tmp
WHERE   has_m_raw = 1
        AND l_m IS NOT NULL
        AND (r_m IS NULL OR r_m - l_m + 1 >= 10)
GROUP BY group_id;


-- ###########################################################################
-- >>> VERIFY-BLOCK-BEGIN  (sort-based oracle; remove once trusted) <<<
-- Reference algorithm: sort each group's points, scan once, emit the first
-- length-K consecutive run start. Disagreement with seg_result is recorded
-- in seg_result_diff.
-- ###########################################################################

-- Sort + scan oracle, expressed via a window function. Does require sorting
-- per group, so do not rely on it for production-scale runs; it is here only
-- to validate the agg algorithm above.
DROP TABLE IF EXISTS seg_result_sort;
CREATE TABLE IF NOT EXISTS seg_result_sort (
    group_id BIGINT,
    start_x  BIGINT
);

INSERT OVERWRITE TABLE seg_result_sort
SELECT  group_id,
        MIN(run_start) AS start_x
FROM (
    SELECT  group_id,
            x,
            run_start,
            -- length of the run that starts at run_start and contains x
            x - run_start + 1 AS run_len_so_far,
            -- length of the maximal run = MAX over its members
            MAX(x - run_start + 1) OVER (PARTITION BY group_id, run_start) AS run_len
    FROM (
        SELECT  group_id,
                x,
                -- new run whenever the gap to the previous distinct x is > 1
                MAX(IF(prev_x IS NULL OR x - prev_x > 1, x, NULL))
                    OVER (PARTITION BY group_id ORDER BY x
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS run_start
        FROM (
            SELECT  group_id,
                    x,
                    LAG(x) OVER (PARTITION BY group_id ORDER BY x) AS prev_x
            FROM (
                SELECT DISTINCT group_id, x FROM raw_data
            ) d
        ) lagged
    ) tagged
) runs
WHERE run_len >= 10
GROUP BY group_id;

-- Diff: full outer join on group_id. Tag rows with a status; rows tagged OK
-- are filtered out. A `DIFF_RUN` row means both algorithms hit but reported
-- starts that are at least K apart (i.e. different runs); same-run picks
-- with |delta| < K are accepted because both algorithms are allowed to
-- return any qualifying start within the same run.
DROP TABLE IF EXISTS seg_result_diff;
CREATE TABLE IF NOT EXISTS seg_result_diff (
    group_id   BIGINT,
    start_agg  BIGINT,
    start_sort BIGINT,
    status     STRING
);

INSERT OVERWRITE TABLE seg_result_diff
SELECT  group_id,
        start_agg,
        start_sort,
        status
FROM (
    SELECT  COALESCE(a.group_id, b.group_id) AS group_id,
            a.start_x  AS start_agg,
            b.start_x  AS start_sort,
            CASE
                WHEN a.group_id IS NULL                       THEN 'ONLY_SORT'
                WHEN b.group_id IS NULL                       THEN 'ONLY_AGG'
                WHEN ABS(a.start_x - b.start_x) >= 10         THEN 'DIFF_RUN'
                ELSE 'OK'
            END AS status
    FROM        seg_result      a
    FULL OUTER JOIN seg_result_sort b
    ON          a.group_id = b.group_id
) j
WHERE status <> 'OK';

-- >>> VERIFY-BLOCK-END <<<
-- ###########################################################################
