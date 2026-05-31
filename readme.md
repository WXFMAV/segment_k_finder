# Sortless K-Run Detection (Halo-Encoded Bucket Aggregation)

> A general-purpose distributed-SQL algorithm for finding length-≥K consecutive
> integer runs in massive sets of integer trajectories — without ever sorting
> the points. Works on any engine that supports `UNION ALL`, `GROUP BY`, and
> conditional aggregates (SCOPE / Cosmos, MaxCompute / ODPS, Spark SQL,
> Hive, BigQuery, Trino, ClickHouse, ...).

[![Algorithm](https://img.shields.io/badge/Algorithm-Halo%20Encoded%20Bucket%20Aggregation-blue)]()
[![Status](https://img.shields.io/badge/Status-Experimental-orange)]()
[![License](https://img.shields.io/badge/License-MIT-green)]()

---

## TL;DR

Given `N` integer-point trajectories, each containing `M` (possibly duplicated)
integer points, decide for every trajectory whether it contains a run of `K`
consecutive integers, and if so emit one such run's start.

| Input | Output |
| --- | --- |
| `(group_id, x)` | `(group_id, start_x)` |

The classic SQL recipe is **gaps-and-islands** — `ROW_NUMBER()` / `LAG()` over
a sort-by-`x`, costing `O(M log M)` per group. This repo implements a
**sortless** variant: a `UNION ALL` fan-out of indicator rows + two
`GROUP BY` stages.
Time is `O(M)` per group, per-row state is `O(1)`, and the entire pipeline is
embarrassingly parallel because each `(group_id, bucket)` pair is independent.

---

## File map

| File | Engine | Role |
| --- | --- | --- |
| [dist_k_seg_finder_agg.script](dist_k_seg_finder_agg.script) | SCOPE | End-to-end script: data generator + agg algorithm + sort-based oracle + diff |
| [dist_k_seg_finder_agg.sql](dist_k_seg_finder_agg.sql) | MaxCompute SQL | Same algorithm in standard SQL; portable to Spark / Hive / Trino with minor tweaks |
| `README.md` | — | You are here. |

Both implementations are mathematically identical and ship with a sort-based
oracle so any disagreement is easy to spot.

---

## Algorithm

### Step 0 — Halo fan-out (5 indicators)

Each raw point `x` is exploded into 5 indicator rows at offsets
`{0, +1, -1, -K, -(K+1)}`:

| `temp_x` | indicator | meaning |
| --- | --- | --- |
| `x`       | `I_raw = 1` | a raw point sits at `t` |
| `x + 1`   | `I_d1 = 1`  | a raw point sits at `t - 1` |
| `x - 1`   | `I_p1 = 1`  | a raw point sits at `t + 1` |
| `x - K`   | `I_k = 1`   | a raw point sits at `t + K` |
| `x - K-1` | `I_kp1 = 1` | a raw point sits at `t + K + 1` |

Then `GROUP BY (group_id, temp_x)` and `MAX` (logical OR) the indicators.
After this step, every position `t` on the integer line knows the raw status
of its small neighborhood `{t, t±1, t+K, t+K+1}` in `O(1)` columns.

### Step 1 — Bucket aggregation (`bucket = ⌊temp_x / K⌋`)

Group rows into buckets of width `K`. For each bucket `B`, define its anchor
`M = B*K + K - 1` (the last cell of the bucket's left half).
Per bucket compute three values:

```
has_M_raw = MAX( I_raw=1 AND temp_x = M )
L_M       = MAX( temp_x  : I_raw=1 AND I_d1=0  AND temp_x ≤ M )
R_M       = MIN( endpoint candidates inside the 2K window from M )
```

**Key lemma.** A length-≥K consecutive run `[s, e]` has its start `s` lying
in bucket `B`'s left half ⇔ the anchor `M` lies inside that run ⇔
`I_raw(M) = 1` and the maximal raw run containing `M` has length ≥ K.

**Why no inner-point check is needed.** The `MAX` defining `L_M` automatically
lands on the start of the run containing `M`: any later position in the run
has `I_d1 = 1` (its left neighbor is also raw) and is excluded. Symmetric
argument for `R_M`.

**Unique reporting right.** If a run's start `s < B*K`, then every cell in
`[B*K, M]` is raw, so no `t` in bucket `B` satisfies `I_d1(t) = 0`, so
`L_M = NULL`, so bucket `B` stays silent. Bucket `B-1` (which owns `s`)
reports it instead. Each run start `s` is therefore reported by exactly one
bucket: `⌊s / K⌋`.

### Step 2 — Per-group dedup

Multiple disjoint long runs in the same group can each get reported by their
own bucket. The task only asks for one start per group, so:

```sql
SELECT group_id, MIN(start_x) FROM hits GROUP BY group_id
```

---

## Complexity

| | Sortless agg (this repo) | Classic sort + scan |
| --- | --- | --- |
| Time per group | `O(M)` | `O(M log M)` |
| Per-row state | `O(1)` | `O(1)` (after sort) |
| Cluster shuffle | hash on `(group_id, temp_x)` then on `(group_id, bucket)` | sort by `(group_id, x)` |
| Skew sensitivity | low — work scales with **distinct** `temp_x` per bucket | high — one fat group blocks one reducer |
| Row blow-up | 5× input | 1× input |

The 5× fan-out is the price you pay for skipping the sort. With heavy
per-group skew it is typically a net win on any shared-nothing engine.

---

## Cross-validation

Both implementations ship with a sort-based oracle that runs the gaps-and-
islands recipe per group and emits the first qualifying run.
A `FULL OUTER JOIN` on `group_id` produces a diff stream; rows are tagged:

- `OK` — both algorithms agree (filtered out of the diff output).
- `ONLY_AGG` / `ONLY_SORT` — one algorithm hit, the other missed.
- `DIFF_RUN` — both hit but the reported starts differ by `≥ K` (i.e. they
  belong to different runs). Differences `< K` are accepted because both
  algorithms are allowed to return *any* qualifying start within the same run.

The `K` parameter has a single source of truth in each script (a `#DECLARE`
in SCOPE, an inlined literal in MaxCompute SQL) so the two algorithms can
never drift on `K`.

---

## Outputs

All outputs are exported as TSV (or whatever the engine's default text format
is) for easy download into pandas / Excel / spreadsheets:

```
seg_result        -- agg algorithm result   (group_id, start_x)
seg_result_sort   -- sort-based oracle      (group_id, start_x)
seg_result_diff   -- non-OK rows only       (group_id, start_agg, start_sort, status)
```

---

## Parameters

```
N      = number of trajectories (groups)
M      = points per trajectory
K      = minimum run length        <-- the only thing the algorithm depends on
XRange = x ∈ [0, XRange)           <-- only used by synthetic data generation
Seed   = RNG seed for reproducibility
```

In the SCOPE script `K` is a `#DECLARE`; in the MaxCompute SQL it is the
inlined literal `10` — replace it once and the rest follows.

---

## Usage

### SCOPE

1. Open `scope_script.sln` in Visual Studio (with the SCOPE / Cosmos SDK).
2. Open `dist_k_seg_finder_agg.script`.
3. Adjust `@RootPath` to a writable VC path you own.
4. Submit. Download the four TSVs after completion.

### MaxCompute / ODPS

1. Make sure an input table `raw_data(group_id BIGINT, x BIGINT)` exists.
   The synthetic-data block at the top of the script will populate it for
   you if you uncomment those lines.
2. Run `dist_k_seg_finder_agg.sql`. Outputs land in `seg_result`,
   `seg_result_sort`, `seg_result_diff`.

### Other engines (Spark SQL / Hive / Trino / BigQuery / ClickHouse)

The MaxCompute file is intentionally close to ANSI SQL. Engine-specific
adjustments usually amount to:

- Replacing `IF(cond, a, b)` with `CASE WHEN`. Most engines keep `IF` too.
- Replacing `INSERT OVERWRITE TABLE … SELECT …` with `CREATE OR REPLACE
  TABLE … AS SELECT …` or the engine's native equivalent.
- Confirming that floor-style modulo (`(t % K + K) % K`) is correct for your
  engine's `%` operator (most engines do the right thing for `BIGINT`).

---

## Verify block

Everything between the `>>> VERIFY-BLOCK-BEGIN <<<` and
`>>> VERIFY-BLOCK-END <<<` markers exists solely for cross-validation. Once
you trust the agg result on your data, delete those blocks; the production
pipeline keeps only Step 0 → Step 1 → Step 2 and the single `seg_result`
output.

---

## Related work / what this is *not* novel about

This pipeline is a recombination of well-known building blocks; none of the
ingredients are new. References by component:

- **Gaps-and-islands** (the underlying problem family). Itzik Ben-Gan, *T-SQL
  Querying*, 2015. The "Tabibitosan" trick (Aketi Jyuuzou, Oracle community)
  is the canonical sort-based solution.
- **Replication / fan-out joins** in MapReduce: Afrati & Ullman,
  *Optimizing Joins in a Map-Reduce Environment*, EDBT 2010.
- **Paned aggregation** for sliding windows: Li, Maier, Tufte, Papadimos,
  Tucker, *No Pane, No Gain: Efficient Evaluation of Sliding-Window
  Aggregates over Data Streams*, SIGMOD Record 2005.
- **Halo / ghost cells** in stencil / HPC computations — the offset
  indicator trick is structurally the same idea.
- **Distributed connected components on MapReduce**: Kiveris, Lattanzi,
  Mirrokni, Rastogi, Vassilvitskii, *Connected Components in MapReduce and
  Beyond*, SOCC 2014. The 1-D specialization of CC is what we are really
  computing here, with `K`-buckets exploiting the linear structure to skip
  the iterative pointer-jumping.

To my knowledge there is no published name for this exact stack; if you
need a phrase for it in a doc or PR, **"Halo-Encoded Bucket Aggregation
for Run Detection"** (a.k.a. **Sortless Islands**) is what's used here.

---

## License

MIT. See repository root for details.
