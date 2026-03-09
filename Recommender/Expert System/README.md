# Spark Monitoring Analyzer — How It Works

## Overview

The analyzer reads Spark event logs stored in Fabric/Synapse from Kusto, parses them per application on the driver, extracts timing and resource signals, runs a physics-based scaling model, detects performance issues, and writes structured results to five Kusto tables.

---

## 1. Pipeline Execution Flow

```
Fabric Spark Event Logs (Kusto)
        ↓
  run(kusto_uri, database, chunk_size)
        ↓
  [Cell 6]   Anti-join: skip already-processed app IDs
        ↓
  [Cell 9]   Chunked driver-side loop
             • read_kusto_df(chunk_query).collect()     ← raw event rows pulled to driver
             • Python groupby applicationId
             • per_app_analyzer(pdf) called directly    ← no Spark shuffle
             • spark.createDataFrame(result_pdf)
             • _write_chunk_results(result_df)
               → sparklens_metadata
               → sparklens_summary
               → sparklens_metrics
               → sparklens_predictions
               → sparklens_recommedations
```

Each chunk processes at most `CHUNK_SIZE` applications. Oversized applications
(> 5 million event-log rows) are deferred to a solo queue and processed one at a time.
All Kusto reads use `distributedMode=false` — a direct driver-side REST pull.

---

## 2. Metric Extraction (`per_app_analyzer`)

All analysis runs inside a single function `per_app_analyzer(pdf)` that receives
a Pandas DataFrame of raw event-log rows for one application.

**Event parsing:**

Each row's `records` field is parsed as JSON. The analyzer walks every event and
extracts `SparkListenerApplicationStart`, `SparkListenerTaskEnd`,
`SparkListenerStageCompleted`, `SparkListenerJobEnd`, and streaming
`QueryProgressEvent` records.

**Raw signals collected per application:**

| Signal | How Derived |
|---|---|
| `total_cpu` | Sum of `Executor CPU Time` across all `SparkListenerTaskEnd` events |
| `total_exec` | Sum of `Executor Run Time` across all task-end events |
| `total_gc_ms` | Sum of `JVM GC Time` across all task-end events |
| `task_count` | Count of `SparkListenerTaskEnd` events |
| `executor_count` | Distinct executor IDs from `Task Info` |
| `exec_run_times_sec[]` | Per-task run times (used for skew and predictions) |
| `executor_cores_per_executor` | From `spark.executor.cores`; defaults to 8 (Fabric standard) |
| `app_duration_sec` | `ApplicationEnd` − `ApplicationStart` timestamps |
| `executor_wall_clock_time` | `max(task finish)` − `min(task launch)` across all tasks |
| `driver_wall_clock_time` | `app_duration_sec` − `executor_wall_clock_time` (batch only) |
| `streaming_stats[]` | Per-batch progress from `QueryProgressEvent` records |

---

## 3. Derived Features

Four normalized features drive all downstream scoring and issue detection:

**Executor Efficiency** — CPU utilization fraction:

$$\varepsilon = \frac{\sum t_{\text{cpu}}}{\sum t_{\text{run}}}$$

**Parallelism Score** — task slot saturation, clamped to 1.0:

$$\rho = \min\!\left(1.0,\; \frac{N_{\text{tasks}}}{N_{\text{exec}} \times C}\right)$$

**GC Overhead** — fraction of executor time in GC:

$$G = \frac{\sum t_{\text{gc}}}{\sum t_{\text{run}}}$$

**Task Skew Ratio** — straggler measure:

$$S = \frac{\max(t_{\text{tasks}})}{\overline{t_{\text{tasks}}}}$$

Where `executor_eff` = ε, `parallelism_score` = ρ, `gc_overhead` = G, `task_skew` = S.

---

## 4. Streaming Detection

A streaming confidence score is accumulated from multiple signals:

| Signal | Points |
|---|---|
| `StreamingQueryListener` events present | +10 |
| Streaming keywords in stage names/details | +10 |
| Job frequency > 10/min | +3 |
| Job frequency 5–10/min | +2 |
| Stage frequency > 0.5/sec | +2 |
| Micro-batch pattern (job count > 50, stage/job ratio 0.8–1.5) | +5 |
| Job count > 80 | +3 |
| App name contains stream/checkpoint keywords | +2 |

`is_streaming_job = True` when `streaming_confidence >= 5`.

For streaming jobs, `driver_wall_clock_time` is not computed from task times (set to −1).
Driver overhead for streaming is separately estimated as
`app_duration_sec` − `executor_wall_clock_time`.

---

## 5. Scaling Predictions (Batch Jobs)

For each multiplier $m \in \{0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 3.0, 4.0, 6.0, 8.0\}$:

**Core model:**

$$T_{\text{pred}}(N) = T_s + \max\!\left(\frac{T_p}{N} \cdot \text{penalty}(N),\; T_{\text{floor}}\right)$$

Where:
- $T_s$ = `driver_wall_clock_time` (serial — cannot be parallelized)
- $T_p$ = `executor_wall_clock_time` (parallel portion)
- $N = \lfloor N_0 \times m \rfloor$, with $N_0$ = current executor count
- $T_{\text{floor}}$ = `max(exec_run_times_sec)` — no stage completes faster than its slowest task

**Efficiency penalty** — product of four independent factors, each ≥ 1.0:

$$\text{penalty}(N) = P_c \times P_s \times P_g \times P_h$$

| Factor | Formula | What it captures |
|---|---|---|
| Communication $P_c$ | $1 + 0.03 \log_2(N/N_0)$ | All-reduce / shuffle coordination cost |
| Skew $P_s$ | $1 + (S-1) \times 0.85 / N$ | Straggler effect; 0.85 = AQE mitigation factor |
| GC $P_g$ | $1 + G \times 0.5 \log_2(N/N_0)$ | Smaller per-executor heap → more frequent GC |
| Shuffle $P_h$ | $1 + \max(0, r_{\text{sw}}-1) \times 0.02 \times (N/N_0 - 1)$ | More executors = more cross-node shuffle reads |

**Amdahl floor** — theoretical minimum used as an additional lower bound:

$$T_{\text{amdahl}} = \frac{T_0}{f_s + (1-f_s)/m}, \qquad f_s = T_s / T_0$$

Final prediction: $T_{\text{pred}} = \max(T_{\text{pred}},\; T_{\text{amdahl}})$

**Confidence range** — two additional predictions bracket the central estimate:

$$T_{\text{low}} = T_s + \max\!\left(\frac{T_p}{N} \cdot P \cdot 0.85,\; T_{\text{floor}}\right)$$

$$T_{\text{high}} = T_s + \max\!\left(\frac{T_p}{N} \cdot P \cdot 1.30,\; T_{\text{floor}}\right)$$

The multiplier label uses `v` (narrow: range/total < 15%) or `~` (wide) as a confidence indicator.

**Driver-bound cap:** when $T_s \geq 0.95 \times T_0$, all predicted durations equal the
current duration — executor scaling saves less than 1 second.

**Streaming jobs** emit a single 1.0× row with directly measured
`executor_wall_clock_time` and `app_duration_sec`. Scale-up/down rows are omitted —
streaming throughput is bounded by source rate and state store I/O, not executor count.

---

## 6. Issue Detection

Issues are collected into `detected_issues`, sorted by `impact_score` (highest first),
and the top issue becomes the PRIMARY recommendation.

**Batch issues:**

| Issue | Trigger | Severity |
|---|---|---|
| Low CPU efficiency | `executor_eff < 0.4` | CRITICAL if < 0.2, else HIGH |
| Memory pressure | `gc_overhead > 0.25` | CRITICAL if > 0.4, else HIGH |
| Driver bottleneck | `driver_wall_clock_time > executor_wall_clock_time` | CRITICAL |
| Low parallelism | `parallelism_score < 0.4` | HIGH if < 0.2, else MEDIUM |

**Streaming-specific issues** (only when `is_streaming_job` and not wound-down):

| Issue | Trigger |
|---|---|
| Driver overhead in foreachBatch | Driver > 50% of runtime AND app > 60 s AND driver > 10 s |
| Executor scaling advisory | Always emitted (impact\_score 9,999) |
| Trigger interval too short | Median batch duration > 2× trigger interval |
| State store growth | Late-batch state avg > 2× early-batch avg |
| State memory large (single observation) | Single snapshot > 1,000 MB |

**Streaming completed-job signals** (requires ≥ 5 active batches, skipped if wound-down):

| Signal | Fires when |
|---|---|
| Batch duration variance | p95/median > 3× (MEDIUM) or > 5× (HIGH) |
| State growth trend | Late-third avg > 1.3× early-third avg; no alert if state drained |
| Processing vs trigger interval | Median active duration / inferred trigger > 1.0× |
| Outlier batches | Any batch > 3× median; names worst 5 batch IDs |
| Input vs processing rate | `processed_rows_per_sec / input_rows_per_sec` < 0.95 |

**Wound-down streaming job:** all recorded batches have 0 input rows and 0 state rows.
A single diagnostic recommendation is emitted and all performance recommendations are
suppressed — metrics reflect idle trigger cycles, not real work.

---

## 7. Performance Score (0–100)

Computed for each batch-job analysis pass; reported in the PERFORMANCE SUMMARY line:

$$\text{score} = 30\varepsilon + 30\rho + 20(1 - \min(1, G)) + \frac{20}{\max(1, S)}$$

| Grade | Threshold |
|---|---|
| EXCELLENT | > 75 |
| GOOD | 50–75 |
| NEEDS OPTIMIZATION | < 50 |

---

## 8. Output Tables

All output is assembled as `(applicationId, dataset, payload_json)` rows inside
`per_app_analyzer` and fanned out by `_write_chunk_results` into five Kusto tables:

| Table | Content | Schema |
|---|---|---|
| `sparklens_metadata` | One row per app — Spark conf properties, executor min/max | `METADATA_SCHEMA` |
| `sparklens_summary` | One row per stage — task counts, durations, shuffle/IO sizes | `SUMMARY_SCHEMA` (30 fields, all counters LongType) |
| `sparklens_metrics` | One row per metric per app — flat key/value pairs | `METRICS_SCHEMA` (app\_id, metric, value) |
| `sparklens_predictions` | One row per multiplier per app — scaling estimates | `PREDICTIONS_SCHEMA` (5 fields) |
| `sparklens_recommedations` | One row per recommendation text per app | `RECOMMENDATIONS_SCHEMA` |

`_write_chunk_results` persists each subset to `MEMORY_AND_DISK`, counts rows,
skips the write if the subset is empty, then unpersists. The outer `result_df` is
persisted for the duration of the fan-out and unpersisted at the end.

---

## 9. Token and Kusto Access

All Kusto reads and writes share a single module-level token cache (`get_cached_token`).
Cell 9 delegates to this cache rather than maintaining its own — this prevents duplicate
`getToken()` calls when both approach expiry simultaneously after long runs.

`read_kusto_df` (event data) and `read_kusto_df_small` (metadata / count queries) both
set `distributedMode=false` to force a direct driver-side REST pull, avoiding the
`exportPartitionToBlob` path that fails on Fabric Kusto clusters.

---

## 10. Fabric-Specific Configuration Checks

Eight Spark/Delta properties are validated per application and reported in recommendations:

| Property | Recommended |
|---|---|
| `spark.databricks.delta.autoCompact.enabled` | `true` |
| `spark.microsoft.delta.targetFileSize.adaptive.enabled` | `true` |
| `spark.microsoft.delta.optimize.fast.enabled` | `true` |
| `spark.microsoft.delta.optimize.fileLevelTarget.enabled` | `true` |
| `spark.microsoft.delta.stats.collect.extended` | `true` |
| `spark.microsoft.delta.snapshot.driverMode.enabled` | `true` |
| `spark.sql.parquet.vorder.default` | `false` |
| `spark.microsoft.delta.optimizeWrite.enabled` | `false` |
