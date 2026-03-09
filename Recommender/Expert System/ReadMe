# Spark Monitoring Analyzer — How It Works

## Overview

The analyzer reads Spark event logs stored in Fabric/Synapse, extracts per-application metrics, scores each job using weighted heuristics, predicts scaling outcomes using a modified Amdahl's Law model, classifies workloads into types, and writes structured recommendations to Kusto (Azure Data Explorer).

---

## 1. Metric Extraction from Event Logs

Raw Spark event logs (JSON records per stage/task/executor) are parsed in parallel using Spark itself. Per application, the following raw signals are collected:

| Signal | How Derived |
|---|---|
| `total_cpu` | Sum of `executorCpuTime` across all tasks |
| `total_exec` | Sum of `executorRunTime` across all tasks |
| `total_gc_ms` | Sum of `jvmGCTime` across all tasks |
| `task_count` | Count of completed task-end events |
| `executor_count` | Distinct executor IDs |
| `exec_run_times_sec[]` | Per-task run times for skew calculation |
| `executor_cores_per_executor` | From `spark.executor.cores` conf |
| `app_duration_sec` | `appEnd` − `appStart` timestamps |

---

## 2. Derived Feature Set

Four normalized features drive all downstream scoring and classification:

**Executor Efficiency** (CPU utilization fraction):

$$\text{executor\_eff} = \frac{\sum \text{cpuTime}}{\sum \text{runTime}}$$

**Parallelism Score** (task slot saturation, clamped to 1.0):

$$\text{parallelism\_score} = \min\!\left(1.0,\ \frac{\text{task\_count}}{\text{executor\_count} \times \text{cores\_per\_executor}}\right)$$

**GC Overhead** (fraction of executor time in GC):

$$\text{gc\_overhead} = \frac{\sum \text{gcTime\_s}}{\sum \text{runTime\_s}}$$

**Task Skew Ratio** (straggler measure):

$$\text{task\_skew} = \frac{\max(\text{task\_times})}{\overline{\text{task\_times}}}$$

---

## 3. Weighted Performance Score (0–100)

The score linearly combines the four features with empirically tuned weights:

$$\text{score} = (\text{executor\_eff} \times 30) + (\text{parallelism\_score} \times 30) + \left(\left(1 - \min(1, \text{gc\_overhead})\right) \times 20\right) + \left(\frac{1}{\max(1, \text{task\_skew})} \times 20\right)$$

| Component | Weight | Rationale |
|---|---|---|
| Executor CPU efficiency | 30 | Core measure of compute utilization |
| Parallelism | 30 | Determines how well cores are kept busy |
| GC health (inverted overhead) | 20 | High GC is a memory pressure signal |
| Skew resistance (inverted ratio) | 20 | Straggler tasks inflate wall-clock time |

**Post-score skew penalty** (applied additively after the weighted calculation):

| Skew | Penalty |
|---|---|
| < 2× | 0 pts |
| 2–5× | −5 pts |
| 5–10× | −15 pts |
| > 10× | −25 pts |

**Grade thresholds:** `> 75` → EXCELLENT · `50–75` → GOOD · `< 50` → NEEDS OPTIMIZATION

---

## 4. Workload Classification

Nine mutually exclusive workload types are classified from the feature set before scaling simulation:

| Type | Trigger Conditions |
|---|---|
| `cpu_starved` | `executor_eff < 0.3` and `gc_overhead < 0.15` |
| `memory_bound` | `gc_overhead > 0.3` |
| `severely_skewed` | `task_skew > 10×` |
| `moderately_skewed` | `task_skew > 3×` |
| `optimal_scalable` | `executor_eff > 0.7` and `parallelism_score > 0.7` |
| `starved_parallelism` | `parallelism_score < 0.3` |
| `underutilized` | `task_density < 2.0` (tasks per executor-core) |
| `driver_bound` | `driver_ratio > 0.5` |
| `balanced` | None of the above |

---

## 5. Scaling Simulation with Modified Amdahl's Law

For each executor multiplier $m \in \{0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 3.0, 4.0, 6.0, 8.0\}$, predicted duration is computed as:

**Theoretical lower bound (Amdahl's Law):**

$$S(m) = \frac{1}{f_s + \dfrac{1 - f_s}{m}}, \qquad f_s = \max(0.05,\ 1 - \text{parallelism\_score})$$

$$T_{\min}(m) = \frac{T_{\text{base}}}{S(m)}$$

Where $f_s$ is the serial fraction — the portion of work that cannot be parallelized, bounded at 5% minimum to account for unavoidable coordination overhead.

**Driver floor (hard lower bound):**

The measured driver wall-clock time is used directly as a floor on all predictions:

$$T_{\text{driver}} = T_{\text{app}} - T_{\text{executor\_wallclock}}$$

No amount of executor scaling can reduce the job below $T_{\text{driver}}$, because that time is spent by the driver orchestrating stages, running queries, etc. For a job with 68.9% driver overhead, scaling executors from 1× to 8× can save at most the 31.1% executor fraction.

**Heuristic scaling efficiency** (workload-type tuned):

| Workload | Scaling Efficiency | Interpretation |
|---|---|---|
| `cpu_starved` | $0.10 + \text{executor\_eff} \times 0.20$ | More executors won't help — CPU is not the bottleneck |
| `memory_bound` | $0.20 + (1 - \text{gc\_overhead}) \times 0.15$ | GC pressure worsens with more JVMs |
| `severely_skewed` | $\min(0.30,\ 1/\text{task\_skew})$ | Straggler caps all speedup |
| `optimal_scalable` | $0.85 + \text{parallelism\_score} \times 0.10$ | Near-linear scaling expected |
| `starved_parallelism` | $\min(0.90,\ \text{task\_count} / (m \times N))$ | Hard cap: tasks can't exceed cores |

**Estimated wall-clock time at multiplier $m$:**

$$T_{\text{est}}(m) = \underbrace{\frac{W_{\text{parallel}}}{N \cdot (1 + (m-1) \cdot \eta)}}_{\text{parallel work}} + \underbrace{W_{\text{serial}}}_{\text{serial work}} + \underbrace{T_{\text{driver}}}_{\text{driver floor}} + \underbrace{\left(\frac{N_m}{N} - 1\right) \cdot c}_{\text{coordination overhead}}$$

Then clamped: $T_{\text{est}}(m) = \max(T_{\text{est}}(m),\ T_{\min}(m),\ T_{\text{driver}})$

**GC penalty** applied for memory-bound jobs:

$$T_{\text{est}} \mathrel{\times}= 1 + \text{gc\_overhead} \times m \times 0.25$$

**Skew floor** for severely skewed jobs:

$$T_{\text{est}} = \max(T_{\text{est}},\ \bar{t}_{\text{task}} \times \text{skew} \times 0.75)$$

**Confidence scoring** per prediction:

| Factor | Values |
|---|---|
| Task count | `> 100` → 95% · `20–100` → 80% · `5–20` → 60% · `< 5` → 30% |
| Skew | `< 1.5×` → 95% · `1.5–3×` → 75% · `3–5×` → 50% · `> 5×` → 25% |

Final confidence = geometric mean of factors.

---

## 6. Issue Detection & Recommendation Prioritization

Issues are detected with severity levels (CRITICAL / HIGH / MEDIUM / LOW) and ranked by impact. Priority rules:

1. **Task skew > 10×** → always promoted to PRIMARY ISSUE (CRITICAL), regardless of other signals
2. **Task skew 5–10×** → promoted to PRIMARY ISSUE (HIGH)
3. **Driver overhead > 90%** → PRIMARY: cost optimization, suppress unrelated secondaries
4. **Executor efficiency > 50% + driver < 60%** → executor-heavy path, emit all 8 Fabric best-practice checks
5. If no actionable rule fires → emit "No Fabric-level changes recommended"

When skew is promoted to primary, the previous primary issue is demoted to a secondary issue rather than dropped.

---

## 7. Fabric-Specific Best-Practice Checks

Eight Spark/Delta configuration properties are validated per application when the job is executor-heavy. Each emits a three-way status:

| Status | Condition |
|---|---|
| `⚙️ not configured` | Property absent from session conf |
| `⚙️ wrong value` | Present but not matching recommended value |
| `✅ Validated` | Correctly set — no action needed |

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

---

## 8. Pipeline Execution Flow

```
Fabric Spark Event Logs (Kusto / Lakehouse)
        ↓
  [Cell 2-5] Load Spark Event Log Data
        ↓
  [Cell 6]  Anti-join: skip already-processed app IDs
        ↓
  [Cell 7]  Parallel per-app metric extraction
             • executor_eff, parallelism_score, gc_overhead, task_skew
             • driver time ratio, task density, stage summaries
        ↓
  [Cell 8]  Weighted performance scoring + issue detection
             • generate_enhanced_recommendations()
             • _format_plaintext_recommendation_block() [driver-side]
        ↓
  [Cell 9]  Scaling predictions (Amdahl + workload heuristics)
             • write_kusto_df() → sparklens_metadata
             •                  → sparklens_summary
             •                  → sparklens_metrics
             •                  → sparklens_predictions
             •                  → sparklens_recommedations
             •                  → fabric_recommedations
```
