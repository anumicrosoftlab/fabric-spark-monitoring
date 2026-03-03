# Vasa Spark Simulator

30 Spark workload simulations for Microsoft Fabric — anti-patterns, performance issues, and best-practice industry apps. Use them to exercise the Spark Monitoring Analyzer and demonstrate optimization impact.

## Installation

```bash
# Build wheel
python -m build --wheel

# Install locally
pip install dist/vasa_spark_simulator-0.1.0-py3-none-any.whl

# On Fabric — upload wheel to Lakehouse, then in a notebook cell:
# %pip install /lakehouse/default/Files/vasa_spark_simulator-0.1.0-py3-none-any.whl
```

## How to Run

### Fabric Notebook

```python
from vasa_spark_simulator import fabric_notebook_entry

fabric_notebook_entry(1)                                    # by job number
fabric_notebook_entry('pharma_etl')                         # by job name
fabric_notebook_entry(5, num_rows=5_000_000, duration_minutes=3)
```

### Pipeline (structured result)

```python
from vasa_spark_simulator import pipeline_entry

result = pipeline_entry(1, num_rows=1_000_000, duration_minutes=2)
# result = { 'status': 'success'|'failed', 'job_name': ..., 'duration': ..., ... }
```

### Command Line

```bash
python -m vasa_spark_simulator --list                        # list all jobs
python -m vasa_spark_simulator --job 1
python -m vasa_spark_simulator --job pharma_etl --num-rows 5000000 --duration 5
```

### Optimized Variants

Every job has an `_optimized` counterpart that applies all 8 Fabric best-practice configs:

```python
from vasa_spark_simulator.optimized_jobs import OPTIMIZED_JOBS

OPTIMIZED_JOBS['driver_heavy_collect'](spark)
```

## Job Reference

### Anti-Patterns — Jobs 1–12

| # | Job Name | Description |
|---|---|---|
| 1 | `driver_heavy_collect` | Collects large dataset to driver |
| 2 | `driver_heavy_map` | Driver-side map processing |
| 3 | `no_caching_recompute` | Recomputes DataFrames multiple times |
| 4 | `python_udf_instead_of_builtin` | Python UDFs instead of built-ins |
| 5 | `groupbykey_instead_of_reducebykey` | Inefficient aggregation pattern |
| 6 | `excessive_repartitioning` | Over-partitioning / small files |
| 7 | `cartesian_product_join` | Expensive cross join |
| 8 | `count_then_collect` | Redundant actions on same DF |
| 9 | `small_file_problem` | Too many small output partitions |
| 10 | `broadcast_large_table` | Broadcasting oversized tables |
| 11 | `iterative_without_caching` | No caching inside loops |
| 12 | `select_star_large_table` | Reading all columns unnecessarily |

### Performance Issues — Jobs 13–18

| # | Job Name | Description |
|---|---|---|
| 13 | `shuffle_groupby` | Heavy shuffle (50M rows) |
| 14 | `shuffle_join` | Large shuffle join (10M rows) |
| 15 | `spill_large_join` | Memory-spill join (25M rows) |
| 16 | `spill_aggregation` | Spill-heavy aggregation (100M rows) |
| 17 | `skew_key_join` | Severely skewed join (15M rows) |
| 18 | `skew_groupby` | Skewed group-by (20M rows) |

### Best Practice Industry Apps — Jobs 19–30

| # | Job Name | Description |
|---|---|---|
| 19 | `pharma_etl` | Multi-stage clinical trial ETL (5M rows, 5 stages) |
| 20 | `adverse_event_agg` | Event aggregation pipeline |
| 21 | `patient_journey` | Journey analysis with window functions |
| 22 | `genomic_variant` | Genomic analysis with pivots (50M rows) |
| 23 | `pharma_fraud` | Fraud detection with array functions |
| 24 | `inventory_analytics` | Supply chain analytics |
| 25 | `prescription_token` | Text processing pipeline |
| 26 | `cohort_selection` | Patient cohort identification |
| 27 | `drug_recommend` | Recommendation system |
| 28 | `time_series_health` | Time series analysis |
| 29 | `cost_prediction` | ML feature engineering (8M rows, 9 stages) |
| 30 | `log_analysis` | Large-scale log processing (30M rows, 7 stages) |
