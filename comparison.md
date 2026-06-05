# A.P.E.X. Comparison and Benchmarking Guide

This document describes how to compare A.P.E.X. against baseline sorters in a
repeatable way. For command-line usage, see [usage.md](usage.md). For the
algorithm execution model, see [operation-execution.md](operation-execution.md).

## Purpose

The comparison harness is intended to answer three questions:

- How does A.P.E.X. perform on large 64-bit key workloads?
- How does the record-sort path compare with key-only baselines?
- How does performance change across different data topologies?

The harness runs deterministic generated datasets, checks sorted output after
each run, and reports timing statistics across warmups and measured runs.

## Important Scope

A.P.E.X. sorts 16-byte records:

```text
key:   raw 64-bit
value: 64-bit payload
```

Unsigned key order is the default. Pass `signed=true` or `keyOrder=signed` to
compare signed `long` ordering; the harness applies the same selected order to
A.P.E.X. verification and compatible baselines.

Some baselines sort only `long[]` keys. Those are useful comparisons, but they
are not the same workload as moving key/value records. When publishing results,
keep `record-sort`, `object-record`, and `key-only` results visibly separated.

## Build

Compile the project:

```powershell
New-Item -ItemType Directory -Force out | Out-Null
$sources = Get-ChildItem -Recurse src -Filter *.java | ForEach-Object { $_.FullName }
javac --enable-preview --release 25 --add-modules jdk.incubator.vector -d out $sources
```

The Fastutil baselines are loaded at runtime. Include the bundled jar on the
runtime classpath when running comparison modes that include Fastutil.

## Fastutil Dependency Provenance

The bundled jar:

```text
src/Comparison/fastutil-8.5.13.jar
```

is provided only for the optional Fastutil comparison baselines. It comes from
the original Fastutil project and should remain attributed to that project:

- Original project: [vigna/fastutil](https://github.com/vigna/fastutil)
- Versioned artifact: [it.unimi.dsi:fastutil:8.5.13](https://repo1.maven.org/maven2/it/unimi/dsi/fastutil/8.5.13/)
- License: Apache License 2.0

## Quick Comparison Run

PowerShell:

```powershell
java --enable-preview --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -Xmx16G -XX:MaxDirectMemorySize=80g -cp "out;src\Comparison\fastutil-8.5.13.jar" Comparison.SortComparison curated=true modes=RANDOM,DUPLICATES records=1m runs=5 warmups=1 threads=auto config=13,12,128
```

Bash:

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -Xmx16G -XX:MaxDirectMemorySize=80g -cp "out:src/Comparison/fastutil-8.5.13.jar" Comparison.SortComparison curated=true modes=RANDOM,DUPLICATES records=1m runs=5 warmups=1 threads=auto config=13,12,128
```

The default comparison set is intentionally large. It covers multiple modes,
record counts up to `500m`, multiple algorithms, `3` warmups, and `21`
measured runs. For normal development, pass explicit `modes`, `records`,
`runs`, and `warmups`.

## Curated Baselines

By default, `curated=true` runs the publication-friendly set:

| Algorithm | Kind | Notes |
| --- | --- | --- |
| `apex-records` | `record-sort` | Full A.P.E.X. key/value record pipeline |
| `apex-keys` | `key-only` | A.P.E.X. pipeline with key-order verification |
| `jdk-arrays-sort` | `key-only` | JDK `Arrays.sort`, with unsigned transform when needed |
| `jdk-parallel-sort` | `key-only` | JDK `Arrays.parallelSort`, with unsigned transform when needed |
| `fastutil-parallel-radix` | `key-only` | Fastutil parallel radix sort |

This set is a practical first table for GitHub or paper-adjacent reporting.

## Expanded Baselines

Use `curated=false` to select a larger comparison set. The `algos` option is
used only when curated mode is disabled.

```bash
curated=false algos=records
curated=false algos=keys
curated=false algos=all
curated=false algos=apex-records,record-msd-radix-8,record-lsd-radix-16
```

Record-oriented options:

| Algorithm | Kind |
| --- | --- |
| `apex-records` | `record-sort` |
| `jdk-object-arrays-sort` | `object-record` |
| `jdk-object-parallel-sort` | `object-record` |
| `record-lsd-radix-16` | `record-sort` |
| `record-msd-radix-8` | `record-sort` |
| `record-american-flag` | `record-sort` |

Key-only options:

| Algorithm | Kind |
| --- | --- |
| `apex-keys` | `key-only` |
| `jdk-arrays-sort` | `key-only` |
| `jdk-parallel-sort` | `key-only` |
| `lsd-radix-16` | `key-only` |
| `msd-radix-8` | `key-only` |
| `american-flag` | `key-only` |
| `fastutil-radix` | `key-only` |
| `fastutil-parallel-radix` | `key-only` |

The JDK object-record baselines are capped at `50m` records by the harness. If
an algorithm cannot run for a selected size, the result row is shown as skipped.

## Harness Options

| Option | Meaning |
| --- | --- |
| `curated` | `true` for default curated set, `false` to use `algos` |
| `mode` | Single `DataMode` |
| `modes` | Comma list, `all`, or enum range |
| `records`, `n` | Record count, list, or range |
| `runs` | Measured run count |
| `warmup`, `warmups` | Warmup run count |
| `threads` | A.P.E.X. worker count, or `auto` |
| `config` | Locked `MSD,LSD,TINY` configuration |
| `algos`, `algo` | Expanded algorithm selection when `curated=false` |
| `tupleBits` | Direct tuple-space bit cap |
| `contiguousTupleBits`, `directTupleContiguousBits` | Direct tuple bit cap for contiguous masks |
| `directTupleInPlaceMax`, `directTupleInPlaceMaxRecords`, `tupleInPlaceMax` | Max records for direct tuple in-place projection |
| `directTupleManyPartitions`, `directTupleManyPartitionMin`, `tupleManyPartitions` | Direct tuple partition count that forces in-place projection |
| `tuplePacking` | Force packed sparse tuple cycles |
| `staggerTuples`, `staggerTupleCycles` | Enable adaptive wider tuple-cycle planning |
| `staggerTupleBits` | Max tuple-cycle width considered, capped at `16` |
| `staggerTupleCostModel`, `staggerCostModel` | Score stagger widths; `false` uses fixed wider width when it saves a pass |
| `staggerTupleMin`, `staggerTupleMinRecords` | Minimum partition size for staggered tuple planning |
| `lsdHeapUnroll`, `heapUnroll` | `0` adaptive, `8` force heap unroll-8 path |
| `lsdHeapUnrollMin`, `heapUnrollMin`, `lsdHeapUnrollMinRecords` | Minimum size for adaptive heap unroll |
| `heapScratch`, `heapScratchRecords` | Heap scratch record limit |
| `localMsdBits` | Override local MSD repartition width |
| `localMsdMaxChildren`, `localMsdChildren`, `maxLocalMsdChildren` | Cap total local-MSD child buckets; `0` disables the cap |
| `dominantCore`, `dominantCoreFastPath` | Enable duplicate-heavy/outlier dominant-core fast path |
| `dominantCoreSample`, `dominantCoreSampleRecords` | Records sampled while finding dominant-key candidates |
| `dominantCoreCandidates` | Candidate slots used by dominant-core detection |
| `dominantCoreMinShare`, `dominantCoreMinSharePercent` | Minimum combined dominant-core share, as a percent |
| `dominantKeyMinShareDivisor`, `dominantKeyShareDivisor` | Minimum per-key share divisor for dominant candidates |
| `largePermits`, `largePartitionPermits` | Concurrent large partition permits |
| `orderFastPath`, `inputOrderFastPath`, `prescan` | Enable the input order pre-scan fast path |
| `workStealing`, `lsdWorkStealing` | Enable or disable LSD work stealing |
| `workBatch`, `stealBatch`, `workStealBatch` | Work claim batch size |

Boolean values accept forms such as `true`, `yes`, `1`, and `on`.

## Data Modes

The curated defaults cover:

```text
RANDOM
LOW_BITS_ONLY
HIGH_BITS_ONLY
DUPLICATES
ZIPFIANISH
SPARSE_ENTROPY_EXPLOSION
```

Use `modes=all` to run every generator mode, or a range such as:

```text
modes=RANDOM..ZIPFIANISH
```

## Output Columns

The comparison table reports:

| Column | Meaning |
| --- | --- |
| `algorithm` | Benchmark name |
| `kind` | Workload category |
| `median(s)` | Median measured time |
| `mean(s)` | Mean measured time |
| `stddev(s)` | Standard deviation |
| `CV(%)` | Coefficient of variation |
| `p95(s)` | 95th percentile measured time |
| `p99(s)` | 99th percentile measured time |
| `M rec/s` | Records per second from median time |

Median time is the preferred headline number. Use `p95`, `p99`, and `CV(%)`
to show stability and outliers.

## Verification

Every benchmark run is verified:

- key-only baselines are checked for selected key order, sum, and xor
- record baselines are checked for selected key order, sum, and xor
- A.P.E.X. record runs are verified through the project verifier

The generator is deterministic for a given mode and record count, so each
algorithm sees the same logical dataset.

## Recommended Publication Runs

Small sanity run:

```bash
modes=RANDOM,DUPLICATES records=1m runs=5 warmups=1 threads=auto config=13,12,128
```

Curated table:

```bash
curated=true modes=RANDOM,LOW_BITS_ONLY,HIGH_BITS_ONLY,DUPLICATES,ZIPFIANISH,SPARSE_ENTROPY_EXPLOSION records=1m,10m,100m runs=21 warmups=3 threads=auto config=13,12,128
```

Expanded record-only table:

```bash
curated=false algos=records modes=RANDOM,DUPLICATES records=1m,10m,50m runs=21 warmups=3 threads=auto config=13,12,128
```

Expanded key-only table:

```bash
curated=false algos=keys modes=RANDOM,LOW_BITS_ONLY,HIGH_BITS_ONLY records=1m,10m,100m runs=21 warmups=3 threads=auto config=13,12,128
```

## Reporting Guidance

For publication, report:

- CPU model, core count, memory speed if known, operating system, and JDK
- JVM flags, heap size, and direct memory size
- selected `config=MSD,LSD,TINY`
- `threads`, `tupleBits`, `tuplePacking`, `localMsdMaxChildren`, `dominantCore`, and `workStealing`
- mode list, record counts, warmups, and measured runs
- whether the table is curated or expanded

Avoid mixing key-only and record-sort results into one speedup claim. They move
different amounts of data and exercise different memory behavior.
