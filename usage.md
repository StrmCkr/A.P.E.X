# A.P.E.X. Usage Guide

This guide focuses on running A.P.E.X. from the command line. For project
overview, architecture, and publication notes, see [README.md](README.md). For
the data layout and descriptor vocabulary behind the run output, see
[Data and Descriptors](data-descriptors.md).

## Build

PowerShell:

```powershell
New-Item -ItemType Directory -Force out | Out-Null
$sources = Get-ChildItem -Recurse src -Filter *.java | ForEach-Object { $_.FullName }
javac --enable-preview --release 25 --add-modules jdk.incubator.vector -d out $sources
```

Bash:

```bash
mkdir -p out
javac --enable-preview --release 25 --add-modules jdk.incubator.vector -d out $(find src -name "*.java")
```

If you use a different preview-capable JDK, replace `25` with that release.

## JVM Flags

Recommended runtime flags:

```text
--enable-preview
--enable-native-access=ALL-UNNAMED
--add-modules jdk.incubator.vector
-Xmx16G
-XX:MaxDirectMemorySize=80g
```

Optional large-page and AVX flags may help on compatible systems:

```text
-XX:+UseLargePages
-XX:LargePageSizeInBytes=2m
-XX:UseAVX=3
```

## Minimal Run

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -cp out main.Apex
```

Uses:

- `RANDOM` mode
- mode-dependent default record count
- available processor threads
- auto-tuned MSD/LSD/tiny configuration

## Common Runs

Run 10 million random records:

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -cp out main.Apex mode=RANDOM records=10m
```

Set worker threads:

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -cp out main.Apex mode=RANDOM records=10m threads=16
```

Run multiple modes:

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -cp out main.Apex modes=RANDOM,DUPLICATES,LOW_BITS_ONLY records=5m
```

Run a mode range:

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -cp out main.Apex modes=RANDOM..ZIPFIANISH records=5m
```

Run every mode:

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -cp out main.Apex modes=all records=1m
```

## Record Counts

Suffixes:

- `k` = thousand
- `m` = million
- `g` = billion

Accepted forms:

```text
records=100m
records=1m,10m,100m
records=1m..16m:1m
```

## Auto-Tuning

Auto-tuning is enabled unless `config` is supplied.

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -cp out main.Apex mode=RANDOM records=10m msd=11..13 lsd=12..17 tiny=32..1024
```

Search ranges:

- `msd=MIN..MAX`
- `lsd=MIN..MAX`
- `tiny=MIN..MAX`

Use a single value to lock that search dimension:

```text
msd=12
lsd=16
tiny=128
```

## Locked Configuration

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -cp out main.Apex mode=RANDOM records=10m config=11,12,128
```

Format:

```text
config=MSD_BITS,LSD_BITS,TINY_THRESHOLD
```

This bypasses auto-tuning and is recommended for reproducible comparisons.

## Parallel Work

```text
threads=auto
threads=16
orderFastPath=true
workStealing=true
workBatch=8
```

`orderFastPath=true` enables the input-order pre-scan before MSD planning.
Ascending input can return immediately; descending input can use the global
reverse path.

`workStealing` enables dynamic redistribution of bucket refinement tasks.
`workBatch` controls how many work items are claimed per shared queue hit.

## Tuple Options

```text
tupleBits=9
tuplePacking=true
staggerTuples=true
staggerTupleBits=16
staggerTupleCostModel=true
staggerTupleMinRecords=0
```

`tupleBits` caps direct tuple-space width. The current maximum cap is `16`.
`tuplePacking=true` forces packed sparse tuple cycles. `staggerTuples=true`
lets LSD refinement consider wider packed tuple cycles up to
`staggerTupleBits`. With `staggerTupleCostModel=true`, candidate widths are
scored; with it disabled, the wider width is used whenever it reduces pass
count. `staggerTupleMinRecords` can limit this route to larger partitions.

## Memory Options

```text
heapScratch=1048576
largePermits=4
lsdHeapUnroll=0
lsdHeapUnrollMinRecords=4096
```

`heapScratch` controls the max heap-backed scratch records per worker before
larger paths are used. `largePermits` limits concurrent large off-heap
partitions. If `largePermits` is omitted, A.P.E.X. derives a value from the
thread count. `lsdHeapUnroll=0` uses the 8-record heap unroll path adaptively
for partitions at or above `lsdHeapUnrollMinRecords`; `lsdHeapUnroll=8` forces
that path.

## Adaptive Refinement Options

```text
localMsdBits=0
localMsdMaxChildren=8192
dominantCore=true
dominantCoreSample=262144
dominantCoreCandidates=64
dominantCoreMinShare=80
dominantKeyMinShareDivisor=1024
```

`localMsdBits=0` lets A.P.E.X. derive the local repartition width from the
selected config. `localMsdMaxChildren` caps total local child buckets across
the top-level MSD plan; use `0` for uncapped local-MSD planning.

`dominantCore` enables the duplicate-heavy/outlier route. It samples candidate
dominant keys, verifies them with exact counts, places the sorted dominant
core, and refines only the remaining tail when the core is a safe sorted
prefix.

## Runtime Option Names

| Option | Meaning |
| --- | --- |
| `mode`, `modes` | Single mode, comma list, `all`, or enum range |
| `records`, `record`, `n` | Record count, list, or range |
| `config`, `locked` | Locked `MSD,LSD,TINY` configuration |
| `msd`, `msdRange` | MSD auto-tune bit range |
| `lsd`, `lsdRange` | LSD auto-tune bit range |
| `tiny`, `tinyRange` | Tiny threshold auto-tune range |
| `threads` | Worker threads, or `auto` |
| `orderFastPath`, `inputOrderFastPath`, `prescan` | Enable the input order pre-scan fast path |
| `workStealing`, `lsdWorkStealing`, `steal` | Enable/disable LSD work stealing |
| `workBatch`, `stealBatch`, `workStealBatch` | Work claim batch size |
| `tupleBits`, `tupleCap`, `tupleCapBits`, `directTupleBits` | Direct tuple bit cap |
| `tuplePacking`, `packedTuples`, `tupleCycles`, `tuples` | Force packed sparse tuple cycles |
| `staggerTuples`, `staggerTupleCycles` | Enable adaptive wider tuple-cycle planning |
| `staggerTupleBits` | Max tuple-cycle width considered, capped at `16` |
| `staggerTupleCostModel`, `staggerCostModel` | Score stagger widths; `false` uses fixed wider width when it saves a pass |
| `staggerTupleMin`, `staggerTupleMinRecords` | Minimum partition size for staggered tuple planning |
| `lsdHeapUnroll`, `heapUnroll` | `0` adaptive, `8` force heap unroll-8 path |
| `lsdHeapUnrollMin`, `heapUnrollMin`, `lsdHeapUnrollMinRecords` | Minimum size for adaptive heap unroll |
| `heapScratch`, `heapScratchRecords`, `maxHeapScratch` | Heap scratch record limit |
| `largePermits`, `largePartitionPermits`, `largePartitions` | Concurrent large partition permits |
| `localMsdBits`, `secondaryMsdBits`, `subMsdBits` | Override local MSD repartition width |
| `localMsdMaxChildren`, `localMsdChildren`, `maxLocalMsdChildren` | Cap total local-MSD child buckets; `0` disables the cap |
| `dominantCore`, `dominantCoreFastPath` | Enable duplicate-heavy/outlier dominant-core fast path |
| `dominantCoreSample`, `dominantCoreSampleRecords` | Records sampled while finding dominant-key candidates |
| `dominantCoreCandidates` | Candidate slots used by dominant-core detection |
| `dominantCoreMinShare`, `dominantCoreMinSharePercent` | Minimum combined dominant-core share, as a percent |
| `dominantKeyMinShareDivisor`, `dominantKeyShareDivisor` | Minimum per-key share divisor for dominant candidates |
| `tuneRecords`, `tune` | Auto-tune subset size |
| `warmupRecords`, `warmup` | Selected-config warmup size |
| `sweep` | Run every data mode |
| `sweepRecords`, `sweepN` | Record count per sweep mode |

## Warmup and Tuning Counts

```text
tuneRecords=10m
warmupRecords=100m
```

`tuneRecords` controls the single auto-tune pass size. `warmupRecords` controls
selected-config warmup size.

## Sweep Mode

Run all data modes:

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -cp out main.Apex sweep=true sweepRecords=100m
```

`sweepRecords` sets records per mode during the sweep.

## Data Modes

Modes are values from `generator.DataMode`. Examples:

```text
RANDOM
SORTED
REVERSE
DUPLICATES
LOW_BITS_ONLY
HIGH_BITS_ONLY
FEW_UNIQUE_VALUES
DESCENDING_BLOCKS
TINY_PARTITIONS_STRESS
SPARSE_ENTROPY_EXPLOSION
LOW_CARDINALITY_HIGH_VOLUME
```

Use `modes=all` for every mode, or ranges such as:

```text
modes=RANDOM..ZIPFIANISH
```

## Visualizer

The browser visualizer is static:

```text
index.html
app.js
styles.css
```

Open `index.html` locally, or publish the repository through GitHub Pages.

The visualizer shows:

- source array
- MSD bucket scatter
- tiny subroutes
- tuple direct/tail routes
- LSD refinement
- global reverse
- bucket reverse
- final sorted array

Controls:

| Control | Values |
| --- | --- |
| `N` | Record count from `0` to `1,000,000`, default `2,048` |
| `Data type` | Visualizer data topology |
| `MSD` | MSD bit width, `2..13`, default `8` |
| `LSD` | LSD bit width, `2..17`, default `8` |
| `Threads` | `1`, `2`, `4`, `8`, `16`, `32`, or `64`, default `16` |
| `Tiny` | `32`, `64`, `128`, `512`, or `1024`, default `128` |
| `Tuple bits` | Direct tuple cap, `2..16`, default `9` |
| `Tuples` | Enables tuple direct/tail planning and shows the tuple lane |
| `Play/Pause` | Runs or pauses the current execution plan |
| `Step` | Advances one execution step |
| `Reset` | Rebuilds the visual plan from the current controls |

The tuple lane and tuple colour-key items are hidden when `Tuples` is off. The
display order is source, MSD, tiny, tuples, LSD, reverse, then sorted output.

## Notes

- A.P.E.X. sorts unsigned 64-bit keys carried in 16-byte key/value records.
- Large runs are memory intensive.
- Record buffers can require roughly `records * 16 * 2` bytes.
- Runtime performance depends on CPU topology, memory bandwidth, JDK version,
  JVM flags, and data distribution.
