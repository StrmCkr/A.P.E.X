# A.P.E.X.

**Adaptive Parallel Extremal Dispatch**

A.P.E.X. is a high-performance Java sorting framework for large fixed-width
64-bit key/value record datasets. It uses descriptor-driven radix planning,
parallel scatter, per-bucket dispatch, tuple projection, tiny-sort fallbacks,
and local refinement to sort unsigned 64-bit keys by default, with an optional
signed-key ordering mode, while avoiding unnecessary passes over bits that are
already resolved.

The project also includes an interactive browser visualizer that explains the
execution plan: source array, MSD scatter, tiny routes, tuple routes, LSD
refinement, global/bucket reverse paths, and final sorted placement.

## Documentation

- [Data and Descriptors](data-descriptors.md): record layout, data topologies,
  descriptor formulas, and why descriptors drive dispatch
- [Usage Guide](usage.md): build commands, runtime options, and common runs
- [Comparison Guide](comparison.md): benchmark harness, baselines, and reporting
- [Operation Model](operation-execution.md): execution flow, dispatch rules, and
  math-paper reference

## Name

**A - Adaptive**

Execution changes with the observed input structure: radix geometry, bucket
routes, tuple use, tiny thresholds, and refinement strategy are selected from
the data rather than fixed up front.

**P - Parallel**

Histogramming, scatter, bucket refinement, and work scheduling are designed for
multi-core execution.

**E - Extremal**

A.P.E.X. uses per-bucket extremal descriptors, especially bitwise `OR` and
bitwise `AND`, to identify which key bits still vary. The variable-bit mask is:

```text
VBM = OR ^ AND
```

Only unresolved bits continue into refinement.

**X - Dispatch**

Buckets are routed to the cheapest applicable path: already done, global
reverse, bucket reverse, tiny sort, tuple projection, LSD cycles, or LSD with a
tuple tail.

## Core Idea

Most radix sort pipelines pay for fixed key width. A.P.E.X. pays for remaining
structure.

For each partition, A.P.E.X. tracks:

- record count
- bitwise `OR`
- bitwise `AND`
- variable-bit mask
- local order classification

That lets the sorter eliminate constant bits, skip resolved buckets, reverse
monotonic descending regions, and collapse compact sparse variability into
tuple domains.

## Execution Model

1. **Global order scan**

   Whole-input ascending data is accepted immediately. Whole-input descending
   data takes the **global reverse** fast path.

2. **Adaptive MSD planning**

   A.P.E.X. chooses an MSD extraction window from observed high-information
   regions instead of blindly using the top bits.

3. **Parallel MSD scatter**

   Threads read contiguous blocks and scatter records into MSD buckets.

4. **Bucket classification**

   Each bucket is marked as empty, all-equal, ascending, descending, or mixed.
   Ascending buckets are skipped. Descending buckets take the **bucket reverse**
   path.

5. **Refinement dispatch**

   Mixed buckets are routed to one of:

   - tiny sort
   - direct tuple projection
   - LSD radix cycles
   - LSD cycles followed by tuple-tail projection

6. **Final assembly**

   Completed buckets land in final sorted order.

## Tiny Sort Routes

Tiny sort is a bucket-level fallback for partitions below the configured tiny
threshold. Inside that route, the implementation selects a function by size:

| Size | Tiny function |
| ---: | --- |
| `1..23` | `insertionSmall` |
| `24..63` | `binaryInsertionSmall` |
| `64..127` | `quickSort` |
| `128..191` | `threeWayQuickSort` |
| `192+` | `MsdRadix8KV` |

The configured tiny threshold controls which bucket sizes enter the tiny route.
For example, `tiny=128` can reach the first three tiny functions; `tiny=1024`
can reach all five.

## Tuple Projection

Tuple projection uses the active variable bits as an exact compact index:

```text
tupleIndex = Long.compress(key, variableMask)
tupleRadix = 1 << bitCount(variableMask)
```

A direct tuple pass counts tuple indexes, prefix-sums their target ranges, and
places records into those ranges. It is not a comparison sort. After LSD cycles,
a tuple-tail pass can terminate remaining sparse variable bits when the tuple
space fits the configured cap.

## Visualizer

The visualizer is a static browser app:

- [index.html](index.html)
- [app.js](app.js)
- [styles.css](styles.css)

Open `index.html` locally, or publish the repository with GitHub Pages. The
current project link is:

[https://strmckr.github.io/A.P.E.X/](https://strmckr.github.io/A.P.E.X/)

Visualizer controls:

| Control | Values |
| --- | --- |
| `N` | Record count from `0` to `1,000,000`, default `2,048` |
| `Data type` | Visualizer data topology; labels display `ENTROPY` as `EXTREMAL` |
| `MSD` | MSD bit width, `2..13`, default `8` |
| `LSD` | LSD bit width, `2..17`, default `8` |
| `Threads` | `1`, `2`, `4`, `8`, `16`, `32`, or `64`, default `16` |
| `Tiny` | `32`, `64`, `128`, `512`, or `1024`, default `128` |
| `Tuple bits` | Direct tuple cap, `2..16`, default `9` |
| `Tuples` | Enables tuple direct/tail planning; when off, the tuple lane and tuple key items are hidden |
| `Play/Pause` | Runs or pauses the current execution plan |
| `Step` | Advances one execution step |
| `Reset` | Rebuilds the visual plan from the current controls |

Changing a control rebuilds the visual plan. The lane order is source array,
MSD bucket plan and scatter, tiny partition lane, tuple direct/tail lane, LSD
refinement lane, reverse lane, and sorted output array.

The visualizer is educational, but its routes are aligned with the Java planner:
global reverse, bucket reverse, tiny subroutes, tuple direct/tail, LSD cycles,
and final bucket placement.

## Requirements

Recommended:

- JDK 25 or newer
- `jdk.incubator.vector`
- preview enabled for builds/runs that require it
- enough heap and direct memory for the selected record count

A.P.E.X. stores records as 16-byte key/value pairs. Large runs can allocate both
source and destination buffers, so plan memory as roughly:

```text
records * 16 bytes * 2
```

Example: `100m` records can require about 3 GiB just for source/destination
record buffers before other runtime overhead.

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

If you are compiling with a different preview-capable JDK, replace `25` with
your installed JDK release.

## Usage

Run the main sorter:

```bash
java \
  --enable-preview \
  --enable-native-access=ALL-UNNAMED \
  --add-modules jdk.incubator.vector \
  -Xmx16G \
  -XX:MaxDirectMemorySize=80g \
  -cp out \
  main.Apex mode=RANDOM records=10m threads=16
```

PowerShell single-line example:

```powershell
java --enable-preview --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -Xmx16G -XX:MaxDirectMemorySize=80g -cp out main.Apex mode=RANDOM records=10m threads=16
```

### Basic Runs

Default mode and mode-dependent record count:

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -cp out main.Apex
```

Explicit data mode and record count:

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -cp out main.Apex mode=LOW_BITS_ONLY records=50m
```

Multiple modes:

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -cp out main.Apex modes=RANDOM,DUPLICATES,SPARSE_ENTROPY_EXPLOSION records=10m
```

Mode range:

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -cp out main.Apex modes=RANDOM..ZIPFIANISH records=5m
```

### Record Counts

Record counts accept suffixes:

- `k` = thousand
- `m` = million
- `g` = billion

Lists and ranges are supported:

```bash
records=1m,10m,100m
records=1m..16m:1m
```

### Configuration

Auto-tuning is the default. It searches MSD, LSD, and tiny threshold ranges:

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -cp out main.Apex mode=RANDOM records=10m msd=11..13 lsd=12..17 tiny=32..1024
```

Use a locked configuration for reproducible experiments:

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -cp out main.Apex mode=RANDOM records=10m config=11,12,128
```

The locked format is:

```text
config=MSD_BITS,LSD_BITS,TINY_THRESHOLD
```

### Parallelism

```bash
threads=auto
threads=16
orderFastPath=true
signed=true
workStealing=true
workBatch=8
descendingScatter=true
```

`orderFastPath=true` enables an input-order pre-scan before MSD planning so
already ascending input can return immediately and fully descending input can
take the global reverse path.
`signed=true` or `keyOrder=signed` flips only the key ordering view
(`key ^ Long.MIN_VALUE`); stored records keep their raw key bits.
MSD scatter uses the direct unrolled scatter path.
`descendingScatter=true` lets the planned MSD scatter normalize fully
descending inputs instead of doing a separate whole-array reverse copy.

Work stealing dynamically redistributes bucket refinement when buckets are
imbalanced.

### Tuples

```bash
tupleBits=9
contiguousTupleBits=16
directTupleInPlaceMax=262144
directTupleManyPartitions=16
tuplePacking=true
staggerTuples=true
staggerTupleBits=16
staggerTupleCostModel=true
staggerTupleMinRecords=0
```

`tupleBits` caps direct tuple-space width. The current maximum cap is `16`.
`contiguousTupleBits` lets plain contiguous tuple tails use a wider direct
route when the partition fits heap refinement; sparse tuple masks still obey
`tupleBits`.
`directTupleInPlaceMax` controls when direct tuple projection uses the in-place
cycle route instead of the off-heap scatter/copy route. The default is
262,144 records.
`directTupleManyPartitions` keeps direct tuple buckets in-place when a plan has
many tuple refinement items. The default is `16`; set it to `0` to disable
that override.
`tuplePacking=true` forces packed sparse tuple cycles; automatic packed cycles
can still be selected when they reduce cycle count. `staggerTuples=true` lets
LSD refinement consider wider packed tuple cycles up to `staggerTupleBits`.
When `staggerTupleCostModel=true`, A.P.E.X. scores candidate widths; when it is
`false`, the configured wider width is used whenever it reduces pass count.
`staggerTupleMinRecords` can restrict staggered tuple planning to larger
partitions.

### Memory

```bash
heapScratch=1048576
largePermits=4
lsdHeapUnroll=0
lsdHeapUnrollMinRecords=4096
```

`heapScratch` controls when worker scratch spills toward the off-heap path.
`largePermits` limits concurrent large off-heap partitions. If omitted, A.P.E.X.
chooses an automatic permit count from the thread count. `lsdHeapUnroll=0`
uses the 8-record heap unroll path adaptively for partitions at or above
`lsdHeapUnrollMinRecords`; `lsdHeapUnroll=8` forces that path.

### Adaptive Refinement

```bash
localMsdBits=0
localMsdMaxChildren=8192
dominantCore=true
dominantCoreSample=262144
dominantCoreCandidates=64
dominantCoreMinShare=80
dominantKeyMinShareDivisor=1024
```

`localMsdBits=0` lets the selected LSD width choose the local-MSD child width.
`localMsdMaxChildren` caps total local child planning across the top-level MSD
plan; set it to `0` to remove the cap. `dominantCore=true` enables the
duplicate-heavy/outlier fast path, which samples candidate dominant keys,
confirms them with an exact count, places the sorted dominant core, and then
refines only the remaining tail when the key order makes that safe.

### Benchmark Sweep

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -cp out main.Apex sweep=true sweepRecords=100m
```

Sweep mode runs all `DataMode` values using the selected or auto-tuned
configuration.

## Runtime Options

| Option | Meaning |
| --- | --- |
| `mode` | Single data mode |
| `modes` | Comma list, `all`, or enum range such as `RANDOM..ZIPFIANISH` |
| `records`, `record`, `n` | Record count, list, or range |
| `config`, `locked` | Locked `MSD,LSD,TINY` configuration |
| `msd` | MSD auto-tune bit range |
| `lsd` | LSD auto-tune bit range |
| `tiny` | Tiny threshold auto-tune range |
| `threads` | Worker threads, or `auto` |
| `orderFastPath`, `inputOrderFastPath`, `prescan` | Enable the input order pre-scan fast path |
| `signed`, `signedKeys`, `keyOrder` | Sort by signed `long` order instead of unsigned order |
| `workStealing` | Enable/disable LSD work stealing |
| `workBatch` | Work-steal batch size |
| `descendingScatter`, `descScatter` | Normalize fully descending planned inputs through scatter |
| `tupleBits` | Direct tuple bit cap |
| `contiguousTupleBits`, `directTupleContiguousBits` | Direct tuple bit cap for contiguous masks |
| `directTupleInPlaceMax`, `directTupleInPlaceMaxRecords`, `tupleInPlaceMax` | Max records for direct tuple in-place projection |
| `directTupleManyPartitions`, `directTupleManyPartitionMin`, `tupleManyPartitions` | Direct tuple partition count that forces in-place projection |
| `tuplePacking` | Force packed tuple cycles |
| `staggerTuples`, `staggerTupleCycles` | Enable adaptive wider tuple-cycle planning |
| `staggerTupleBits` | Max tuple-cycle width considered, capped at `16` |
| `staggerTupleCostModel`, `staggerCostModel` | Score stagger widths; `false` uses fixed wider width when it saves a pass |
| `staggerTupleMin`, `staggerTupleMinRecords` | Minimum partition size for staggered tuple planning |
| `lsdHeapUnroll`, `heapUnroll` | `0` adaptive, `8` force heap unroll-8 path |
| `lsdHeapUnrollMin`, `heapUnrollMin`, `lsdHeapUnrollMinRecords` | Minimum size for adaptive heap unroll |
| `heapScratch` | Max heap scratch records per worker |
| `largePermits` | Concurrent large partition permits |
| `localMsdBits` | Override local MSD repartition width |
| `localMsdMaxChildren` | Cap total local-MSD child buckets; `0` disables the cap |
| `dominantCore` | Enable duplicate-heavy/outlier dominant-core fast path |
| `dominantCoreSample`, `dominantCoreSampleRecords` | Records sampled while finding dominant-key candidates |
| `dominantCoreCandidates` | Candidate slots used by dominant-core detection |
| `dominantCoreMinShare` | Minimum combined dominant-core share, as a percent |
| `dominantKeyMinShareDivisor` | Minimum per-key share divisor for dominant candidates |
| `tuneRecords` | Record count for auto-tuning |
| `warmupRecords` | Warmup record count for selected config |
| `sweep` | Run all data modes |
| `sweepRecords` | Record count for sweep mode |

Boolean options accept values such as `true`, `yes`, `1`, and `on`.

## Data Modes

The generator includes many topologies for normal, skewed, sparse, duplicate,
pathological, and adversarial distributions. Common examples:

- `RANDOM`
- `SORTED`
- `REVERSE`
- `DUPLICATES`
- `LOW_BITS_ONLY`
- `HIGH_BITS_ONLY`
- `FEW_UNIQUE_VALUES`
- `DESCENDING_BLOCKS`
- `TINY_PARTITIONS_STRESS`
- `SPARSE_ENTROPY_EXPLOSION`
- `PREFIX_CONSTANT_RANDOM_TAIL`
- `LOW_CARDINALITY_HIGH_VOLUME`

Run with `modes=all` to cover every enum value.

## Repository Layout

```text
src/main/          Apex entry point and orchestration
src/MSD/           Adaptive MSD bucket planning
src/scatter/       Parallel MSD scatter
src/LSD/           Bucket refinement and work scheduling
src/Tuples/        Tuple projection and tuple-tail logic
src/tinysorts/     Tiny partition sort functions
src/generator/     Data modes and topology generation
src/Tools/         Utilities and verification
src/Comparison/    Optional benchmark comparison harness
data-descriptors.md  Data layout, topology families, and descriptor guide
usage.md           Command-line usage guide
comparison.md      Benchmark comparison guide
operation-execution.md  Operation model and math-paper reference
index.html         Interactive visualizer
app.js             Visualizer model and rendering
styles.css         Visualizer styling
```

## Publication Notes

A.P.E.X. is a research-oriented implementation. It is intended for algorithm
experimentation, performance analysis, and visualization of adaptive radix
dispatch behavior. Results depend heavily on CPU core count, memory bandwidth,
JDK version, JVM flags, and dataset topology.

## License

GNU General Public License v3.0. See [LICENSE](LICENSE).

## Commercial Licensing

A.P.E.X. is published as research software under GPLv3. Commercial licensing is
available by arrangement for organizations that want proprietary integration,
closed-source redistribution, private evaluation, custom support, or other
non-GPL terms.

If your use case requires a separate commercial license, contact the author to
discuss licensing, consulting, or research collaboration.
