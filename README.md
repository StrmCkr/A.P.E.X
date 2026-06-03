# A.P.E.X.

**Adaptive Parallel Extremal Dispatch**

A.P.E.X. is a high-performance Java sorting framework for large fixed-width
64-bit key/value record datasets. It uses descriptor-driven radix planning,
parallel scatter, per-bucket dispatch, tuple projection, tiny-sort fallbacks,
and local refinement to sort unsigned 64-bit keys while avoiding unnecessary
passes over bits that are already resolved.

The project also includes an interactive browser visualizer that explains the
execution plan: source array, MSD scatter, tiny routes, tuple routes, LSD
refinement, global/bucket reverse paths, and final sorted placement.

## Documentation

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

Visualizer controls include:

- `N` record count, up to `1,000,000`
- data type / topology
- MSD bits
- LSD bits
- thread count
- tiny threshold
- tuple bit cap
- tuple on/off
- play, step, reset

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
workStealing=true
workBatch=8
```

Work stealing dynamically redistributes bucket refinement when buckets are
imbalanced.

### Tuples

```bash
tupleBits=9
tuplePacking=true
```

`tupleBits` caps direct tuple-space width. The current maximum cap is `16`.
`tuplePacking=true` forces packed sparse tuple cycles; automatic packed cycles
can still be selected when they reduce cycle count.

### Memory

```bash
heapScratch=1048576
largePermits=4
```

`heapScratch` controls when worker scratch spills toward the off-heap path.
`largePermits` limits concurrent large off-heap partitions. If omitted, A.P.E.X.
chooses an automatic permit count from the thread count.

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
| `workStealing` | Enable/disable LSD work stealing |
| `workBatch` | Work-steal batch size |
| `tupleBits` | Direct tuple bit cap |
| `tuplePacking` | Force packed tuple cycles |
| `heapScratch` | Max heap scratch records per worker |
| `largePermits` | Concurrent large partition permits |
| `localMsdBits` | Override local MSD repartition width |
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
