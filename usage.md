# A.P.E.X. Usage Guide

This guide focuses on running A.P.E.X. from the command line. For project
overview, architecture, and publication notes, see [README.md](README.md).

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
workStealing=true
workBatch=8
```

`workStealing` enables dynamic redistribution of bucket refinement tasks.
`workBatch` controls how many work items are claimed per shared queue hit.

## Tuple Options

```text
tupleBits=9
tuplePacking=true
```

`tupleBits` caps direct tuple-space width. The current maximum cap is `16`.
`tuplePacking=true` forces packed sparse tuple cycles.

## Memory Options

```text
heapScratch=1048576
largePermits=4
```

`heapScratch` controls the max heap-backed scratch records per worker before
larger paths are used. `largePermits` limits concurrent large off-heap
partitions. If `largePermits` is omitted, A.P.E.X. derives a value from the
thread count.

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

## Notes

- A.P.E.X. sorts unsigned 64-bit keys carried in 16-byte key/value records.
- Large runs are memory intensive.
- Record buffers can require roughly `records * 16 * 2` bytes.
- Runtime performance depends on CPU topology, memory bandwidth, JDK version,
  JVM flags, and data distribution.
