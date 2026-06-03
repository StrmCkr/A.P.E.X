package main;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import java.util.concurrent.Semaphore;
import LSD.lsdbucketplan;
import MSD.msdbucketplan;
import MSD.msdbucketplan.MsdBucketPlan;
import Tools.tools;
import Tools.verifier;
import Tuples.tuples;
import config.configurations;
import config.configurations.Config;
import config.runoptions;
import config.runoptions.Options;
import generator.DataMode;
import generator.DataTopology;
import generator.initiatedata;
import scatter.scattered;


/*
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

 */
public class Apex {

    public static int THREADS = Integer.getInteger(
            "apex.threads",
            Runtime.getRuntime().availableProcessors()
    );

    public static int threadsPerDomainGroup() {
        return Math.max(1, THREADS / 2);
    }
    
    // 🚀 Symmetrical Hardware-Adaptive Species: Instantly visible across all classes
    @SuppressWarnings({"removal", "preview"})
    public static final jdk.incubator.vector.VectorSpecies<Long> L_SPECIES = jdk.incubator.vector.LongVector.SPECIES_PREFERRED;
    public static final int RECORDS_PER_REG = L_SPECIES.vectorByteSize() >>> 4;


   public static final int RECORD_BYTES = 16;
   public static final long SEED = 0x9E3779B97F4A7C15L;
   public static final long DEFAULT_RECORDS = 100_000_000L;
   public static final long TUNE_RECORDS = 10_000_000L;
   public static final long WARMUP_RECORDS = 100_000_000L;
   public static int MAX_HEAP_SCRATCH_RECORDS = Integer.getInteger("apex.heapScratchRecords", 1_048_576);
   public static final int TUNE_WARMUPS = 1;
   public  static final int TUNE_RUNS = 3;
   public static final int MAX_DIRECT_TUPLE_BITS = 16;
   public static final int SMALL_TUPLE_LOOKUP_BITS = 10;

   public    static ExecutorService POOL;
   public  static boolean LSD_WORK_STEALING = true;
   public  static boolean PACKED_TUPLE_CYCLES = Boolean.getBoolean("apex.tuplePacking");
   public  static boolean LOCAL_MSD_REPARTITION = Boolean.parseBoolean(
           System.getProperty("apex.localMsd", "true")
   );
   public  static int DIRECT_TUPLE_BITS = Integer.getInteger("apex.tupleBits", 9);
   public  static int LOCAL_MSD_MIN_RECORDS = Integer.getInteger("apex.localMsdMinRecords", 1_048_576);
   public  static int LOCAL_MSD_MIN_PASSES = Integer.getInteger("apex.localMsdMinPasses", 2);
   public  static int LOCAL_MSD_MIN_WINDOW_BITS = Integer.getInteger("apex.localMsdMinWindowBits", 2);
   public  static int LOCAL_MSD_BITS = Integer.getInteger("apex.localMsdBits", 0);
   public static int LOCAL_MSD_MIN_SHARE_DIVISOR = Integer.getInteger(
	        "apex.localMsdMinShareDivisor", 
	        Math.max(2, THREADS / 2)
	);
   public static int WORK_STEAL_BATCH = Integer.getInteger(
	        "apex.workBatch", 
	        Math.max(4, THREADS / 2)
	);   
  
   
   public static int LARGE_PARTITION_PERMIT_COUNT = 1;
   public static Semaphore LARGE_PARTITION_PERMITS = new Semaphore(1);
   public static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG_UNALIGNED;

   public  static final byte BUCKET_EMPTY = 0;
   public  static final byte BUCKET_ALL_EQUAL = 1;
   public  static final byte BUCKET_MIXED = 2;
   public  static final byte BUCKET_ASCENDING = 3;
   public  static final byte BUCKET_DESCENDING = 4;
     
   public static boolean isL3CacheLocal(int currentWorkerId, int targetWorkerId) {
       int domainGroupSize = threadsPerDomainGroup();
       int currentDomainGroup = currentWorkerId / domainGroupSize;
       int targetDomainGroup = targetWorkerId / domainGroupSize;
       return (currentDomainGroup == targetDomainGroup);
   }

   public static class Scratch {
       // --- Cache Line Padding Group 1: Isolate Temporary Swapping Arrays ---
       private long p01, p02, p03, p04, p05, p06, p07, p08; 
       public long[] k1 = new long[1024];
       public long[] v1 = new long[1024];
       private long p09, p10, p11, p12, p13, p14, p15, p16;
       public long[] k2 = new long[1024];
       public long[] v2 = new long[1024];
       

       // --- Cache Line Padding Group 2: Isolate Dense Core Histograms ---
       private long p17, p18, p19, p20, p21, p22, p23, p24;
       public int[] counts;
       public int[] bucketStarts = new int[0];
       public int[] bucketOffsets = new int[0];
       public int[] bucketEnds = new int[0];

       // --- Cache Line Padding Group 3: Isolate Entropic Masks ---
       private long p25, p26, p27, p28, p29, p30, p31, p32;
       public long[] bucketOrMasks = new long[0];
       public long[] bucketAndMasks = new long[0];

       // --- Cache Line Padding Group 4: Isolate Radix Cycle Plans ---
       private long p33, p34, p35, p36, p37, p38, p39, p40;
       public final int[] cycleShifts = new int[64];
       public final int[] cycleMasks = new int[64];
       public final long[] cycleBitMasks = new long[64];
       public final long[] cycleTuplePlans = new long[64];
       private long p41, p42, p43, p44, p45, p46, p47, p48;

       public Scratch(int lsdRadix) {
           counts = new int[lsdRadix];
       }

       public void ensure(int n) {
           if (k1.length >= n) {
               return;
           }
           int cap = Integer.highestOneBit(n - 1) << 1;
           k1 = new long[cap];
           v1 = new long[cap];
           k2 = new long[cap];
           v2 = new long[cap];
       }

       public void ensureCounts(int n) {
           if (counts.length < n) {
               counts = new int[n];
           }
       }

       public void ensureBucketScratch(int n) {
           if (bucketStarts.length >= n) {
               return;
           }
           bucketStarts = new int[n];
           bucketOffsets = new int[n];
           bucketEnds = new int[n];
           bucketOrMasks = new long[n];
           bucketAndMasks = new long[n];
       }
   }

  

    public static void main(String[] args) throws Exception {
        Options options = runoptions.parseOptions(args);

        runoptions.applyApexSettings(options);
        POOL = Executors.newFixedThreadPool(THREADS);

        try {
            boolean isApexHardware =
                    System.getProperty("os.arch").contains("amd64") &&
                    Runtime.getRuntime().availableProcessors() >= 32;

            long alignment = isApexHardware ? 2 * 1024 * 1024 : 64;

            System.out.println("Mode: " + (isApexHardware ? "APEX: {2x1024*1024} " : "Apex: {64}"));
            System.out.println("Threads: " + THREADS);
            System.out.println("Large partition permits: " + LARGE_PARTITION_PERMIT_COUNT);
            System.out.println("LSD work stealing: " + LSD_WORK_STEALING);
            System.out.println("Work steal batch: " + WORK_STEAL_BATCH);
            System.out.println("MSD scatter mode:src->dst");
            System.out.println("Packed tuple cycles: " + (PACKED_TUPLE_CYCLES ? "forced" : "auto"));
            System.out.println("Local MSD repartition: " + LOCAL_MSD_REPARTITION +
                    " minRecords=" + LOCAL_MSD_MIN_RECORDS +
                    " minPasses=" + LOCAL_MSD_MIN_PASSES +
                    " bits=" + (LOCAL_MSD_BITS > 0 ? LOCAL_MSD_BITS : "config") +
                    " minWindowBits=" + LOCAL_MSD_MIN_WINDOW_BITS +
                    " minShare=1/" + LOCAL_MSD_MIN_SHARE_DIVISOR);            
            System.out.println("Direct tuple bits: " + DIRECT_TUPLE_BITS);
            System.out.println("Small tuple lookup bits: 2.." + SMALL_TUPLE_LOOKUP_BITS);
            System.out.println("Heap scratch records: " + MAX_HEAP_SCRATCH_RECORDS);
            System.out.println("Tune records: " + options.tuneRecords);
            System.out.println("MSD range: " + options.minMsdBits + ".." + options.maxMsdBits);
            System.out.println("LSD range: " + options.minLsdBits + ".." + options.maxLsdBits);
            System.out.println("Tiny range: " + options.minTiny + ".." + options.maxTiny);
            if (options.lockedConfig != null) {
                System.out.println("Locked config: " + options.lockedConfig);
            }

            Config selectedConfig = selectRunConfig(alignment, options);

            if (options.sweep) {
                for (DataMode mode : DataMode.values()) {
                    runOneMode(mode, options.sweepRecords, alignment, selectedConfig, options);
                }
            } else {
                for (long records : options.recordsList) {
                    for (DataMode mode : options.modes) {
                        runOneMode(mode, records, alignment, selectedConfig, options);
                    }
                }
            }
        } finally {
            if (POOL != null) {
                POOL.shutdown();
            }
        }
    }

    static Config selectRunConfig(long alignment, Options options) throws Exception {
        if (options.lockedConfig != null) {
            return options.lockedConfig;
        }

        DataMode tuneMode = tools.firstNonEmptyMode(options.modes);
        long recordBasis = tools.firstPositiveRecord(options.recordsList);

        if (options.sweep) {
            recordBasis = tools.firstPositive(recordBasis, options.sweepRecords);
            if (tuneMode == null) {
                tuneMode = DataMode.RANDOM;
            }
        }

        if (tuneMode == null || recordBasis <= 0) {
            Config fallback = configurations.defaultConfig();
            System.out.println("Auto-tune skipped (no non-empty run requested). Using " + fallback);
            return fallback;
        }

        System.out.println("Auto-tune mode: " + tuneMode);
        System.out.println("Auto-tune record basis: " + recordBasis);
        return autoTune(alignment, tuneMode, recordBasis, options);
    }
  

   
    static void runOneMode(
            DataMode mode,
            long records,
            long alignment,
            Config cfg,
            Options options
    ) throws Exception {
        System.out.println("Records: " + records);
        DataTopology.printTopology(mode);
        System.out.println("Config: " + cfg);
        System.out.printf("Data buffers: %.2f GiB max (src+dst; dst allocated lazily)%n",
                    dataBufferGiB(records));

        if (records == 0 || mode == DataMode.EMPTY) {
            System.out.println("EMPTY dataset - nothing to do");
            return;
        }

        warmUp("Selected-config warmup", cfg, Math.min(records, options.warmupRecords), alignment, mode);

        try (Arena arena = Arena.ofShared()) {
            long bytes = tools.bytesForRecords(records);
            MemorySegment src = arena.allocate(bytes, alignment);
            MemorySegment dst = null;

            System.out.println("Initializing: " + records + " records");
            initiatedata.initData(src, records, mode);

            Timer total = Timer.start();
            int inputOrder = detectInputOrderFastPath(src, records, true);
            MemorySegment sorted = null;

            if (inputOrder == tools.ORDER_ASCENDING) {
                sorted = src;
                System.out.println("MSD plan/scatter/LSD skipped (input already ascending)");
            } else if (inputOrder == tools.ORDER_DESCENDING) {
                dst = arena.allocate(bytes, alignment);
                Timer t0 = Timer.start();
                tools.reverseCopyRecords(src, 0, dst, 0, records);
                sorted = dst;
                report("Descending reverse", records, t0);
            } else {               
                Timer t0 = Timer.start();
                MsdBucketPlan msdPlan = msdbucketplan.buildAdaptiveMsdBucketPlan(src, records, cfg);
                report("MSD adaptive plan", records, t0);
                printMsdBucketStats(msdPlan, cfg);

                MemorySegment lsdScratch = src;
                if (msdPlan.inputAscending) {
                    sorted = src;
                    System.out.println("MSD/LSD skipped (input already ascending)");
                } else if (msdPlan.inputDescending) {
                    dst = arena.allocate(bytes, alignment);
                    t0 = Timer.start();
                    tools.reverseCopyRecords(src, 0, dst, 0, records);
                    sorted = dst;
                    report("Descending reverse", records, t0);
                } else if (sourceAlreadyFinal(msdPlan, cfg)) {
                    sorted = src;
                    System.out.println("MSD scatter skipped (source already final)");
                } else if (singleBucketCanRefineInSource(msdPlan, records, cfg)) {
                    sorted = src;
                    if (planNeedsOffHeapScratch(msdPlan, cfg)) {
                        dst = arena.allocate(bytes, alignment);
                        lsdScratch = dst;
                    }
                    System.out.println("MSD scatter skipped (single bucket; refining source)");
                } else {
                    dst = arena.allocate(bytes, alignment);
                    sorted = dst;
                    t0 = Timer.start();
                    scattered.scatterIntoMsdBuckets(src, dst, records, msdPlan, cfg);
                    report("MSD scatter", records, t0);
                }

                if (!msdPlan.inputAscending && !msdPlan.inputDescending && planNeedsRefinement(msdPlan, cfg)) {
                    t0 = Timer.start();
                    lsdbucketplan.sortMsdBucketsWithLsdRadix(lsdScratch, sorted, msdPlan, cfg);
                    report("Bucket refinement", records, t0);
                } else if (!msdPlan.inputAscending && !msdPlan.inputDescending) {
                    System.out.println("Bucket refinement skipped");
                }
            }

            report("TOTAL", records, total);

            verifier.verify(sorted, records, mode);
        }
    } 

    static double elapsed(long start) {
        return (System.nanoTime() - start) / 1e9;
    }

    static double dataBufferGiB(long records) {
        return (records * (double) RECORD_BYTES * 2.0) / (1024.0 * 1024.0 * 1024.0);
    }

    static Config autoTune(long alignment, DataMode mode, long records, Options options) throws Exception {
        if (mode == DataMode.EMPTY || records <= 0) {
            System.out.println("Auto-tune skipped (EMPTY/no-record mode)");
            return configurations.defaultConfig();
        }

        Config[] candidates = buildConfigCandidates(
                options.minMsdBits,
                options.maxMsdBits,
                options.minLsdBits,
                options.maxLsdBits,
                options.minTiny,
                options.maxTiny
        );

        long testN = Math.min(records, options.tuneRecords);
        warmUp("Auto-tune warmup", configurations.defaultConfig(), testN, alignment, mode);

        System.out.println("Auto-tuning on subset: " + testN + " / full=" + records);
        System.out.println("Auto-tune repeats: warmups=" + TUNE_WARMUPS + " runs=" + TUNE_RUNS + " score=median");

        Config best = candidates[0];
        double bestSec = Double.POSITIVE_INFINITY;
        boolean measuredAny = false;

        for (Config cfg : candidates) {
            if (!memoryLooksReasonable(cfg)) {
                System.out.println("SKIP " + cfg + " memory estimate too high");
                continue;
            }

            double sec = benchmarkCandidateMedian(cfg, testN, alignment, mode);
            measuredAny = true;
            System.out.printf("TUNE %-42s median %.3f sec | %.2f M rec/sec (subset=%d)%n",
                    cfg, sec, (testN / sec) / 1e6, testN);

            if (sec < bestSec) {
                bestSec = sec;
                best = cfg;
            }
        }

        if (!measuredAny) {
            best = configurations.defaultConfig();
            System.out.println("Auto-tune found no memory-safe candidates. Using " + best);
        }

        System.out.println("Auto-tune picked: " + best);
        return best;
    }

    static double benchmarkCandidateMedian(Config cfg, long testN, long alignment, DataMode mode) throws Exception {
        return benchmarkCandidateMedian(cfg, testN, alignment, mode, TUNE_WARMUPS, TUNE_RUNS);
    }

    static double benchmarkCandidateMedian(
            Config cfg,
            long testN,
            long alignment,
            DataMode mode,
            int warmups,
            int runs
    ) throws Exception {
        for (int i = 0; i < warmups; i++) {
            benchmarkCandidate(cfg, testN, alignment, mode, false);
        }

        double[] measured = new double[runs];

        for (int i = 0; i < runs; i++) {
            measured[i] = benchmarkCandidate(cfg, testN, alignment, mode, false);
        }

        Arrays.sort(measured);
        return measured[measured.length >>> 1];
    }

    static void warmUp(String label, Config cfg, long warmN, long alignment, DataMode mode) throws Exception {
        if (warmN <= 0 || mode == DataMode.EMPTY) {
            return;
        }

        System.out.println(label + " on " + warmN + " records using " + cfg + "...");
        double sec = benchmarkCandidate(cfg, warmN, alignment, mode, false);
        System.out.printf("%s %.3f sec | %.2f M/sec%n", label, sec, (warmN / sec) / 1e6);
    }

    static Config[] buildConfigCandidates(
            int minMsdBits,
            int maxMsdBits,
            int minLsdBits,
            int maxLsdBits,
            int minTinyPartitionThreshold,
            int maxTinyPartitionThreshold
    ) {
        ArrayList<Config> candidates = new ArrayList<>();

        for (int msdBits = minMsdBits; msdBits <= maxMsdBits; msdBits++) {
            for (int lsdBits = minLsdBits; lsdBits <= maxLsdBits; lsdBits++) {
                for (int tiny = minTinyPartitionThreshold; tiny <= maxTinyPartitionThreshold; tiny <<= 1) {
                    candidates.add(new Config(msdBits, lsdBits, tiny));
                }
            }
        }

        return candidates.toArray(new Config[0]);
    }

    static boolean memoryLooksReasonable(Config cfg) {
        long heapMax = Runtime.getRuntime().maxMemory();
        long bucketThreadCells = (long) THREADS * cfg.msdBucketCount;

        long histBytes = bucketThreadCells * Integer.BYTES;
        long masksBytes = bucketThreadCells * 2L * Long.BYTES;
        long scatterOffsetBytes = bucketThreadCells * Integer.BYTES;
        long countsBytes = (long) THREADS * Math.max(cfg.lsdRadix, tuples.directTupleRadixCap()) * Integer.BYTES;
        long planBytes = (long) cfg.msdBucketCount * (
                Long.BYTES +            // starts
                Integer.BYTES +         // sizes
                Byte.BYTES +            // flags
                Long.BYTES +            // variable masks
                Long.BYTES +            // tuple-tail masks
                Long.BYTES +            // tuple-tail plans
                Integer.BYTES +         // cycle counts
                4L * Long.BYTES         // cycle-plan references, approximate compressed/oop-safe
        );
        long scratchBytes = (long) THREADS * MAX_HEAP_SCRATCH_RECORDS * 4L * Long.BYTES;

        long estimate = histBytes + masksBytes + scatterOffsetBytes + countsBytes + planBytes + scratchBytes;

        return estimate < heapMax / 3;
    }

    public static MemorySegment tryInputOrderFastPath(
            MemorySegment src,
            MemorySegment dst,
            long records,
            boolean announce
    ) throws Exception {
        int order = detectInputOrderFastPath(src, records, announce);
        if (order == tools.ORDER_MIXED) {
            return null;
        }

        if (order == tools.ORDER_ASCENDING) {
            if (announce) {
                System.out.println("MSD plan/scatter/LSD skipped (input already ascending)");
            }
            return src;
        }

        Timer reverse = announce ? Timer.start() : null;        

        tools.reverseCopyRecords(src, 0, dst, 0, records);
        if (announce) {
            report("Descending reverse", records, reverse);
        }
        return dst;
    }

    public static int detectInputOrderFastPath(
            MemorySegment src,
            long records,
            boolean announce
    ) {
        Timer probe = announce ? Timer.start() : null;
        int order = tools.quickOrderProbe(src, records);

        if (announce) {
            report("Input order probe", records, probe);
        }

        if (order == tools.ORDER_MIXED) {
            return tools.ORDER_MIXED;
        }

        Timer scan = announce ? Timer.start() : null;
        order = tools.detectMonotonicOrder(src, records);

        if (announce) {
            report("Input order scan", records, scan);
        }

        return order;
    }

    static double benchmarkCandidate(
            Config cfg,
            long testN,
            long alignment,
            DataMode mode,
            boolean announceVerify
    ) throws Exception {
        try (Arena arena = Arena.ofShared()) {
            long bytes = tools.bytesForRecords(testN);
            MemorySegment src = arena.allocate(bytes, alignment);
            MemorySegment dst = arena.allocate(bytes, alignment);

            initiatedata.initData(src, testN, mode);

            long start = System.nanoTime();
            MemorySegment sorted = sortPipeline(src, dst, testN, cfg);

            double sec = elapsed(start);

            verifier.verify(sorted, testN, mode, announceVerify);

            return sec;
        }
    }
   

    public static MemorySegment sortPipeline(
            MemorySegment src,
            MemorySegment dst,
            long records,
            Config cfg
    ) throws Exception {
        MemorySegment sorted = tryInputOrderFastPath(src, dst, records, false);

        if (sorted != null) {
            return sorted;
        }

        MsdBucketPlan msdPlan = msdbucketplan.buildAdaptiveMsdBucketPlan(src, records, cfg);

        sorted = dst;
        MemorySegment lsdScratch = src;
        if (msdPlan.inputAscending) {
            sorted = src;
        } else if (msdPlan.inputDescending) {
            tools.reverseCopyRecords(src, 0, dst, 0, records);
            sorted = dst;
        } else if (sourceAlreadyFinal(msdPlan, cfg)) {
            sorted = src;
        } else if (singleBucketCanRefineInSource(msdPlan, records, cfg)) {
            sorted = src;
            lsdScratch = dst;
        } else {
            scattered.scatterIntoMsdBuckets(src, dst, records, msdPlan, cfg);
        }

        if (!msdPlan.inputAscending && !msdPlan.inputDescending && planNeedsRefinement(msdPlan, cfg)) {
            lsdbucketplan.sortMsdBucketsWithLsdRadix(lsdScratch, sorted, msdPlan, cfg);
        }

        return sorted;
    }

    public static boolean sourceAlreadyFinal(MsdBucketPlan plan, Config cfg) {
        int nonEmpty = 0;

        for (int b = 0; b < cfg.msdBucketCount; b++) {
            if (plan.sizes[b] == 0) {
                continue;
            }

            nonEmpty++;
            if (nonEmpty > 1 || lsdbucketplan.bucketHasLsdWork(plan, cfg, b)) {
                return false;
            }
        }

        return true;
    }

    public static boolean singleBucketCanRefineInSource(MsdBucketPlan plan, long records, Config cfg) {
        if (plan.hasLocalMsd) {
            return false;
        }

        int nonEmpty = 0;
        for (int b = 0; b < cfg.msdBucketCount; b++) {
            if (plan.sizes[b] == 0) {
                continue;
            }

            nonEmpty++;
            if (nonEmpty > 1 || plan.starts[b] != 0L || plan.sizes[b] != records) {
                return false;
            }
        }

        return nonEmpty == 1;
    }

    public static boolean planNeedsRefinement(MsdBucketPlan plan, Config cfg) {
        for (int b = 0; b < cfg.msdBucketCount; b++) {
            if (lsdbucketplan.bucketHasScheduledLsdWork(plan, cfg, b)) {
                return true;
            }
        }

        return false;
    }

    public static boolean planNeedsOffHeapScratch(MsdBucketPlan plan, Config cfg) {
        for (int b = 0; b < cfg.msdBucketCount; b++) {
            if (!lsdbucketplan.bucketHasLsdWork(plan, cfg, b)) {
                continue;
            }

            int localMsdShift = plan.localMsdShifts[b];
            if (localMsdShift >= 0) {
                int[] childSizes = plan.localSizes[b];
                long[] childVariableMasks = plan.localVariableMasks[b];

                for (int child = 0; child < childSizes.length; child++) {
                    int childSize = childSizes[child];
                    boolean childDescending = plan.localDescending[b] != null &&
                            plan.localDescending[b][child];
                    if (!lsdbucketplan.localChildHasLsdWork(plan, b, child) ||
                            childDescending ||
                            childSize <= MAX_HEAP_SCRATCH_RECORDS ||
                            childSize < cfg.tinyPartitionThreshold ||
                            childVariableMasks[child] == 0L) {
                        continue;
                    }

                    return true;
                }

                continue;
            }

            if (plan.sizes[b] > MAX_HEAP_SCRATCH_RECORDS &&
                    plan.sizes[b] >= cfg.tinyPartitionThreshold &&
                    (plan.cycleCounts[b] > 0 || plan.tupleTailMasks[b] != 0L)) {
                return true;
            }
        }

        return false;
    }

    static void printMsdBucketStats(MsdBucketPlan plan, Config cfg) {
        int nonEmpty = 0;
        int empty = 0;
        int allEqual = 0;
        int mixed = 0;
        int max = 0;
        long total = 0;
        int refinementBuckets = 0;
        int tinySortBuckets = 0;
        int directTupleBuckets = 0;
        int lsdCycleBuckets = 0;
        int tupleTailPasses = 0;
        int localMsdBuckets = 0;
        int offHeapRefinements = 0;
        long lsdCyclePasses = 0;
        long contiguousCyclePasses = 0;
        long sparseCyclePasses = 0;
        long smallTupleCyclePasses = 0;
        int refinementWorkItems = 0;
        int localMsdChildWorkItems = 0;
        int[] tempCycleShifts = new int[64];
        int[] tempCycleMasks = new int[64];
        long[] tempCycleBitMasks = new long[64];
        long[] tempCycleTuplePlans = new long[64];

        for (int i = 0; i < cfg.msdBucketCount; i++) {
            int s = plan.sizes[i];
            if (s != 0) {
                nonEmpty++;
            }
            if (s > max) {
                max = s;
            }

            if (plan.bucketFlags[i] == BUCKET_EMPTY) {
                empty++;
            } else if (plan.bucketFlags[i] == BUCKET_ALL_EQUAL) {
                allEqual++;
            } else if (plan.bucketFlags[i] == BUCKET_MIXED) {
                mixed++;
            }

            if (lsdbucketplan.bucketHasScheduledLsdWork(plan, cfg, i)) {
                refinementBuckets++;
            }

            if (lsdbucketplan.bucketHasLsdWork(plan, cfg, i)) {

                if (plan.bucketDescending[i]) {
                    refinementWorkItems++;
                } else if (s < cfg.tinyPartitionThreshold) {
                    refinementWorkItems++;
                    tinySortBuckets++;
                } else {
                    int localMsdShift = plan.localMsdShifts[i];

                    if (localMsdShift >= 0) {
                        localMsdBuckets++;
                        int[] childSizes = plan.localSizes[i];
                        long[] childVariableMasks = plan.localVariableMasks[i];

                        for (int child = 0; child < childSizes.length; child++) {
                            if (!lsdbucketplan.localChildHasLsdWork(plan, i, child)) {
                                continue;
                            }

                            int childSize = childSizes[child];
                            long childVariableMask = childVariableMasks[child];
                            boolean childDescending = plan.localDescending[i] != null &&
                                    plan.localDescending[i][child];

                            refinementWorkItems++;
                            localMsdChildWorkItems++;

                            if (childDescending) {
                                continue;
                            }

                            if (childSize < cfg.tinyPartitionThreshold) {
                                tinySortBuckets++;
                                continue;
                            }

                            if (childSize > MAX_HEAP_SCRATCH_RECORDS) {
                                offHeapRefinements++;
                            }

                            if (tuples.tupleSpaceFitsDirectPass(childVariableMask, childSize)) {
                                directTupleBuckets++;
                                continue;
                            }

                            int cycles = lsdbucketplan.buildLsdCyclePlan(
                                    childVariableMask,
                                    cfg,
                                    localMsdShift,
                                    tempCycleShifts,
                                    tempCycleMasks,
                                    tempCycleBitMasks,
                                    tempCycleTuplePlans
                            );
                            int plannedCycles = tuples.plannedCyclePrefixBeforeTupleTail(
                                    childVariableMask,
                                    tempCycleBitMasks,
                                    cycles,
                                    childSize
                            );
                            long tupleTailMask = tuples.tupleTailMaskAfterPrefix(
                                    childVariableMask,
                                    tempCycleBitMasks,
                                    plannedCycles,
                                    childSize
                            );

                            if (plannedCycles > 0) {
                                lsdCycleBuckets++;
                                lsdCyclePasses += plannedCycles;

                                for (int cycle = 0; cycle < plannedCycles; cycle++) {
                                    if (tempCycleShifts[cycle] >= 0) {
                                        contiguousCyclePasses++;
                                    } else {
                                        sparseCyclePasses++;
                                    }

                                    if (tempCycleTuplePlans[cycle] != 0L) {
                                        smallTupleCyclePasses++;
                                    }
                                }
                            }

                            if (plannedCycles > 0 && tupleTailMask != 0L) {
                                tupleTailPasses++;
                            }
                        }

                        total += s;
                        continue;
                    }

                    if (s > MAX_HEAP_SCRATCH_RECORDS) {
                        offHeapRefinements++;
                    }

                    refinementWorkItems++;

                    int cycles = plan.cycleCounts[i];
                    if (cycles == 0 && plan.tupleTailMasks[i] != 0L) {
                        directTupleBuckets++;
                    } else {
                        if (cycles > 0) {
                            lsdCycleBuckets++;
                            lsdCyclePasses += cycles;

                            int[] shifts = plan.cycleShifts[i];
                            long[] tuplePlans = plan.cycleTuplePlans[i];

                            for (int cycle = 0; cycle < cycles; cycle++) {
                                if (shifts[cycle] >= 0) {
                                    contiguousCyclePasses++;
                                } else {
                                    sparseCyclePasses++;
                                }

                                if (tuplePlans != null && tuplePlans[cycle] != 0L) {
                                    smallTupleCyclePasses++;
                                }
                            }
                        }

                        if (cycles > 0 && plan.tupleTailMasks[i] != 0L) {
                            tupleTailPasses++;
                        }
                    }
                }
            }

            total += s;
        }

        System.out.println("MSD buckets non-empty: " + nonEmpty + " / " + cfg.msdBucketCount);
        System.out.println("MSD bucket states: mixed=" + mixed +
                " all-equal=" + allEqual + " empty=" + empty);
        System.out.println("MSD bucket shift: " + plan.msdShift +
                " (bits " + plan.msdShift + ".." + (plan.msdShift + cfg.msdBits - 1) + ")");
        System.out.println("Largest MSD bucket: " + max);
        System.out.println("Top MSD buckets needing refinement: " + refinementBuckets);
        System.out.println("Refinement work items: " + refinementWorkItems +
                " (local-MSD children=" + localMsdChildWorkItems + ")");
        System.out.println("Tiny-sort buckets: " + tinySortBuckets);
        System.out.println("Direct tuple partitions: " + directTupleBuckets);
        System.out.println("LSD cycle buckets: " + lsdCycleBuckets);
        System.out.println("LSD cycle passes: " + lsdCyclePasses +
                " (contiguous=" + contiguousCyclePasses +
                " sparse=" + sparseCyclePasses +
                " small-tuple=" + smallTupleCyclePasses + ")");
        System.out.println("Tuple-tail passes: " + tupleTailPasses);
        System.out.println("Local MSD repartition buckets: " + localMsdBuckets);
        System.out.println("Off-heap refinements: " + offHeapRefinements);
        System.out.println("Total bucketed: " + total);
    }

    static final class Timer {
        long start;

        static Timer start() {
            Timer t = new Timer();
            t.start = System.nanoTime();
            return t;
        }

        double seconds() {
            return (System.nanoTime() - start) / 1e9;
        }
    }

    static void report(String label, long records, Timer t) {
        double sec = t.seconds();
        double rate = (records / sec) / 1e6;
        System.out.printf("%-30s %.3f sec | %.2f M rec/sec%n", label, sec, rate);
    }

    public static void sort(MemorySegment src, MemorySegment dst, long n, Config cfg) throws Exception {
        sortPipeline(src, dst, n, cfg);
    }
}
