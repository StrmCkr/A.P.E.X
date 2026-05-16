package main;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
import generator.initiatedata;
import scatter.scattered;


/*
 * Apex : Adaptive Parallel Entropic Dispatch {MSD-LSD}
 *
 * High-performance entropy-adaptive radix sorting for large-scale 64-bit
 * key/value datasets on modern multi-core processors.
 *
 * Core idea:
 *   Apex dynamically shapes radix partition topology around observed entropy,
 *   selecting optimal MSD bit windows to maximize partition quality,
 *   improve locality, balance parallel work distribution,
 *   and minimize unnecessary memory movement and radix passes.
 *
 * Architectural model:
 *   - Entropy-adaptive MSD partition planning
 *   - Parallel radix scatter and partition execution
 *   - Hybrid MSD-LSD radix pipeline
 *   - Locality-aware memory dispatch and execution routing
 *   - Adaptive in-place and off-heap processing strategies
 *
 * A : Adaptive
 *     - dynamically selects radix windows, partition geometry,
 *       execution thresholds, and processing strategies from
 *       observed key distributions and runtime characteristics
 *
 * P : Parallel
 *     - fully parallel histogramming, scatter, partition execution,
 *       and LSD processing across all available CPU cores
 *
 * E : Entropic
 *     - analyzes entropy distribution across bit regions to identify
 *       high-information radix windows while avoiding degenerate
 *       or low-cardinality partition collapse
 *
 * X : Dispatch
 *     - coordinates adaptive execution paths, partition scheduling,
 *       memory locality, and heap/off-heap execution strategies
 *
 * Entropy determination engine:
 *   Apex uses bitwise statistical analysis to determine effective
 *   entropy regions and eliminate non-contributing radix windows.
 *
 *   Bitwise entropy reduction includes:
 *     - XOR analysis to detect changing bit regions
 *     - AND reduction to identify globally shared bit patterns
 *     - OR reduction to identify globally active bit regions
 *     - Constant-prefix elimination
 *     - Sparse-entropy detection
 *     - Delayed-entropy recognition
 *     - Duplicate-density detection
 *
 *   These analyses allow Apex to:
 *     - skip entropy-free radix passes
 *     - avoid pathological partition fanout
 *     - reduce tiny partition explosion
 *     - improve cache locality and partition balance
 *     - dynamically relocate MSD extraction windows
 *
 * Hybrid radix pipeline:
 *   - MSD radix scatter for global entropy-guided partitioning
 *   - LSD radix refinement for efficient in-partition ordering
 *   - Adaptive tuple-cycle optimization for compact entropy domains
 *   - Dynamic fallback paths for pathological distributions
 *
 * Features:
 *   - Entropy-aware MSD window planning
 *   - Adaptive radix-width selection
 *   - Runtime auto-tuning of radix geometry and thresholds
 *   - Packed tuple-cycle execution
 *   - Locality-aware partition scheduling
 *   - Hybrid heap and off-heap execution
 *   - Parallel histogram, scatter, and partition processing
 *   - Adaptive tiny-partition handling
 *   - Degenerate partition mitigation
 *
 *  Sparse entropy tuple projection:
 *  - Apex can dynamically remap sparse distributed entropy regions
 *   into compact tuple domains, reducing radix space, histogram size,
 *   and memory traffic for low-density entropy distributions.  
 *  - Tuple-cycle execution is adaptively enabled only when projected
 *   entropy density produces a net locality and radix-efficiency gain.
 *
 * Designed for:
 *   - Large-scale 64-bit sorting workloads
 *   - High-core-count desktop and server processors
 *   - Memory-bandwidth-sensitive workloads
 *   - Adversarial and real-world data distributions
 *
 * Robustness:
 *   Maintains stable behavior across:
 *     - duplicates and low-cardinality datasets
 *     - sparse and delayed entropy
 *     - clustered and skewed partitions
 *     - sorted and partially sorted runs
 *     - pathological radix distributions
 *     - highly imbalanced partition topologies
 *
 * Philosophy:
 *   Apex prioritizes adaptive partition quality, locality,
 *   scalability, and memory efficiency over fixed radix geometry
 *   or microbenchmark specialization.
 *
 *   The architecture is designed for experimentation,
 *   extensibility, and deep runtime adaptability rather than
 *   static one-size-fits-all radix behavior.
 */
 
 
 /* Runtime notes:
 *   --enable-native-access=ALL-UNNAMED
 *   --enable-preview
 *   -XX:MaxDirectMemorySize=80g
 *   -Xmx16G
 *
 * Options:
 *   mode          Single data mode for default run.
 *   modes         Multiple modes or mode range for default run.
 *   records       Default-run record count, list, or range.
 *   msd           MSD_BITS auto-tune range.
 *   lsd           LSD_BITS auto-tune range.
 *   tiny          Tiny partition threshold auto-tune range.
 *   tupleBits     Entropy tuple dimension cap for direct tuple-space passes.
 *   config        Locked config: MSD_BITS,LSD_BITS,TINY. Bypasses auto-tune.
 *   threads       Worker thread count. auto uses available processors.
 *   largePermits  Concurrent off-heap large partitions. auto uses max(1, threads/8).
 *   workStealing  Dynamic largest-first LSD bucket scheduling. true by default.
 *   tuplePacking  Packs sparse entropy coordinates into LSD passes. false by default.
 *   heapScratch   Max heap-backed scratch records per worker before off-heap path.
 *   tuneRecords   Record count used during the single auto-tune pass.
 *   warmupRecords Selected-config warmup record count.
 *   sweep         Run the full mode sweep instead of the default selected-mode run.
 *   sweepRecords  Records per mode during sweep.
 */
public class Apex {

    public static int THREADS = Integer.getInteger(
            "apex.threads",
            Runtime.getRuntime().availableProcessors()
    );

   public static final int RECORD_BYTES = 16;
   public static final long SEED = 0x9E3779B97F4A7C15L;
   public static final long DEFAULT_RECORDS = 500_000_000L;
   public static final long TUNE_RECORDS = 10_000_000L;
   public static final long WARMUP_RECORDS = 100_000_000L;
   public static int MAX_HEAP_SCRATCH_RECORDS = Integer.getInteger("apex.heapScratchRecords", 1_048_576);
   public static final int TUNE_WARMUPS = 1;
   public  static final int TUNE_RUNS = 3;
   public static final int MAX_DIRECT_TUPLE_BITS = 16;
   public static final int SMALL_TUPLE_LOOKUP_BITS = 5;

   public    static ExecutorService POOL;
   public  static boolean LSD_WORK_STEALING = true;
   public  static boolean PACKED_TUPLE_CYCLES = Boolean.getBoolean("apex.tuplePacking");
   public  static int DIRECT_TUPLE_BITS = Integer.getInteger("apex.tupleBits", 9);
   public static int LARGE_PARTITION_PERMIT_COUNT = 1;
   public static Semaphore LARGE_PARTITION_PERMITS = new Semaphore(1);
   public static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG_UNALIGNED;

   public  static final byte BUCKET_EMPTY = 0;
   public  static final byte BUCKET_ALL_EQUAL = 1;
   public  static final byte BUCKET_MIXED = 2;
     

    public static class Scratch {
        public long[] k1 = new long[1024];
        public long[] v1 = new long[1024];
        public long[] k2 = new long[1024];
        public  long[] v2 = new long[1024];
        public int[] counts;

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
    }
  

    public static void main(String[] args) throws Exception {
        Options options = runoptions.parseOptions(args);

        THREADS = options.threads;
        LSD_WORK_STEALING = options.lsdWorkStealing;
        PACKED_TUPLE_CYCLES = options.packedTupleCycles;
        DIRECT_TUPLE_BITS = options.directTupleBits;
        MAX_HEAP_SCRATCH_RECORDS = options.heapScratchRecords;
        tools.configureLargePartitionPermits(options);
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
            System.out.println("Packed tuple cycles: " + PACKED_TUPLE_CYCLES);
            System.out.println("Direct tuple bits: " + DIRECT_TUPLE_BITS);
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
                runModesOnce(selectedConfig, options.sweepRecords, alignment, Arrays.asList(DataMode.values()));
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
        System.out.println("Data mode: " + mode);
        System.out.println("Config: " + cfg);
        System.out.printf("Data buffers: %.2f GiB (src+dst; src reused as LSD scratch)%n",
                dataBufferGiB(records));

        if (records == 0 || mode == DataMode.EMPTY) {
            System.out.println("EMPTY dataset - nothing to do");
            return;
        }

        warmUp("Selected-config warmup", cfg, Math.min(records, options.warmupRecords), alignment, mode);

        try (Arena arena = Arena.ofShared()) {
            long bytes = tools.bytesForRecords(records);
            MemorySegment src = arena.allocate(bytes, alignment);
            MemorySegment dst = arena.allocate(bytes, alignment);

            System.out.println("Initializing: " + records + " records");
            initiatedata.initData(src, records, mode);

            Timer total = Timer.start();

            Timer t0 = Timer.start();
            MsdBucketPlan msdPlan = msdbucketplan.buildAdaptiveMsdBucketPlan(src, records, cfg);
            report("MSD adaptive plan", records, t0);
            printMsdBucketStats(msdPlan, cfg);

            t0 = Timer.start();
            scattered.scatterIntoMsdBuckets(src, dst, records, msdPlan, cfg);
            report("MSD scatter", records, t0);

            t0 = Timer.start();
            lsdbucketplan.sortMsdBucketsWithLsdRadix(src, dst, msdPlan, cfg);
            report("LSD radix partitions", records, t0);

            report("TOTAL", records, total);

            verifier.verifyLight(dst, records, mode);
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
                3L * Long.BYTES         // cycle-plan references, approximate compressed/oop-safe
        );
        long scratchBytes = (long) THREADS * MAX_HEAP_SCRATCH_RECORDS * 4L * Long.BYTES;

        long estimate = histBytes + masksBytes + scatterOffsetBytes + countsBytes + planBytes + scratchBytes;

        return estimate < heapMax / 3;
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
            MsdBucketPlan msdPlan = msdbucketplan.buildAdaptiveMsdBucketPlan(src, testN, cfg);

            scattered.scatterIntoMsdBuckets(src, dst, testN, msdPlan, cfg);
            lsdbucketplan.sortMsdBucketsWithLsdRadix(src, dst, msdPlan, cfg);

            double sec = elapsed(start);

            verifier.verifyLight(dst, testN, mode, announceVerify);

            return sec;
        }
    }


  static void printMsdBucketStats(MsdBucketPlan plan, Config cfg) {
        int nonEmpty = 0;
        int max = 0;
        long total = 0;
        int directTupleCandidates = 0;
        int plannedTupleTails = 0;
        int plannedTinySorts = 0;

        for (int i = 0; i < cfg.msdBucketCount; i++) {
            int s = plan.sizes[i];
            if (s != 0) {
                nonEmpty++;
            }
            if (s > max) {
                max = s;
            }
            if (plan.bucketFlags[i] == BUCKET_MIXED && tuples.tupleSpaceFitsDirectPass(plan.variableMasks[i])) {
                directTupleCandidates++;
            }
            if (plan.tupleTailMasks[i] != 0L) {
                plannedTupleTails++;
            }
            if (plan.bucketFlags[i] == BUCKET_MIXED &&
                    s > 1 &&
                    s < cfg.tinyPartitionThreshold &&
                    plan.cycleCounts[i] == 0 &&
                    plan.tupleTailMasks[i] == 0L) {
                plannedTinySorts++;
            }
            total += s;
        }

        System.out.println("MSD buckets non-empty: " + nonEmpty + " / " + cfg.msdBucketCount);
        System.out.println("MSD bucket shift: " + plan.msdShift +
                " (bits " + plan.msdShift + ".." + (plan.msdShift + cfg.msdBits - 1) + ")");
        System.out.println("Largest MSD bucket: " + max);
        System.out.println("Direct tuple candidates: " + directTupleCandidates);
        System.out.println("Planned tiny sorts: " + plannedTinySorts);
        System.out.println("Planned tuple tails: " + plannedTupleTails);
        System.out.println("Total bucketed: " + total);
    }

 

    public static void runModesOnce(Config cfg, long n, long alignment, List<DataMode> modes) throws Exception {
        System.out.println("=== SINGLE FULL MODE RUN START ===");
        System.out.println("Records per mode: " + n);
        System.out.println("Config: " + cfg);
        System.out.println("Modes: " + modes.size());

        for (DataMode mode : modes) {
            if (mode == DataMode.EMPTY) {
                System.out.println("[SKIP] EMPTY");
                continue;
            }

            long startAll = System.nanoTime();

            try (Arena arena = Arena.ofShared()) {
                long bytes = tools.bytesForRecords(n);
                MemorySegment src = arena.allocate(bytes, alignment);
                MemorySegment dst = arena.allocate(bytes, alignment);

                initiatedata.initData(src, n, mode);

                MsdBucketPlan plan = msdbucketplan.buildAdaptiveMsdBucketPlan(src, n, cfg);

                scattered.scatterIntoMsdBuckets(src, dst, n, plan, cfg);

                lsdbucketplan.sortMsdBucketsWithLsdRadix(src, dst, plan, cfg);

                verifier.verifyLight(dst, n, mode);
            }

            double sec = (System.nanoTime() - startAll) / 1e9;
            double mps = (n / sec) / 1e6;

            System.out.printf("MODE %-28s | %.3f sec | %.2f M/sec%n",
                    mode, sec, mps);
        }

        System.out.println("=== SINGLE FULL MODE RUN COMPLETE ===");
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

    static long[] sampleKeys(MemorySegment seg, long start, int size, int maxSamples) {
        int n = Math.min(size, maxSamples);
        long[] out = new long[n];

        long step = Math.max(1, size / n);

        for (int i = 0; i < n; i++) {
            long idx = start + ((long) i * step);
            long p = idx << 4;
            out[i] = seg.get(LONG, p);
        }

        return out;
    }
}
