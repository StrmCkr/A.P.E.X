package io.github.strmckr.apex.engine;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import io.github.strmckr.apex.config.SortConfig;
import io.github.strmckr.apex.config.RunOptions;
import io.github.strmckr.apex.config.RunOptions.Options;
import io.github.strmckr.apex.generator.DataMode;
import io.github.strmckr.apex.generator.DataTopology;
import io.github.strmckr.apex.generator.DataInitializer;
import io.github.strmckr.apex.lsd.LsdBucketPlan;
import io.github.strmckr.apex.msd.MsdBucketPlanner;
import io.github.strmckr.apex.msd.MsdBucketPlanner.MsdBucketPlan;
import io.github.strmckr.apex.reporting.Reporter;
import io.github.strmckr.apex.reporting.Reporters;
import io.github.strmckr.apex.scatter.Scattered;
import io.github.strmckr.apex.tools.Tools;
import io.github.strmckr.apex.tools.Verifier;
import io.github.strmckr.apex.tuning.AutoTuner;
import io.github.strmckr.apex.tuples.Tuples;


/**
 * Command-line sorting engine and internal pipeline implementation.
 */
public class ApexEngine {

    public static int THREADS = Integer.getInteger(
            "apex.threads",
            Runtime.getRuntime().availableProcessors()
    );

    public static int threadsPerDomainGroup() {
        return Math.max(1, THREADS / 2);
    }
    
    // Shared vector species used by copy and scan routines.
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

   public    static ExecutorService POOL;
   public  static boolean ORDER_FAST_PATH = Boolean.parseBoolean(
           System.getProperty("apex.orderFastPath", "false")
   );
   public  static boolean SIGNED_KEYS = Boolean.parseBoolean(
           System.getProperty("apex.signedKeys", "false")
   );
   public  static long KEY_ORDER_XOR = SIGNED_KEYS ? Long.MIN_VALUE : 0L;
   public  static boolean LSD_WORK_STEALING = true;
   public  static boolean PACKED_TUPLE_CYCLES = Boolean.getBoolean("apex.tuplePacking");
   public  static boolean LOCAL_MSD_REPARTITION = Boolean.parseBoolean(
           System.getProperty("apex.localMsd", "true")
   );
   public  static int DIRECT_TUPLE_BITS = Integer.getInteger("apex.tupleBits", 9);
   public  static int DIRECT_TUPLE_CONTIGUOUS_BITS = Integer.getInteger(
           "apex.contiguousTupleBits",
           MAX_DIRECT_TUPLE_BITS
   );
   public  static final int DEFAULT_DIRECT_TUPLE_IN_PLACE_MAX_RECORDS = Integer.getInteger(
           "apex.defaultDirectTupleInPlaceMaxRecords",
           262_144
   );
   public  static final int DEFAULT_DIRECT_TUPLE_MANY_PARTITION_MIN = Integer.getInteger(
           "apex.defaultDirectTupleManyPartitions",
           16
   );
   public  static int DIRECT_TUPLE_IN_PLACE_MAX_RECORDS = Integer.getInteger(
           "apex.directTupleInPlaceMaxRecords",
           DEFAULT_DIRECT_TUPLE_IN_PLACE_MAX_RECORDS
   );
   public  static int DIRECT_TUPLE_MANY_PARTITION_MIN = Integer.getInteger(
           "apex.directTupleManyPartitions",
           DEFAULT_DIRECT_TUPLE_MANY_PARTITION_MIN
   );
   public  static boolean STAGGER_TUPLE_CYCLES = Boolean.parseBoolean(
           System.getProperty("apex.staggerTuples", "true")
   );
   public  static boolean STAGGER_TUPLE_COST_MODEL = Boolean.parseBoolean(
           System.getProperty("apex.staggerTupleCostModel", "true")
   );
   public  static int STAGGER_TUPLE_BITS = Integer.getInteger("apex.staggerTupleBits", 16);
   public  static int STAGGER_TUPLE_MIN_RECORDS = Integer.getInteger("apex.staggerTupleMinRecords", 0);
   public  static int LSD_HEAP_UNROLL = Integer.getInteger("apex.lsdHeapUnroll", 0);
   public  static int LSD_HEAP_UNROLL_MIN_RECORDS = Integer.getInteger("apex.lsdHeapUnrollMinRecords", 4_096);
   public  static int LOCAL_MSD_MIN_RECORDS = Integer.getInteger("apex.localMsdMinRecords", 65_536);
   public  static int LOCAL_MSD_MIN_PASSES = Integer.getInteger("apex.localMsdMinPasses", 2);
   public  static int LOCAL_MSD_MIN_WINDOW_BITS = Integer.getInteger("apex.localMsdMinWindowBits", 2);
   public  static int LOCAL_MSD_BITS = Integer.getInteger("apex.localMsdBits", 0);
   public  static int LOCAL_MSD_MAX_CHILDREN = Integer.getInteger("apex.localMsdMaxChildren", 8_192);
   public static int LOCAL_MSD_MIN_SHARE_DIVISOR = Integer.getInteger(
	        "apex.localMsdMinShareDivisor", 
	        Math.max(2, THREADS / 2)
	);
   public static boolean DOMINANT_CORE_FAST_PATH = Boolean.parseBoolean(
           System.getProperty("apex.dominantCore", "true")
   );
   public static int DOMINANT_CORE_SAMPLE_RECORDS = Integer.getInteger(
           "apex.dominantCoreSampleRecords", 262_144
   );
   public static int DOMINANT_CORE_CANDIDATES = Integer.getInteger(
           "apex.dominantCoreCandidates", 64
   );
   public static int DOMINANT_CORE_MIN_SHARE_PERCENT = Integer.getInteger(
           "apex.dominantCoreMinShare", 80
   );
   public static int DOMINANT_KEY_MIN_SHARE_DIVISOR = Integer.getInteger(
           "apex.dominantKeyMinShareDivisor", 1024
   );
   public static int WORK_STEAL_BATCH = Integer.getInteger(
	        "apex.workBatch", 
	        Math.max(4, THREADS / 2)
	);   
   public static boolean DESCENDING_SCATTER_FAST_PATH = Boolean.parseBoolean(
           System.getProperty("apex.descendingScatter", "true")
   );
  
   
   public static int LARGE_PARTITION_PERMIT_COUNT = 1;
   public static Semaphore LARGE_PARTITION_PERMITS = new Semaphore(1);
   public static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG_UNALIGNED;

   public  static final byte BUCKET_EMPTY = 0;
   public  static final byte BUCKET_ALL_EQUAL = 1;
   public  static final byte BUCKET_MIXED = 2;
   public  static final byte BUCKET_ASCENDING = 3;
   public  static final byte BUCKET_DESCENDING = 4;

   public static Reporter reporter() {
       return Reporters.current();
   }

   public static void setReporter(Reporter reporter) {
       Reporters.set(reporter);
   }
     
   public static boolean isL3CacheLocal(int currentWorkerId, int targetWorkerId) {
       int domainGroupSize = threadsPerDomainGroup();
       int currentDomainGroup = currentWorkerId / domainGroupSize;
       int targetDomainGroup = targetWorkerId / domainGroupSize;
       return (currentDomainGroup == targetDomainGroup);
   }

   public static class Scratch {
       // Cache Line Padding Group 1: Isolate Temporary Swapping Arrays
       private long p01, p02, p03, p04, p05, p06, p07, p08; 
       public long[] k1 = new long[1024];
       public long[] v1 = new long[1024];
       private long p09, p10, p11, p12, p13, p14, p15, p16;
       public long[] k2 = new long[1024];
       public long[] v2 = new long[1024];
       

       // Cache Line Padding Group 2: Isolate Dense Core Histograms
       private long p17, p18, p19, p20, p21, p22, p23, p24;
       public int[] counts;
        public int[] bucketStarts = new int[0];
        public int[] bucketOffsets = new int[0];
        public int[] bucketEnds = new int[0];

       // Cache Line Padding Group 3: Isolate Radix Cycle Plans
       private long p25, p26, p27, p28, p29, p30, p31, p32;
       private long p33, p34, p35, p36, p37, p38, p39, p40;
       public final int[] cycleShifts = new int[64];
       public final int[] cycleMasks = new int[64];
       public final long[] cycleBitMasks = new long[64];
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
       }
   }

  

    public static void main(String[] args) throws Exception {
        Options options = RunOptions.parseOptions(args);

        Reporters.configure(options.reportingMode, options.reportFile);
        RunOptions.applyApexSettings(options);
        POOL = Executors.newFixedThreadPool(THREADS);

        try {
            boolean isApexHardware =
                    System.getProperty("os.arch").contains("amd64") &&
                    Runtime.getRuntime().availableProcessors() >= 32;

            long alignment = isApexHardware ? 2 * 1024 * 1024 : 64;

            printRuntimeHeader(isApexHardware, options);

            SortConfig selectedConfig = AutoTuner.selectRunConfig(alignment, options);

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
            Reporters.close();
        }
    }

    static void runOneMode(
            DataMode mode,
            long records,
            long alignment,
            SortConfig cfg,
            Options options
    ) throws Exception {
        if (Reporters.isDetailEnabled()) {
            Reporters.println("Records: " + records);
            DataTopology.printTopology(mode);
            Reporters.println("Config: " + cfg);
            Reporters.printf("Data buffers: %.2f GiB max (src+dst; dst allocated lazily)%n",
                    dataBufferGiB(records));
        } else if (Reporters.isSummary()) {
            Reporters.println();
            Reporters.println("Run: mode=" + mode + " records=" + records + " config=" + cfg);
            Reporters.printf("Buffers: %.2f GiB max (source + destination)%n", dataBufferGiB(records));
        }

        if (records == 0 || mode == DataMode.EMPTY) {
            Reporters.println("EMPTY dataset - nothing to do");
            return;
        }

        AutoTuner.warmUp("Selected-config warmup", cfg, Math.min(records, options.warmupRecords), alignment, mode);

        try (Arena arena = Arena.ofShared()) {
            long bytes = Tools.bytesForRecords(records);
            MemorySegment src = arena.allocate(bytes, alignment);
            MemorySegment dst = null;

            if (Reporters.isDetailEnabled()) {
                Reporters.println("Initializing: " + records + " records");
            }
            DataInitializer.initData(src, records, mode);

            Timer total = Timer.start();
            int inputOrder = ORDER_FAST_PATH
                    ? detectInputOrderFastPath(src, records, true)
                    : Tools.ORDER_MIXED;
            MemorySegment sorted = null;

            if (inputOrder == Tools.ORDER_ASCENDING) {
                sorted = src;
                Reporters.println("MSD plan/scatter/LSD skipped (input already ascending)");
            } else if (inputOrder == Tools.ORDER_DESCENDING) {
                dst = allocateDestination(arena, bytes, alignment, records);
                Timer t0 = Timer.start();
                Tools.reverseCopyRecords(src, 0, dst, 0, records);
                sorted = dst;
                report("Descending reverse", records, t0);
            } else {               
                Timer t0 = Timer.start();
                MsdBucketPlan msdPlan = MsdBucketPlanner.buildAdaptiveMsdBucketPlan(src, records, cfg);
                report("MSD adaptive plan", records, t0);
                printMsdBucketStats(msdPlan, cfg);

                MemorySegment lsdScratch = src;
                if (msdPlan.inputAscending) {
                    sorted = src;
                    Reporters.println("MSD/LSD skipped (input already ascending)");
                } else if (msdPlan.inputDescending) {
                    dst = allocateDestination(arena, bytes, alignment, records);
                    t0 = Timer.start();
                    if (DESCENDING_SCATTER_FAST_PATH) {
                        Scattered.scatterIntoMsdBuckets(src, dst, records, msdPlan, cfg);
                    } else {
                        Tools.reverseCopyRecords(src, 0, dst, 0, records);
                    }
                    sorted = dst;
                    report(DESCENDING_SCATTER_FAST_PATH ? "Descending scatter" : "Descending reverse", records, t0);
                } else if (sourceAlreadyFinal(msdPlan, cfg)) {
                    sorted = src;
                    Reporters.println("MSD scatter skipped (source already final)");
                } else if (singleBucketCanRefineInSource(msdPlan, records, cfg)) {
                    sorted = src;
                    if (planNeedsOffHeapScratch(msdPlan, cfg)) {
                        dst = allocateDestination(arena, bytes, alignment, records);
                        lsdScratch = dst;
                    }
                    Reporters.println("MSD scatter skipped (single bucket; refining source)");
                } else {
                    dst = allocateDestination(arena, bytes, alignment, records);
                    sorted = dst;
                    t0 = Timer.start();
                    Scattered.scatterIntoMsdBuckets(src, dst, records, msdPlan, cfg);
                    report("MSD scatter", records, t0);
                }

                if (!msdPlan.inputAscending && !msdPlan.inputDescending && planNeedsRefinement(msdPlan, cfg)) {
                    t0 = Timer.start();
                    LsdBucketPlan.sortMsdBucketsWithLsdRadix(lsdScratch, sorted, msdPlan, cfg);
                    report("Bucket refinement", records, t0);
                } else if (!msdPlan.inputAscending && !msdPlan.inputDescending) {
                    Reporters.println("Bucket refinement skipped");
                }
            }

            report("TOTAL", records, total);

            Verifier.verify(sorted, records, mode);
        }
    } 

    static double elapsed(long start) {
        return (System.nanoTime() - start) / 1e9;
    }

    static double dataBufferGiB(long records) {
        return (records * (double) RECORD_BYTES * 2.0) / (1024.0 * 1024.0 * 1024.0);
    }

    private static void printRuntimeHeader(boolean isApexHardware, Options options) {
        if (Reporters.isSummary()) {
            Reporters.println("A.P.E.X run summary");
            Reporters.println("Mode: " + (isApexHardware ? "APEX aligned" : "standard alignment") +
                    " | threads=" + THREADS +
                    " | keyOrder=" + (SIGNED_KEYS ? "signed" : "unsigned"));
            Reporters.println("Tuples: " + (PACKED_TUPLE_CYCLES ? "forced" : "auto") +
                    " | tupleBits=" + DIRECT_TUPLE_BITS +
                    " | workStealing=" + LSD_WORK_STEALING);
            Reporters.println("Tune range: msd=" + options.minMsdBits + ".." + options.maxMsdBits +
                    " lsd=" + options.minLsdBits + ".." + options.maxLsdBits +
                    " tiny=" + options.minTiny + ".." + options.maxTiny);
            if (options.lockedConfig != null) {
                Reporters.println("Locked config: " + options.lockedConfig);
            }
            return;
        }

        if (!Reporters.isDetailEnabled()) {
            return;
        }

        Reporters.println("Mode: " + (isApexHardware ? "APEX: {2x1024*1024} " : "Apex: {64}"));
        Reporters.println("Threads: " + THREADS);
        Reporters.println("Large partition permits: " + LARGE_PARTITION_PERMIT_COUNT);
        Reporters.println("Input order fast path: " + ORDER_FAST_PATH);
        Reporters.println("Key order: " + (SIGNED_KEYS ? "signed" : "unsigned"));
        Reporters.println("LSD work stealing: " + LSD_WORK_STEALING);
        Reporters.println("Work steal batch: " + WORK_STEAL_BATCH);
        Reporters.println("MSD scatter mode:src->dst");
        Reporters.println("Descending scatter fast path: " + DESCENDING_SCATTER_FAST_PATH);
        Reporters.println("Packed tuple cycles: " + (PACKED_TUPLE_CYCLES ? "forced" : "auto"));
        Reporters.println("Stagger tuple cycles: " + STAGGER_TUPLE_CYCLES +
                " bits=" + STAGGER_TUPLE_BITS +
                " mode=" + (STAGGER_TUPLE_COST_MODEL ? "cost" : "fixed") +
                " minRecords=" + STAGGER_TUPLE_MIN_RECORDS);
        Reporters.println("Local MSD repartition: " + LOCAL_MSD_REPARTITION +
                " minRecords=" + LOCAL_MSD_MIN_RECORDS +
                " minPasses=" + LOCAL_MSD_MIN_PASSES +
                " bits=" + (LOCAL_MSD_BITS > 0 ? LOCAL_MSD_BITS : "config") +
                " maxChildren=" + LOCAL_MSD_MAX_CHILDREN +
                " minWindowBits=" + LOCAL_MSD_MIN_WINDOW_BITS +
                " minShare=1/" + LOCAL_MSD_MIN_SHARE_DIVISOR);
        Reporters.println("Direct tuple bits: " + DIRECT_TUPLE_BITS);
        Reporters.println("Contiguous direct tuple bits: " + DIRECT_TUPLE_CONTIGUOUS_BITS);
        Reporters.println("Direct tuple in-place max records: " +
                (DIRECT_TUPLE_IN_PLACE_MAX_RECORDS == Integer.MAX_VALUE
                        ? "unlimited"
                        : Integer.toString(DIRECT_TUPLE_IN_PLACE_MAX_RECORDS)));
        Reporters.println("Direct tuple many-partition in-place min: " +
                DIRECT_TUPLE_MANY_PARTITION_MIN);
        Reporters.println("Dominant core fast path: " + DOMINANT_CORE_FAST_PATH +
                " minShare=" + DOMINANT_CORE_MIN_SHARE_PERCENT + "%" +
                " candidates=" + DOMINANT_CORE_CANDIDATES);
        Reporters.println("LSD heap unroll: " +
                (LSD_HEAP_UNROLL == 0 ? "adaptive>= " + LSD_HEAP_UNROLL_MIN_RECORDS : LSD_HEAP_UNROLL));
        Reporters.println("Heap scratch records: " + MAX_HEAP_SCRATCH_RECORDS);
        Reporters.println("Tune records: " + options.tuneRecords);
        Reporters.println("MSD range: " + options.minMsdBits + ".." + options.maxMsdBits);
        Reporters.println("LSD range: " + options.minLsdBits + ".." + options.maxLsdBits);
        Reporters.println("Tiny range: " + options.minTiny + ".." + options.maxTiny);
        if (options.lockedConfig != null) {
            Reporters.println("Locked config: " + options.lockedConfig);
        }
    }

    static MemorySegment allocateDestination(Arena arena, long bytes, long alignment, long records) {
        Timer t = Timer.start();
        MemorySegment dst = arena.allocate(bytes, alignment);
        report("Destination allocation", records, t);
        return dst;
    }

    public static MemorySegment tryInputOrderFastPath(
            MemorySegment src,
            MemorySegment dst,
            long records,
            boolean announce
    ) throws Exception {
        int order = detectInputOrderFastPath(src, records, announce);
        if (order == Tools.ORDER_MIXED) {
            return null;
        }

        if (order == Tools.ORDER_ASCENDING) {
            if (announce) {
                Reporters.println("MSD plan/scatter/LSD skipped (input already ascending)");
            }
            return src;
        }

        Timer reverse = announce ? Timer.start() : null;        

        Tools.reverseCopyRecords(src, 0, dst, 0, records);
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
        int order = Tools.quickOrderProbe(src, records);

        if (announce) {
            report("Input order probe", records, probe);
        }

        if (order == Tools.ORDER_MIXED) {
            return Tools.ORDER_MIXED;
        }

        Timer scan = announce ? Timer.start() : null;
        order = Tools.detectMonotonicOrder(src, records);

        if (announce) {
            report("Input order scan", records, scan);
        }

        return order;
    }

    public static MemorySegment sortPipeline(
            MemorySegment src,
            MemorySegment dst,
            long records,
            SortConfig cfg
    ) throws Exception {
        MemorySegment sorted = ORDER_FAST_PATH
                ? tryInputOrderFastPath(src, dst, records, false)
                : null;

        if (sorted != null) {
            return sorted;
        }

        MsdBucketPlan msdPlan = MsdBucketPlanner.buildAdaptiveMsdBucketPlan(src, records, cfg);

        sorted = dst;
        MemorySegment lsdScratch = src;
        if (msdPlan.inputAscending) {
            sorted = src;
        } else if (msdPlan.inputDescending) {
            if (DESCENDING_SCATTER_FAST_PATH) {
                Scattered.scatterIntoMsdBuckets(src, dst, records, msdPlan, cfg);
            } else {
                Tools.reverseCopyRecords(src, 0, dst, 0, records);
            }
            sorted = dst;
        } else if (sourceAlreadyFinal(msdPlan, cfg)) {
            sorted = src;
        } else if (singleBucketCanRefineInSource(msdPlan, records, cfg)) {
            sorted = src;
            lsdScratch = dst;
        } else {
            Scattered.scatterIntoMsdBuckets(src, dst, records, msdPlan, cfg);
        }

        if (!msdPlan.inputAscending && !msdPlan.inputDescending && planNeedsRefinement(msdPlan, cfg)) {
            LsdBucketPlan.sortMsdBucketsWithLsdRadix(lsdScratch, sorted, msdPlan, cfg);
        }

        return sorted;
    }

    public static boolean sourceAlreadyFinal(MsdBucketPlan plan, SortConfig cfg) {
        int nonEmpty = 0;

        for (int b = 0; b < cfg.msdBucketCount; b++) {
            if (plan.sizes[b] == 0) {
                continue;
            }

            nonEmpty++;
            if (nonEmpty > 1 || LsdBucketPlan.bucketHasLsdWork(plan, cfg, b)) {
                return false;
            }
        }

        return true;
    }

    public static boolean singleBucketCanRefineInSource(MsdBucketPlan plan, long records, SortConfig cfg) {
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

    public static boolean planNeedsRefinement(MsdBucketPlan plan, SortConfig cfg) {
        for (int b = 0; b < cfg.msdBucketCount; b++) {
            if (LsdBucketPlan.bucketHasScheduledLsdWork(plan, cfg, b)) {
                return true;
            }
        }

        return false;
    }

    public static boolean planNeedsOffHeapScratch(MsdBucketPlan plan, SortConfig cfg) {
        boolean preferDirectTupleInPlace = LsdBucketPlan.preferManyDirectTuplePartitionsInPlace(plan, cfg);

        for (int b = 0; b < cfg.msdBucketCount; b++) {
            if (!LsdBucketPlan.bucketHasLsdWork(plan, cfg, b)) {
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
                    if (!LsdBucketPlan.localChildHasLsdWork(plan, b, child) ||
                            childDescending ||
                            childSize <= MAX_HEAP_SCRATCH_RECORDS ||
                            childSize < cfg.tinyPartitionThreshold ||
                            childVariableMasks[child] == 0L) {
                        continue;
                    }

                    if (Tuples.tupleSpaceFitsDirectPass(childVariableMasks[child], childSize) &&
                            Tuples.directTupleUsesInPlace(childSize, preferDirectTupleInPlace)) {
                        continue;
                    }

                    return true;
                }

                continue;
            }

            int size = plan.sizes[b];
            if (size > MAX_HEAP_SCRATCH_RECORDS &&
                    size >= cfg.tinyPartitionThreshold &&
                    (plan.cycleCounts[b] > 0 || plan.tupleTailMasks[b] != 0L)) {
                if (plan.cycleCounts[b] == 0 &&
                        Tuples.tupleSpaceFitsDirectPass(plan.tupleTailMasks[b], size) &&
                        Tuples.directTupleUsesInPlace(size, preferDirectTupleInPlace)) {
                    continue;
                }

                return true;
            }
        }

        return false;
    }

    static void printMsdBucketStats(MsdBucketPlan plan, SortConfig cfg) {
        if (!Reporters.isDetailEnabled()) {
            return;
        }

        int nonEmpty = 0;
        int empty = 0;
        int allEqual = 0;
        int mixed = 0;
        int max = 0;
        long total = 0;
        int refinementBuckets = 0;
        int tinySortBuckets = 0;
        int directTupleBuckets = 0;
        int directTupleInPlaceBuckets = 0;
        int directTupleOffHeapBuckets = 0;
        int lsdCycleBuckets = 0;
        int tupleTailPasses = 0;
        int localMsdBuckets = 0;
        int offHeapRefinements = 0;
        long lsdCyclePasses = 0;
        long contiguousCyclePasses = 0;
        long sparseCyclePasses = 0;
        long lsdCycleCounterSlots = 0;
        long directTupleCounterSlots = 0;
        long tupleTailCounterSlots = 0;
        int maxLsdCycleBits = 0;
        int maxDirectTupleBits = 0;
        int maxTupleTailBits = 0;
        int refinementWorkItems = 0;
        int localMsdChildWorkItems = 0;
        int largestRefinementItem = 0;
        int[] tempCycleShifts = new int[64];
        int[] tempCycleMasks = new int[64];
        long[] tempCycleBitMasks = new long[64];
        boolean preferDirectTupleInPlace = LsdBucketPlan.preferManyDirectTuplePartitionsInPlace(plan, cfg);

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

            if (LsdBucketPlan.bucketHasScheduledLsdWork(plan, cfg, i)) {
                refinementBuckets++;
            }

            if (LsdBucketPlan.bucketHasLsdWork(plan, cfg, i)) {

                if (plan.bucketDescending[i]) {
                    refinementWorkItems++;
                    largestRefinementItem = Math.max(largestRefinementItem, s);
                } else if (s < cfg.tinyPartitionThreshold) {
                    refinementWorkItems++;
                    largestRefinementItem = Math.max(largestRefinementItem, s);
                    tinySortBuckets++;
                } else {
                    int localMsdShift = plan.localMsdShifts[i];

                    if (localMsdShift >= 0) {
                        localMsdBuckets++;
                        int[] childSizes = plan.localSizes[i];
                        long[] childVariableMasks = plan.localVariableMasks[i];

                        for (int child = 0; child < childSizes.length; child++) {
                            if (!LsdBucketPlan.localChildHasLsdWork(plan, i, child)) {
                                continue;
                            }

                            int childSize = childSizes[child];
                            long childVariableMask = childVariableMasks[child];
                            boolean childDescending = plan.localDescending[i] != null &&
                                    plan.localDescending[i][child];

                            refinementWorkItems++;
                            localMsdChildWorkItems++;
                            largestRefinementItem = Math.max(largestRefinementItem, childSize);

                            if (childDescending) {
                                continue;
                            }

                            if (childSize < cfg.tinyPartitionThreshold) {
                                tinySortBuckets++;
                                continue;
                            }

                            if (Tuples.tupleSpaceFitsDirectPass(childVariableMask, childSize)) {
                                int directBits = Long.bitCount(childVariableMask);
                                maxDirectTupleBits = Math.max(maxDirectTupleBits, directBits);
                                directTupleCounterSlots += 1L << directBits;
                                directTupleBuckets++;
                                if (Tuples.directTupleUsesInPlace(childSize, preferDirectTupleInPlace)) {
                                    directTupleInPlaceBuckets++;
                                } else {
                                    directTupleOffHeapBuckets++;
                                }
                                continue;
                            }

                            if (childSize > MAX_HEAP_SCRATCH_RECORDS) {
                                offHeapRefinements++;
                            }

                            int cycles = LsdBucketPlan.buildLsdCyclePlan(
                                    childVariableMask,
                                    cfg,
                                    localMsdShift,
                                    childSize,
                                    tempCycleShifts,
                                    tempCycleMasks,
                                    tempCycleBitMasks
                            );
                            int plannedCycles = Tuples.plannedCyclePrefixBeforeTupleTail(
                                    childVariableMask,
                                    tempCycleBitMasks,
                                    cycles,
                                    childSize
                            );
                            long tupleTailMask = Tuples.tupleTailMaskAfterPrefix(
                                    childVariableMask,
                                    tempCycleBitMasks,
                                    plannedCycles,
                                    childSize
                            );

                            if (plannedCycles > 0) {
                                lsdCycleBuckets++;
                                lsdCyclePasses += plannedCycles;

                                for (int cycle = 0; cycle < plannedCycles; cycle++) {
                                    int cycleBits = Integer.bitCount(tempCycleMasks[cycle]);
                                    maxLsdCycleBits = Math.max(maxLsdCycleBits, cycleBits);
                                    lsdCycleCounterSlots += (long) tempCycleMasks[cycle] + 1L;

                                    if (tempCycleShifts[cycle] >= 0) {
                                        contiguousCyclePasses++;
                                    } else {
                                        sparseCyclePasses++;
                                    }
                                }
                            }

                            if (plannedCycles > 0 && tupleTailMask != 0L) {
                                int tailBits = Long.bitCount(tupleTailMask);
                                maxTupleTailBits = Math.max(maxTupleTailBits, tailBits);
                                tupleTailCounterSlots += 1L << tailBits;
                                tupleTailPasses++;
                            }
                        }

                        total += s;
                        continue;
                    }

                    refinementWorkItems++;
                    largestRefinementItem = Math.max(largestRefinementItem, s);

                    int cycles = plan.cycleCounts[i];
                    if (cycles == 0 && plan.tupleTailMasks[i] != 0L) {
                        int directBits = Long.bitCount(plan.tupleTailMasks[i]);
                        maxDirectTupleBits = Math.max(maxDirectTupleBits, directBits);
                        directTupleCounterSlots += 1L << directBits;
                        directTupleBuckets++;
                        if (Tuples.directTupleUsesInPlace(s, preferDirectTupleInPlace)) {
                            directTupleInPlaceBuckets++;
                        } else {
                            directTupleOffHeapBuckets++;
                        }
                    } else {
                        if (s > MAX_HEAP_SCRATCH_RECORDS) {
                            offHeapRefinements++;
                        }

                        if (cycles > 0) {
                            lsdCycleBuckets++;
                            lsdCyclePasses += cycles;

                            int[] shifts = plan.cycleShifts[i];
                            int[] masks = plan.cycleMasks[i];

                            for (int cycle = 0; cycle < cycles; cycle++) {
                                int cycleBits = Integer.bitCount(masks[cycle]);
                                maxLsdCycleBits = Math.max(maxLsdCycleBits, cycleBits);
                                lsdCycleCounterSlots += (long) masks[cycle] + 1L;

                                if (shifts[cycle] >= 0) {
                                    contiguousCyclePasses++;
                                } else {
                                    sparseCyclePasses++;
                                }
                            }
                        }

                        if (cycles > 0 && plan.tupleTailMasks[i] != 0L) {
                            int tailBits = Long.bitCount(plan.tupleTailMasks[i]);
                            maxTupleTailBits = Math.max(maxTupleTailBits, tailBits);
                            tupleTailCounterSlots += 1L << tailBits;
                            tupleTailPasses++;
                        }
                    }
                }
            }

            total += s;
        }

        Reporters.println("MSD buckets non-empty: " + nonEmpty + " / " + cfg.msdBucketCount);
        Reporters.println("MSD bucket states: mixed=" + mixed +
                " all-equal=" + allEqual + " empty=" + empty);
        Reporters.println("MSD bucket shift: " + plan.msdShift +
                " (bits " + plan.msdShift + ".." + (plan.msdShift + cfg.msdBits - 1) + ")");
        Reporters.println("Largest MSD bucket: " + max);
        Reporters.println("Top MSD buckets needing refinement: " + refinementBuckets);
        Reporters.println("Refinement work items: " + refinementWorkItems +
                " (local-MSD children=" + localMsdChildWorkItems + ")");
        Reporters.println("Largest refinement item: " + largestRefinementItem);
        if (plan.localMsdAttachSeconds > 0.0) {
            Reporters.printf("Local MSD planning             %.3f sec%n", plan.localMsdAttachSeconds);
        }
        Reporters.println("Tiny-sort buckets: " + tinySortBuckets);
        Reporters.println("Direct tuple partitions: " + directTupleBuckets);
        if (directTupleBuckets > 0) {
            Reporters.println("Direct tuple width: maxBits=" + maxDirectTupleBits +
                    " counterSlots=" + directTupleCounterSlots);
            Reporters.println("Direct tuple route: in-place=" + directTupleInPlaceBuckets +
                    " off-heap=" + directTupleOffHeapBuckets);
            if (preferDirectTupleInPlace) {
                Reporters.println("Direct tuple route policy: many-partition in-place");
            }
        }
        Reporters.println("LSD cycle buckets: " + lsdCycleBuckets);
        Reporters.println("LSD cycle passes: " + lsdCyclePasses +
                " (contiguous=" + contiguousCyclePasses +
                " sparse=" + sparseCyclePasses + ")");
        if (lsdCyclePasses > 0) {
            Reporters.println("LSD cycle width: maxBits=" + maxLsdCycleBits +
                    " counterSlots=" + lsdCycleCounterSlots);
        }
        Reporters.println("Tuple-tail passes: " + tupleTailPasses);
        if (tupleTailPasses > 0) {
            Reporters.println("Tuple-tail width: maxBits=" + maxTupleTailBits +
                    " counterSlots=" + tupleTailCounterSlots);
        }
        Reporters.println("Local MSD repartition buckets: " + localMsdBuckets);
        Reporters.println("Off-heap refinements: " + offHeapRefinements);
        Reporters.println("Total bucketed: " + total);
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
        Reporters.printf("%-30s %.3f sec | %.2f M rec/sec%n", label, sec, rate);
    }

    public static void sort(MemorySegment src, MemorySegment dst, long n, SortConfig cfg) throws Exception {
        sortPipeline(src, dst, n, cfg);
    }
}
