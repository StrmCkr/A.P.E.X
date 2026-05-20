package Comparison;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import LSD.lsdbucketplan;
import MSD.msdbucketplan;
import MSD.msdbucketplan.MsdBucketPlan;
import Tools.tools;
import Tools.verifier;
import config.configurations;
import config.configurations.Config;
import generator.DataGenerator;
import generator.DataMode;
import generator.initiatedata;
import main.Apex;
import scatter.scattered;

public final class SortComparison {
    static final long DEFAULT_RECORDS = 10_000_000L;
    static final int DEFAULT_RUNS = 3;
    static final int DEFAULT_WARMUPS = 1;
    static final long UNLIMITED = Long.MAX_VALUE;
    static final int INSERTION_THRESHOLD = 64;

    interface Sorter {
        String name();
        String kind();
        long maxRecords();
        void sort(long[] data);
    }

    interface RecordSorter {
        String name();
        String kind();
        long maxRecords();
        void sort(long[] records, int n);
    }

    record Result(String name, String kind, long records, double best, double median, String note) {
    }

    static final class Args {
        DataMode mode = DataMode.RANDOM;
        long records = DEFAULT_RECORDS;
        int runs = DEFAULT_RUNS;
        int warmups = DEFAULT_WARMUPS;
        int threads = Integer.getInteger("apex.threads", Runtime.getRuntime().availableProcessors());
        int tupleBits = Integer.getInteger("apex.tupleBits", 9);
        int heapScratchRecords = Integer.getInteger("apex.heapScratchRecords", 1_048_576);
        int largePermits = 0;
        boolean inPlace = Boolean.parseBoolean(System.getProperty("apex.inPlaceMsd", "false"));
        boolean lsdWorkStealing = true;
        boolean tuplePacking = Boolean.getBoolean("apex.tuplePacking");
        Config config = configurations.defaultConfig();
        String algos = "records";
    }

    public static void main(String[] rawArgs) throws Exception {
        Args args = parseArgs(rawArgs);
        configureApex(args);

        try {
            int n = checkedArrayLength(args.records);
            long alignment = alignment();
            long[] source = generateKeys(n, args.mode, args.records);
            long expectedSum = sum(source);
            long expectedXor = xor(source);
            ArrayList<Benchmark> benchmarks = selectedBenchmarks(args.algos);
            ArrayList<Result> results = new ArrayList<>();

            System.out.println("=== APEX SORT COMPARISON ===");
            System.out.println("Mode: " + args.mode);
            System.out.println("Records: " + args.records);
            System.out.println("Runs: " + args.runs + " warmups=" + args.warmups);
            System.out.println("Threads: " + Apex.THREADS);
            System.out.println("Apex config: " + args.config);
            System.out.println("Default lane: key/value records sorted by unsigned key");
            System.out.println("Optional key-only lane: primitive long[] keys sorted by unsigned key");
            System.out.println();

            for (Benchmark benchmark : benchmarks) {
                if (args.records > benchmark.maxRecords()) {
                    results.add(new Result(
                            benchmark.name(),
                            benchmark.kind(),
                            args.records,
                            Double.NaN,
                            Double.NaN,
                            "skipped over " + benchmark.maxRecords() + " records"
                    ));
                    continue;
                }

                try {
                    for (int i = 0; i < args.warmups; i++) {
                        benchmark.run(source, expectedSum, expectedXor, args.mode, args.records, args.config, alignment);
                    }

                    double[] measured = new double[args.runs];
                    for (int i = 0; i < args.runs; i++) {
                        measured[i] = benchmark.run(
                                source,
                                expectedSum,
                                expectedXor,
                                args.mode,
                                args.records,
                                args.config,
                                alignment
                        );
                    }

                    Arrays.sort(measured);
                    results.add(new Result(
                            benchmark.name(),
                            benchmark.kind(),
                            args.records,
                            measured[0],
                            measured[measured.length >>> 1],
                            "ok"
                    ));
                } catch (Throwable ex) {
                    results.add(new Result(
                            benchmark.name(),
                            benchmark.kind(),
                            args.records,
                            Double.NaN,
                            Double.NaN,
                            "failed: " + ex.getClass().getSimpleName()
                    ));
                }
            }

            printResults(results);
        } finally {
            if (Apex.POOL != null) {
                Apex.POOL.shutdown();
            }
        }
    }

    interface Benchmark {
        String name();
        String kind();
        long maxRecords();

        double run(
                long[] source,
                long expectedSum,
                long expectedXor,
                DataMode mode,
                long records,
                Config cfg,
                long alignment
        ) throws Exception;
    }

    static final class ApexBenchmark implements Benchmark {
        @Override
        public String name() {
            return "apex-records";
        }

        @Override
        public String kind() {
            return "record-sort";
        }

        @Override
        public long maxRecords() {
            return UNLIMITED;
        }

        @Override
        public double run(
                long[] source,
                long expectedSum,
                long expectedXor,
                DataMode mode,
                long records,
                Config cfg,
                long alignment
        ) throws Exception {
            try (Arena arena = Arena.ofShared()) {
                long bytes = tools.bytesForRecords(records);
                MemorySegment src = arena.allocate(bytes, alignment);
                MemorySegment dst = Apex.IN_PLACE_MSD_SCATTER ? src : arena.allocate(bytes, alignment);

                initiatedata.initData(src, records, mode);

                long start = System.nanoTime();
                MemorySegment sorted = Apex.tryInputOrderFastPath(src, dst, records, false);

                if (sorted == null) {
                    MsdBucketPlan msdPlan = msdbucketplan.buildAdaptiveMsdBucketPlan(src, records, cfg);

                    sorted = dst;
                    if (msdPlan.inputAscending) {
                        sorted = src;
                    } else if (msdPlan.inputDescending) {
                        if (Apex.IN_PLACE_MSD_SCATTER) {
                            tools.reverseRecordsInPlace(src, 0, records);
                            sorted = src;
                        } else {
                            tools.reverseCopyRecords(src, 0, dst, 0, records);
                            sorted = dst;
                        }
                    } else if (Apex.sourceAlreadyFinal(msdPlan, cfg)) {
                        sorted = src;
                    } else {
                        if (Apex.IN_PLACE_MSD_SCATTER) {
                            scattered.inPlaceScatterIntoMsdBuckets(src, records, msdPlan, cfg);
                        } else {
                            scattered.scatterIntoMsdBuckets(src, dst, records, msdPlan, cfg);
                        }
                    }

                    MemorySegment lsdScratch = src;
                    if (!msdPlan.inputAscending && !msdPlan.inputDescending &&
                            Apex.IN_PLACE_MSD_SCATTER && Apex.planNeedsOffHeapScratch(msdPlan, cfg)) {
                        lsdScratch = arena.allocate(bytes, alignment);
                    }

                    if (!msdPlan.inputAscending && !msdPlan.inputDescending &&
                            Apex.planNeedsRefinement(msdPlan, cfg)) {
                        lsdbucketplan.sortMsdBucketsWithLsdRadix(lsdScratch, sorted, msdPlan, cfg);
                    }
                }

                double seconds = elapsed(start);
                verifier.verify(sorted, records, mode, false);
                return seconds;
            }
        }
    }

    static final class KeyBenchmark implements Benchmark {
        final Sorter sorter;

        KeyBenchmark(Sorter sorter) {
            this.sorter = sorter;
        }

        @Override
        public String name() {
            return sorter.name();
        }

        @Override
        public String kind() {
            return sorter.kind();
        }

        @Override
        public long maxRecords() {
            return sorter.maxRecords();
        }

        @Override
        public double run(
                long[] source,
                long expectedSum,
                long expectedXor,
                DataMode mode,
                long records,
                Config cfg,
                long alignment
        ) {
            long[] data = Arrays.copyOf(source, source.length);
            long start = System.nanoTime();
            sorter.sort(data);
            double seconds = elapsed(start);
            verifyKeySort(data, expectedSum, expectedXor);
            return seconds;
        }
    }

    static final class RecordBenchmark implements Benchmark {
        final RecordSorter sorter;

        RecordBenchmark(RecordSorter sorter) {
            this.sorter = sorter;
        }

        @Override
        public String name() {
            return sorter.name();
        }

        @Override
        public String kind() {
            return sorter.kind();
        }

        @Override
        public long maxRecords() {
            return sorter.maxRecords();
        }

        @Override
        public double run(
                long[] source,
                long expectedSum,
                long expectedXor,
                DataMode mode,
                long records,
                Config cfg,
                long alignment
        ) {
            long[] data = toInterleavedRecords(source);
            long start = System.nanoTime();
            sorter.sort(data, source.length);
            double seconds = elapsed(start);
            verifyRecordSort(data, expectedSum, expectedXor);
            return seconds;
        }
    }

    static final class JdkObjectRecordBenchmark implements Benchmark {
        final boolean parallel;

        JdkObjectRecordBenchmark(boolean parallel) {
            this.parallel = parallel;
        }

        @Override
        public String name() {
            return parallel ? "jdk-object-parallel-sort" : "jdk-object-arrays-sort";
        }

        @Override
        public String kind() {
            return "object-record";
        }

        @Override
        public long maxRecords() {
            return 50_000_000L;
        }

        @Override
        public double run(
                long[] source,
                long expectedSum,
                long expectedXor,
                DataMode mode,
                long records,
                Config cfg,
                long alignment
        ) {
            RecordItem[] data = new RecordItem[source.length];
            for (int i = 0; i < source.length; i++) {
                data[i] = new RecordItem(source[i], i);
            }

            long start = System.nanoTime();
            if (parallel) {
                Arrays.parallelSort(data, SortComparison::compareRecordItems);
            } else {
                Arrays.sort(data, SortComparison::compareRecordItems);
            }
            double seconds = elapsed(start);
            verifyObjectRecordSort(data, expectedSum, expectedXor);
            return seconds;
        }
    }

    static final class RecordItem {
        final long key;
        final long value;

        RecordItem(long key, long value) {
            this.key = key;
            this.value = value;
        }
    }

    static ArrayList<Benchmark> selectedBenchmarks(String spec) {
        Map<String, Benchmark> available = new HashMap<>();
        add(available, new ApexBenchmark());
        add(available, new JdkObjectRecordBenchmark(false));
        add(available, new JdkObjectRecordBenchmark(true));
        add(available, new RecordBenchmark(new RecordLsdRadixUnsigned("record-lsd-radix-8", 8)));
        add(available, new RecordBenchmark(new RecordLsdRadixUnsigned("record-lsd-radix-11", 11)));
        add(available, new RecordBenchmark(new RecordLsdRadixUnsigned("record-lsd-radix-16", 16)));
        add(available, new RecordBenchmark(new RecordMsdRadix8Unsigned()));
        add(available, new RecordBenchmark(new RecordQuickUnsigned()));
        add(available, new RecordBenchmark(new RecordDualPivotQuickUnsigned()));
        add(available, new RecordBenchmark(new RecordHeapUnsigned()));
        add(available, new RecordBenchmark(new RecordInsertionUnsigned()));
        add(available, new RecordBenchmark(new RecordBubbleUnsigned()));
        add(available, new KeyBenchmark(new JdkSortUnsigned()));
        add(available, new KeyBenchmark(new JdkParallelSortUnsigned()));
        add(available, new KeyBenchmark(new LsdRadixUnsigned("lsd-radix-8", 8)));
        add(available, new KeyBenchmark(new LsdRadixUnsigned("lsd-radix-11", 11)));
        add(available, new KeyBenchmark(new LsdRadixUnsigned("lsd-radix-16", 16)));
        add(available, new KeyBenchmark(new MsdRadix8Unsigned()));
        add(available, new KeyBenchmark(new QuickUnsigned()));
        add(available, new KeyBenchmark(new DualPivotQuickUnsigned()));
        add(available, new KeyBenchmark(new HeapUnsigned()));
        add(available, new KeyBenchmark(new InsertionUnsigned()));
        add(available, new KeyBenchmark(new BubbleUnsigned()));
        add(available, new KeyBenchmark(new BucketUnsigned()));
        add(available, new KeyBenchmark(new IntroSortUnsigned()));
        add(available, new KeyBenchmark(new PdqSortUnsigned()));
        add(available, new KeyBenchmark(new TimSortUnsigned()));
        add(available, new KeyBenchmark(new AmericanFlagUnsigned()));
        add(available, new KeyBenchmark(new SampleSortUnsigned()));
        add(available, new KeyBenchmark(new BitonicUnsigned()));
      

        String normalized = spec.trim().toLowerCase(Locale.ROOT);
        String[] recordNames = new String[] {
                        "apex-records",
                        "jdk-object-arrays-sort",
                        "jdk-object-parallel-sort",
                        "record-lsd-radix-8",
                        "record-lsd-radix-11",
                        "record-lsd-radix-16",
                        "record-msd-radix-8",
                        "record-quick-hoare",
                        "record-dual-pivot-quick",
                        "record-heap-sort",
                        "record-insertion",
                        "record-bubble",
                        "introsort",
                        "pdqsort",
                        "timsort",
                        "american-flag",
                        "samplesort",
                        "bitonic"
                      
                };
        String[] keyNames = new String[] {
                        "jdk-arrays-sort",
                        "jdk-parallel-sort",
                        "lsd-radix-8",
                        "lsd-radix-11",
                        "lsd-radix-16",
                        "msd-radix-8",
                        "quick-hoare",
                        "dual-pivot-quick",
                        "heap-sort",
                        "bucket-demo",
                        "insertion",
                        "bubble"
                };
        String[] names;

        if (normalized.equals("records") || normalized.equals("record")) {
            names = recordNames;
        } else if (normalized.equals("keys") || normalized.equals("keyonly") || normalized.equals("key-only")) {
            names = keyNames;
        } else if (normalized.equals("all")) {
            names = concat(recordNames, keyNames);
        } else {
            names = normalized.split(",");
        }

        ArrayList<Benchmark> out = new ArrayList<>();
        for (String rawName : names) {
            String name = rawName.trim();
            Benchmark benchmark = available.get(name);
            if (benchmark == null) {
                throw new IllegalArgumentException("Unknown comparison algo: " + name +
                        ". Known: " + available.keySet());
            }
            out.add(benchmark);
        }
        return out;
    }

    static void add(Map<String, Benchmark> available, Benchmark benchmark) {
        available.put(benchmark.name(), benchmark);
    }

    static String[] concat(String[] first, String[] second) {
        String[] out = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, out, first.length, second.length);
        return out;
    }
   
    
    
    
    static final class AmericanFlagUnsigned implements Sorter {
        static final int BITS = 8;
        static final int RADIX = 1 << BITS;
        static final int MASK = RADIX - 1;
        static final int INSERTION_THRESHOLD = 64;

        @Override
        public String name() {
            return "american-flag";
        }

        @Override
        public String kind() {
            return "key-only";
        }

        @Override
        public long maxRecords() {
            return 250_000_000L;
        }

        @Override
        public void sort(long[] data) {
            sort(data, 0, data.length, 56);
        }

        static void sort(long[] data, int start, int end, int shift) {
            int size = end - start;

            if (size <= 1 || shift < 0) {
                return;
            }

            if (size <= INSERTION_THRESHOLD) {
                insertion(data, start, end);
                return;
            }

            int[] count = new int[RADIX];

            for (int i = start; i < end; i++) {
                int digit = (int)((data[i] >>> shift) & MASK);
                count[digit]++;
            }

            int[] begin = new int[RADIX];
            int[] next = new int[RADIX];

            begin[0] = start;

            for (int i = 1; i < RADIX; i++) {
                begin[i] = begin[i - 1] + count[i - 1];
            }

            System.arraycopy(begin, 0, next, 0, RADIX);

            for (int b = 0; b < RADIX; b++) {
                int limit = begin[b] + count[b];

                while (next[b] < limit) {
                    long value = data[next[b]];
                    int digit = (int)((value >>> shift) & MASK);

                    if (digit == b) {
                        next[b]++;
                        continue;
                    }

                    int target = next[digit]++;

                    long tmp = data[target];
                    data[target] = value;
                    data[next[b]] = tmp;
                }
            }

            if (shift == 0) {
                return;
            }

            for (int b = 0; b < RADIX; b++) {
                int lo = begin[b];
                int hi = lo + count[b];

                if (hi - lo > 1) {
                    sort(data, lo, hi, shift - BITS);
                }
            }
        }
    } 
    
    
    static final class BitonicUnsigned implements Sorter {
        @Override
        public String name() {
            return "bitonic";
        }

        @Override
        public String kind() {
            return "key-only";
        }

        @Override
        public long maxRecords() {
            return 4_000_000L;
        }

        @Override
        public void sort(long[] data) {
            int n = Integer.highestOneBit(data.length);

            for (int k = 2; k <= n; k <<= 1) {
                for (int j = k >>> 1; j > 0; j >>>= 1) {
                    for (int i = 0; i < n; i++) {
                        int ixj = i ^ j;

                        if (ixj > i) {
                            boolean asc = (i & k) == 0;

                            if ((asc && Long.compareUnsigned(data[i], data[ixj]) > 0) ||
                                (!asc && Long.compareUnsigned(data[i], data[ixj]) < 0)) {
                                swap(data, i, ixj);
                            }
                        }
                    }
                }
            }
        }
    }
    
    
    
    static final class TimSortUnsigned implements Sorter {
        @Override
        public String name() {
            return "timsort";
        }

        @Override
        public String kind() {
            return "key-only";
        }

        @Override
        public long maxRecords() {
            return 25_000_000L;
        }

        @Override
        public void sort(long[] data) {
            Long[] boxed = new Long[data.length];

            for (int i = 0; i < data.length; i++) {
                boxed[i] = data[i];
            }

            Arrays.sort(boxed, Long::compareUnsigned);

            for (int i = 0; i < data.length; i++) {
                data[i] = boxed[i];
            }
        }
    }
    
    static final class SampleSortUnsigned implements Sorter {
        public String name() {
            return "samplesort";
        }

        @Override
        public String kind() {
            return "key-only";
        }

        @Override
        public long maxRecords() {
            return 100_000_000L;
        }

        @Override
        public void sort(long[] data) {
            sampleSort(data, 0, data.length);
        }

        static void sampleSort(long[] data, int lo, int hi) {
            int size = hi - lo;

            if (size <=INSERTION_THRESHOLD) {
                insertion(data, lo, hi);
                return;
            }

            long pivot1 = data[lo + (size >>> 2)];
            long pivot2 = data[lo + (size >>> 1)];
            long pivot3 = data[lo + ((size * 3) >>> 2)];

            long[] pivots = { pivot1, pivot2, pivot3 };
            Arrays.sort(pivots);

            long p1 = pivots[0];
            long p2 = pivots[1];
            long p3 = pivots[2];

            long[] tmp = new long[size];
            int[] bucket = new int[4];

            for (int i = lo; i < hi; i++) {
                long v = data[i];

                if (Long.compareUnsigned(v, p1) < 0) bucket[0]++;
                else if (Long.compareUnsigned(v, p2) < 0) bucket[1]++;
                else if (Long.compareUnsigned(v, p3) < 0) bucket[2]++;
                else bucket[3]++;
            }

            int[] pos = new int[4];
            pos[0] = 0;
            pos[1] = bucket[0];
            pos[2] = pos[1] + bucket[1];
            pos[3] = pos[2] + bucket[2];

            for (int i = lo; i < hi; i++) {
                long v = data[i];

                if (Long.compareUnsigned(v, p1) < 0) tmp[pos[0]++] = v;
                else if (Long.compareUnsigned(v, p2) < 0) tmp[pos[1]++] = v;
                else if (Long.compareUnsigned(v, p3) < 0) tmp[pos[2]++] = v;
                else tmp[pos[3]++] = v;
            }

            System.arraycopy(tmp, 0, data, lo, size);

            int a = lo + bucket[0];
            int b = a + bucket[1];
            int c = b + bucket[2];

            sampleSort(data, lo, a);
            sampleSort(data, a, b);
            sampleSort(data, b, c);
            sampleSort(data, c, hi);
        }
    }
  
    
    
    static final class PdqSortUnsigned implements Sorter {
        static final int INSERTION_THRESHOLD = 24;

        @Override
        public String name() {
            return "pdqsort";
        }

        @Override
        public String kind() {
            return "key-only";
        }

        @Override
        public long maxRecords() {
            return 100_000_000L;
        }

        @Override
        public void sort(long[] data) {
            pdq(data, 0, data.length - 1, false);
        }

        static void pdq(long[] data, int lo, int hi, boolean badPartition) {
            while (hi - lo > INSERTION_THRESHOLD) {
                int mid = (lo + hi) >>> 1;
                long pivot = medianOfThree(data[lo], data[mid], data[hi]);

                int i = lo;
                int j = hi;

                while (i <= j) {
                    while (Long.compareUnsigned(data[i], pivot) < 0) i++;
                    while (Long.compareUnsigned(data[j], pivot) > 0) j--;

                    if (i <= j) {
                        swap(data, i++, j--);
                    }
                }

                boolean highlyUnbalanced =
                        (j - lo) < ((hi - lo) >>> 4) ||
                        (hi - i) < ((hi - lo) >>> 4);

                if (highlyUnbalanced && badPartition) {
                    Arrays.sort(data, lo, hi + 1);
                    return;
                }

                if (j - lo < hi - i) {
                    pdq(data, lo, j, highlyUnbalanced);
                    lo = i;
                } else {
                    pdq(data, i, hi, highlyUnbalanced);
                    hi = j;
                }
            }

            insertion(data, lo, hi + 1);
        }
    }    
    
    
    static final class IntroSortUnsigned implements Sorter {
        static final int INSERTION_THRESHOLD = 32;

        @Override
        public String name() {
            return "introsort";
        }

        @Override
        public String kind() {
            return "key-only";
        }

        @Override
        public long maxRecords() {
            return 100_000_000L;
        }

        @Override
        public void sort(long[] data) {
            int depth = 2 * (31 - Integer.numberOfLeadingZeros(data.length));
            intro(data, 0, data.length - 1, depth);
        }

        static void intro(long[] data, int lo, int hi, int depth) {
            while (hi - lo > INSERTION_THRESHOLD) {
                if (depth == 0) {
                    HeapUnsigned.siftDown(data, 0, hi + 1);
                    Arrays.sort(data, lo, hi + 1);
                    return;
                }

                depth--;
                int p = QuickUnsigned.partition(data, lo, hi);

                if (p - lo < hi - p) {
                    intro(data, lo, p, depth);
                    lo = p + 1;
                } else {
                    intro(data, p + 1, hi, depth);
                    hi = p;
                }
            }

            insertion(data, lo, hi + 1);
        }
    }  
    

    static final class JdkSortUnsigned implements Sorter {
        @Override
        public String name() {
            return "jdk-arrays-sort";
        }

        @Override
        public String kind() {
            return "key-only";
        }

        @Override
        public long maxRecords() {
            return UNLIMITED;
        }

        @Override
        public void sort(long[] data) {
            flipSignBit(data);
            Arrays.sort(data);
            flipSignBit(data);
        }
    }

    static final class JdkParallelSortUnsigned implements Sorter {
        @Override
        public String name() {
            return "jdk-parallel-sort";
        }

        @Override
        public String kind() {
            return "key-only";
        }

        @Override
        public long maxRecords() {
            return UNLIMITED;
        }

        @Override
        public void sort(long[] data) {
            flipSignBit(data);
            Arrays.parallelSort(data);
            flipSignBit(data);
        }
    }

    static final class LsdRadixUnsigned implements Sorter {
        final String name;
        final int bits;

        LsdRadixUnsigned(String name, int bits) {
            this.name = name;
            this.bits = bits;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String kind() {
            return "key-only";
        }

        @Override
        public long maxRecords() {
            return UNLIMITED;
        }

        @Override
        public void sort(long[] data) {
            int maxRadix = 1 << bits;
            int[] count = new int[maxRadix];
            long[] buffer = new long[data.length];
            long[] in = data;
            long[] out = buffer;

            for (int shift = 0; shift < 64; shift += bits) {
                int bitsThisPass = Math.min(bits, 64 - shift);
                int radix = 1 << bitsThisPass;
                int mask = radix - 1;

                Arrays.fill(count, 0, radix, 0);

                for (long value : in) {
                    count[(int) ((value >>> shift) & mask)]++;
                }

                int sum = 0;
                for (int i = 0; i < radix; i++) {
                    int c = count[i];
                    count[i] = sum;
                    sum += c;
                }

                for (long value : in) {
                    int digit = (int) ((value >>> shift) & mask);
                    out[count[digit]++] = value;
                }

                long[] tmp = in;
                in = out;
                out = tmp;
            }

            if (in != data) {
                System.arraycopy(in, 0, data, 0, data.length);
            }
        }
    }

    static final class MsdRadix8Unsigned implements Sorter {
        static final int BITS = 8;
        static final int RADIX = 1 << BITS;
        static final int MASK = RADIX - 1;
        static final int INSERTION_THRESHOLD = 48;

        @Override
        public String name() {
            return "msd-radix-8";
        }

        @Override
        public String kind() {
            return "key-only";
        }

        @Override
        public long maxRecords() {
            return UNLIMITED;
        }

        @Override
        public void sort(long[] data) {
            long[] aux = new long[data.length];
            sort(data, aux, 0, data.length, 56);
        }

        static void sort(long[] data, long[] aux, int lo, int hi, int shift) {
            int size = hi - lo;
            if (size <= 1 || shift < 0) {
                return;
            }

            if (size <= INSERTION_THRESHOLD) {
                insertion(data, lo, hi);
                return;
            }

            int[] count = new int[RADIX + 1];
            for (int i = lo; i < hi; i++) {
                int digit = (int) ((data[i] >>> shift) & MASK);
                count[digit + 1]++;
            }

            for (int r = 0; r < RADIX; r++) {
                count[r + 1] += count[r];
            }

            int[] starts = Arrays.copyOf(count, count.length);
            for (int i = lo; i < hi; i++) {
                int digit = (int) ((data[i] >>> shift) & MASK);
                aux[count[digit]++] = data[i];
            }

            System.arraycopy(aux, 0, data, lo, size);

            for (int r = 0; r < RADIX; r++) {
                int childLo = lo + starts[r];
                int childHi = lo + starts[r + 1];
                if (childHi - childLo > 1) {
                    sort(data, aux, childLo, childHi, shift - BITS);
                }
            }
        }
    }

    static final class QuickUnsigned implements Sorter {
        static final int INSERTION_THRESHOLD = 32;

        @Override
        public String name() {
            return "quick-hoare";
        }

        @Override
        public String kind() {
            return "key-only";
        }

        @Override
        public long maxRecords() {
            return 50_000_000L;
        }

        @Override
        public void sort(long[] data) {
            quick(data, 0, data.length - 1);
        }

        static void quick(long[] data, int lo, int hi) {
            while (lo < hi) {
                if (hi - lo <= INSERTION_THRESHOLD) {
                    insertion(data, lo, hi + 1);
                    return;
                }

                int p = partition(data, lo, hi);
                if (p - lo < hi - p) {
                    quick(data, lo, p);
                    lo = p + 1;
                } else {
                    quick(data, p + 1, hi);
                    hi = p;
                }
            }
        }

        static int partition(long[] data, int lo, int hi) {
            long pivot = medianOfThree(data[lo], data[(lo + hi) >>> 1], data[hi]);
            int i = lo - 1;
            int j = hi + 1;

            for (;;) {
                do {
                    i++;
                } while (Long.compareUnsigned(data[i], pivot) < 0);

                do {
                    j--;
                } while (Long.compareUnsigned(data[j], pivot) > 0);

                if (i >= j) {
                    return j;
                }

                swap(data, i, j);
            }
        }
    }

    static final class DualPivotQuickUnsigned implements Sorter {
        static final int INSERTION_THRESHOLD = 32;

        @Override
        public String name() {
            return "dual-pivot-quick";
        }

        @Override
        public String kind() {
            return "key-only";
        }

        @Override
        public long maxRecords() {
            return 50_000_000L;
        }

        @Override
        public void sort(long[] data) {
            sort(data, 0, data.length - 1);
        }

        static void sort(long[] data, int lo, int hi) {
            if (hi - lo <= INSERTION_THRESHOLD) {
                insertion(data, lo, hi + 1);
                return;
            }

            int third = (hi - lo) / 3;
            int m1 = lo + third;
            int m2 = hi - third;

            if (Long.compareUnsigned(data[m1], data[m2]) > 0) {
                swap(data, m1, m2);
            }

            swap(data, lo, m1);
            swap(data, hi, m2);

            if (Long.compareUnsigned(data[lo], data[hi]) > 0) {
                swap(data, lo, hi);
            }

            long p = data[lo];
            long q = data[hi];
            int lt = lo + 1;
            int gt = hi - 1;
            int i = lt;

            while (i <= gt) {
                if (Long.compareUnsigned(data[i], p) < 0) {
                    swap(data, i++, lt++);
                } else if (Long.compareUnsigned(data[i], q) > 0) {
                    swap(data, i, gt--);
                } else {
                    i++;
                }
            }

            swap(data, lo, --lt);
            swap(data, hi, ++gt);

            sort(data, lo, lt - 1);
            if (Long.compareUnsigned(p, q) < 0) {
                sort(data, lt + 1, gt - 1);
            }
            sort(data, gt + 1, hi);
        }
    }

    static final class HeapUnsigned implements Sorter {
        @Override
        public String name() {
            return "heap-sort";
        }

        @Override
        public String kind() {
            return "key-only";
        }

        @Override
        public long maxRecords() {
            return 50_000_000L;
        }

        @Override
        public void sort(long[] data) {
            int n = data.length;
            for (int i = (n >>> 1) - 1; i >= 0; i--) {
                siftDown(data, i, n);
            }

            for (int end = n - 1; end > 0; end--) {
                swap(data, 0, end);
                siftDown(data, 0, end);
            }
        }

        static void siftDown(long[] data, int root, int end) {
            for (;;) {
                int child = (root << 1) + 1;
                if (child >= end) {
                    return;
                }

                if (child + 1 < end &&
                        Long.compareUnsigned(data[child], data[child + 1]) < 0) {
                    child++;
                }

                if (Long.compareUnsigned(data[root], data[child]) >= 0) {
                    return;
                }

                swap(data, root, child);
                root = child;
            }
        }
    }

    static final class InsertionUnsigned implements Sorter {
        @Override
        public String name() {
            return "insertion";
        }

        @Override
        public String kind() {
            return "key-only";
        }

        @Override
        public long maxRecords() {
            return 200_000L;
        }

        @Override
        public void sort(long[] data) {
            insertion(data, 0, data.length);
        }
    }

    static final class BubbleUnsigned implements Sorter {
        @Override
        public String name() {
            return "bubble";
        }

        @Override
        public String kind() {
            return "key-only";
        }

        @Override
        public long maxRecords() {
            return 20_000L;
        }

        @Override
        public void sort(long[] data) {
            for (int i = 0; i < data.length; i++) {
                boolean swapped = false;
                for (int j = 0; j < data.length - i - 1; j++) {
                    if (Long.compareUnsigned(data[j], data[j + 1]) > 0) {
                        swap(data, j, j + 1);
                        swapped = true;
                    }
                }

                if (!swapped) {
                    return;
                }
            }
        }
    }

    static final class BucketUnsigned implements Sorter {
        @Override
        public String name() {
            return "bucket-demo";
        }

        @Override
        public String kind() {
            return "key-only";
        }

        @Override
        public long maxRecords() {
            return 500_000L;
        }

        @Override
        public void sort(long[] data) {
            if (data.length <= 1) {
                return;
            }

            long min = data[0];
            long max = data[0];
            for (long value : data) {
                if (Long.compareUnsigned(value, min) < 0) {
                    min = value;
                }
                if (Long.compareUnsigned(value, max) > 0) {
                    max = value;
                }
            }

            if (min == max) {
                return;
            }

            int bucketCount = Math.max(1, (int) Math.sqrt(data.length));
            @SuppressWarnings("unchecked")
            ArrayList<Long>[] buckets = new ArrayList[bucketCount];
            for (int i = 0; i < bucketCount; i++) {
                buckets[i] = new ArrayList<>();
            }

            double minDouble = unsignedDouble(min);
            double range = Math.max(1.0, unsignedDouble(max) - minDouble);

            for (long value : data) {
                int bucket = (int) (((unsignedDouble(value) - minDouble) / range) * (bucketCount - 1));
                if (bucket < 0) {
                    bucket = 0;
                } else if (bucket >= bucketCount) {
                    bucket = bucketCount - 1;
                }
                buckets[bucket].add(value);
            }

            int pos = 0;
            for (ArrayList<Long> bucket : buckets) {
                long[] tmp = new long[bucket.size()];
                for (int i = 0; i < tmp.length; i++) {
                    tmp[i] = bucket.get(i);
                }
                insertion(tmp, 0, tmp.length);
                for (long value : tmp) {
                    data[pos++] = value;
                }
            }
        }
    }

    static final class RecordLsdRadixUnsigned implements RecordSorter {
        final String name;
        final int bits;

        RecordLsdRadixUnsigned(String name, int bits) {
            this.name = name;
            this.bits = bits;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String kind() {
            return "record-sort";
        }

        @Override
        public long maxRecords() {
            return UNLIMITED;
        }

        @Override
        public void sort(long[] records, int n) {
            int maxRadix = 1 << bits;
            int[] count = new int[maxRadix];
            long[] buffer = new long[records.length];
            long[] in = records;
            long[] out = buffer;

            for (int shift = 0; shift < 64; shift += bits) {
                int bitsThisPass = Math.min(bits, 64 - shift);
                int radix = 1 << bitsThisPass;
                int mask = radix - 1;

                Arrays.fill(count, 0, radix, 0);

                for (int i = 0; i < n; i++) {
                    count[(int) ((recordKey(in, i) >>> shift) & mask)]++;
                }

                int sum = 0;
                for (int i = 0; i < radix; i++) {
                    int c = count[i];
                    count[i] = sum;
                    sum += c;
                }

                for (int i = 0; i < n; i++) {
                    int p = i << 1;
                    long key = in[p];
                    int digit = (int) ((key >>> shift) & mask);
                    int q = count[digit]++ << 1;
                    out[q] = key;
                    out[q + 1] = in[p + 1];
                }

                long[] tmp = in;
                in = out;
                out = tmp;
            }

            if (in != records) {
                System.arraycopy(in, 0, records, 0, records.length);
            }
        }
    }

    static final class RecordMsdRadix8Unsigned implements RecordSorter {
        static final int BITS = 8;
        static final int RADIX = 1 << BITS;
        static final int MASK = RADIX - 1;
        static final int INSERTION_THRESHOLD = 48;

        @Override
        public String name() {
            return "record-msd-radix-8";
        }

        @Override
        public String kind() {
            return "record-sort";
        }

        @Override
        public long maxRecords() {
            return UNLIMITED;
        }

        @Override
        public void sort(long[] records, int n) {
            long[] aux = new long[records.length];
            sort(records, aux, 0, n, 56);
        }

        static void sort(long[] records, long[] aux, int lo, int hi, int shift) {
            int size = hi - lo;
            if (size <= 1 || shift < 0) {
                return;
            }

            if (size <= INSERTION_THRESHOLD) {
                recordInsertion(records, lo, hi);
                return;
            }

            int[] count = new int[RADIX + 1];
            for (int i = lo; i < hi; i++) {
                int digit = (int) ((recordKey(records, i) >>> shift) & MASK);
                count[digit + 1]++;
            }

            for (int r = 0; r < RADIX; r++) {
                count[r + 1] += count[r];
            }

            int[] starts = Arrays.copyOf(count, count.length);
            for (int i = lo; i < hi; i++) {
                int p = i << 1;
                int digit = (int) ((records[p] >>> shift) & MASK);
                int q = count[digit]++ << 1;
                aux[q] = records[p];
                aux[q + 1] = records[p + 1];
            }

            System.arraycopy(aux, 0, records, lo << 1, size << 1);

            for (int r = 0; r < RADIX; r++) {
                int childLo = lo + starts[r];
                int childHi = lo + starts[r + 1];
                if (childHi - childLo > 1) {
                    sort(records, aux, childLo, childHi, shift - BITS);
                }
            }
        }
    }

    static final class RecordQuickUnsigned implements RecordSorter {
        static final int INSERTION_THRESHOLD = 32;

        @Override
        public String name() {
            return "record-quick-hoare";
        }

        @Override
        public String kind() {
            return "record-sort";
        }

        @Override
        public long maxRecords() {
            return 50_000_000L;
        }

        @Override
        public void sort(long[] records, int n) {
            quick(records, 0, n - 1);
        }

        static void quick(long[] records, int lo, int hi) {
            while (lo < hi) {
                if (hi - lo <= INSERTION_THRESHOLD) {
                    recordInsertion(records, lo, hi + 1);
                    return;
                }

                int p = partition(records, lo, hi);
                if (p - lo < hi - p) {
                    quick(records, lo, p);
                    lo = p + 1;
                } else {
                    quick(records, p + 1, hi);
                    hi = p;
                }
            }
        }

        static int partition(long[] records, int lo, int hi) {
            long pivot = medianOfThree(recordKey(records, lo), recordKey(records, (lo + hi) >>> 1), recordKey(records, hi));
            int i = lo - 1;
            int j = hi + 1;

            for (;;) {
                do {
                    i++;
                } while (Long.compareUnsigned(recordKey(records, i), pivot) < 0);

                do {
                    j--;
                } while (Long.compareUnsigned(recordKey(records, j), pivot) > 0);

                if (i >= j) {
                    return j;
                }

                swapRecord(records, i, j);
            }
        }
    }

    static final class RecordDualPivotQuickUnsigned implements RecordSorter {
        static final int INSERTION_THRESHOLD = 32;

        @Override
        public String name() {
            return "record-dual-pivot-quick";
        }

        @Override
        public String kind() {
            return "record-sort";
        }

        @Override
        public long maxRecords() {
            return 50_000_000L;
        }

        @Override
        public void sort(long[] records, int n) {
            sort(records, 0, n - 1);
        }

        static void sort(long[] records, int lo, int hi) {
            if (hi - lo <= INSERTION_THRESHOLD) {
                recordInsertion(records, lo, hi + 1);
                return;
            }

            int third = (hi - lo) / 3;
            int m1 = lo + third;
            int m2 = hi - third;

            if (Long.compareUnsigned(recordKey(records, m1), recordKey(records, m2)) > 0) {
                swapRecord(records, m1, m2);
            }

            swapRecord(records, lo, m1);
            swapRecord(records, hi, m2);

            if (Long.compareUnsigned(recordKey(records, lo), recordKey(records, hi)) > 0) {
                swapRecord(records, lo, hi);
            }

            long p = recordKey(records, lo);
            long q = recordKey(records, hi);
            int lt = lo + 1;
            int gt = hi - 1;
            int i = lt;

            while (i <= gt) {
                long key = recordKey(records, i);
                if (Long.compareUnsigned(key, p) < 0) {
                    swapRecord(records, i++, lt++);
                } else if (Long.compareUnsigned(key, q) > 0) {
                    swapRecord(records, i, gt--);
                } else {
                    i++;
                }
            }

            swapRecord(records, lo, --lt);
            swapRecord(records, hi, ++gt);

            sort(records, lo, lt - 1);
            if (Long.compareUnsigned(p, q) < 0) {
                sort(records, lt + 1, gt - 1);
            }
            sort(records, gt + 1, hi);
        }
    }

    static final class RecordHeapUnsigned implements RecordSorter {
        @Override
        public String name() {
            return "record-heap-sort";
        }

        @Override
        public String kind() {
            return "record-sort";
        }

        @Override
        public long maxRecords() {
            return 50_000_000L;
        }

        @Override
        public void sort(long[] records, int n) {
            for (int i = (n >>> 1) - 1; i >= 0; i--) {
                siftDown(records, i, n);
            }

            for (int end = n - 1; end > 0; end--) {
                swapRecord(records, 0, end);
                siftDown(records, 0, end);
            }
        }

        static void siftDown(long[] records, int root, int end) {
            for (;;) {
                int child = (root << 1) + 1;
                if (child >= end) {
                    return;
                }

                if (child + 1 < end &&
                        Long.compareUnsigned(recordKey(records, child), recordKey(records, child + 1)) < 0) {
                    child++;
                }

                if (Long.compareUnsigned(recordKey(records, root), recordKey(records, child)) >= 0) {
                    return;
                }

                swapRecord(records, root, child);
                root = child;
            }
        }
    }

    static final class RecordInsertionUnsigned implements RecordSorter {
        @Override
        public String name() {
            return "record-insertion";
        }

        @Override
        public String kind() {
            return "record-sort";
        }

        @Override
        public long maxRecords() {
            return 200_000L;
        }

        @Override
        public void sort(long[] records, int n) {
            recordInsertion(records, 0, n);
        }
    }

    static final class RecordBubbleUnsigned implements RecordSorter {
        @Override
        public String name() {
            return "record-bubble";
        }

        @Override
        public String kind() {
            return "record-sort";
        }

        @Override
        public long maxRecords() {
            return 20_000L;
        }

        @Override
        public void sort(long[] records, int n) {
            for (int i = 0; i < n; i++) {
                boolean swapped = false;
                for (int j = 0; j < n - i - 1; j++) {
                    if (Long.compareUnsigned(recordKey(records, j), recordKey(records, j + 1)) > 0) {
                        swapRecord(records, j, j + 1);
                        swapped = true;
                    }
                }

                if (!swapped) {
                    return;
                }
            }
        }
    }

    static void configureApex(Args args) {
        Apex.THREADS = args.threads;
        Apex.LSD_WORK_STEALING = args.lsdWorkStealing;
        Apex.WORK_STEAL_BATCH = Integer.getInteger("apex.workBatch", 4);
        Apex.PACKED_TUPLE_CYCLES = args.tuplePacking;
        Apex.IN_PLACE_MSD_SCATTER = args.inPlace;
        Apex.DIRECT_TUPLE_BITS = args.tupleBits;
        Apex.MAX_HEAP_SCRATCH_RECORDS = args.heapScratchRecords;

        int permits = args.largePermits > 0 ? args.largePermits : Math.max(1, args.threads / 8);
        Apex.LARGE_PARTITION_PERMIT_COUNT = permits;
        Apex.LARGE_PARTITION_PERMITS = new Semaphore(permits);
        Apex.POOL = Executors.newFixedThreadPool(args.threads);
    }

    static Args parseArgs(String[] rawArgs) {
        Args args = new Args();

        for (String raw : rawArgs) {
            String arg = raw.trim();
            if (arg.isEmpty()) {
                continue;
            }

            int eq = arg.indexOf('=');
            if (eq < 0) {
                args.mode = parseMode(arg);
                continue;
            }

            String key = arg.substring(0, eq).trim().toLowerCase(Locale.ROOT).replace("-", "");
            String value = arg.substring(eq + 1).trim();

            switch (key) {
                case "mode":
                    args.mode = parseMode(value);
                    break;
                case "records":
                case "n":
                    args.records = parseCount(value);
                    break;
                case "runs":
                    args.runs = parsePositiveInt(value);
                    break;
                case "warmup":
                case "warmups":
                    args.warmups = parseNonNegativeInt(value);
                    break;
                case "threads":
                    args.threads = parsePositiveInt(value);
                    break;
                case "config":
                    args.config = parseConfig(value);
                    break;
                case "algos":
                case "algo":
                    args.algos = value;
                    break;
                case "tuplebits":
                    args.tupleBits = parseNonNegativeInt(value);
                    break;
                case "heapscratch":
                case "heapscratchrecords":
                    args.heapScratchRecords = parsePositiveInt(value);
                    break;
                case "largepermits":
                case "largepartitionpermits":
                    args.largePermits = parsePositiveInt(value);
                    break;
                case "inplace":
                case "inplacemsd":
                    args.inPlace = parseBoolean(value);
                    break;
                case "workstealing":
                case "lsdworkstealing":
                    args.lsdWorkStealing = parseBoolean(value);
                    break;
                case "tuplepacking":
                    args.tuplePacking = parseBoolean(value);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown comparison option: " + raw);
            }
        }

        if (args.records < 0) {
            throw new IllegalArgumentException("records must be non-negative");
        }
        return args;
    }

    static void printResults(ArrayList<Result> results) {
        double apexMedian = Double.NaN;
        for (Result result : results) {
            if (result.name.equals("apex-records") && !Double.isNaN(result.median)) {
                apexMedian = result.median;
                break;
            }
        }

        System.out.printf("%-22s %-12s %12s %12s %12s %10s %s%n",
                "algorithm", "kind", "best", "median", "M rec/s", "vs apex", "note");

        for (Result result : results) {
            if (Double.isNaN(result.median)) {
                System.out.printf("%-22s %-12s %12s %12s %12s %10s %s%n",
                        result.name, result.kind, "-", "-", "-", "-", result.note);
                continue;
            }

            double mps = (result.records / result.median) / 1e6;
            String vsApex = Double.isNaN(apexMedian)
                    ? "-"
                    : String.format(Locale.ROOT, "%.2fx", apexMedian / result.median);

            System.out.printf(Locale.ROOT, "%-22s %-12s %11.4fs %11.4fs %11.2f %10s %s%n",
                    result.name,
                    result.kind,
                    result.best,
                    result.median,
                    mps,
                    vsApex,
                    result.note);
        }
    }

    static long[] generateKeys(int n, DataMode mode, long records) {
        long[] data = new long[n];
        for (int i = 0; i < n; i++) {
            data[i] = DataGenerator.keyForMode(i, records, mode);
        }
        return data;
    }

    static void verifyKeySort(long[] data, long expectedSum, long expectedXor) {
        long sum = 0L;
        long xor = 0L;
        long previous = 0L;

        for (int i = 0; i < data.length; i++) {
            long value = data[i];
            if (i > 0 && Long.compareUnsigned(previous, value) > 0) {
                throw new RuntimeException("KEY ORDER FAIL at " + i);
            }

            sum += value;
            xor ^= value;
            previous = value;
        }

        if (sum != expectedSum) {
            throw new RuntimeException("KEY SUM FAIL");
        }

        if (xor != expectedXor) {
            throw new RuntimeException("KEY XOR FAIL");
        }
    }

    static long[] toInterleavedRecords(long[] source) {
        long[] records = new long[source.length << 1];
        for (int i = 0; i < source.length; i++) {
            int p = i << 1;
            records[p] = source[i];
            records[p + 1] = i;
        }
        return records;
    }

    static void verifyRecordSort(long[] data, long expectedKeySum, long expectedKeyXor) {
        long keySum = 0L;
        long keyXor = 0L;
        long valueSum = 0L;
        long valueXor = 0L;
        long previous = 0L;
        int n = data.length >>> 1;

        for (int i = 0; i < n; i++) {
            int p = i << 1;
            long key = data[p];
            long value = data[p + 1];

            if (i > 0 && Long.compareUnsigned(previous, key) > 0) {
                throw new RuntimeException("RECORD ORDER FAIL at " + i);
            }

            keySum += key;
            keyXor ^= key;
            valueSum += value;
            valueXor ^= value;
            previous = key;
        }

        if (keySum != expectedKeySum || keyXor != expectedKeyXor) {
            throw new RuntimeException("RECORD KEY CONTENT FAIL");
        }

        if (valueSum != tools.triangularZeroToNMinusOne(n) ||
                valueXor != tools.xorZeroToNMinusOne(n)) {
            throw new RuntimeException("RECORD VALUE CONTENT FAIL");
        }
    }

    static void verifyObjectRecordSort(RecordItem[] data, long expectedKeySum, long expectedKeyXor) {
        long keySum = 0L;
        long keyXor = 0L;
        long valueSum = 0L;
        long valueXor = 0L;
        long previous = 0L;

        for (int i = 0; i < data.length; i++) {
            RecordItem item = data[i];

            if (i > 0 && Long.compareUnsigned(previous, item.key) > 0) {
                throw new RuntimeException("OBJECT RECORD ORDER FAIL at " + i);
            }

            keySum += item.key;
            keyXor ^= item.key;
            valueSum += item.value;
            valueXor ^= item.value;
            previous = item.key;
        }

        if (keySum != expectedKeySum || keyXor != expectedKeyXor) {
            throw new RuntimeException("OBJECT RECORD KEY CONTENT FAIL");
        }

        if (valueSum != tools.triangularZeroToNMinusOne(data.length) ||
                valueXor != tools.xorZeroToNMinusOne(data.length)) {
            throw new RuntimeException("OBJECT RECORD VALUE CONTENT FAIL");
        }
    }

    static int compareRecordItems(RecordItem left, RecordItem right) {
        return Long.compareUnsigned(left.key, right.key);
    }

    static void insertion(long[] data, int lo, int hi) {
        for (int i = lo + 1; i < hi; i++) {
            long key = data[i];
            int j = i - 1;
            while (j >= lo && Long.compareUnsigned(data[j], key) > 0) {
                data[j + 1] = data[j];
                j--;
            }
            data[j + 1] = key;
        }
    }

    static void recordInsertion(long[] records, int lo, int hi) {
        for (int i = lo + 1; i < hi; i++) {
            int p = i << 1;
            long key = records[p];
            long value = records[p + 1];
            int j = i - 1;

            while (j >= lo && Long.compareUnsigned(recordKey(records, j), key) > 0) {
                int from = j << 1;
                int to = from + 2;
                records[to] = records[from];
                records[to + 1] = records[from + 1];
                j--;
            }

            int out = (j + 1) << 1;
            records[out] = key;
            records[out + 1] = value;
        }
    }

    static long recordKey(long[] records, int index) {
        return records[index << 1];
    }

    static void swapRecord(long[] records, int i, int j) {
        if (i == j) {
            return;
        }

        int a = i << 1;
        int b = j << 1;
        long key = records[a];
        long value = records[a + 1];
        records[a] = records[b];
        records[a + 1] = records[b + 1];
        records[b] = key;
        records[b + 1] = value;
    }

    static long medianOfThree(long a, long b, long c) {
        if (Long.compareUnsigned(a, b) > 0) {
            long t = a;
            a = b;
            b = t;
        }
        if (Long.compareUnsigned(b, c) > 0) {
            long t = b;
            b = c;
            c = t;
        }
        if (Long.compareUnsigned(a, b) > 0) {
            b = a;
        }
        return b;
    }

    static void flipSignBit(long[] data) {
        for (int i = 0; i < data.length; i++) {
            data[i] ^= Long.MIN_VALUE;
        }
    }

    static double unsignedDouble(long value) {
        if (value >= 0) {
            return value;
        }
        return (value & Long.MAX_VALUE) + 0x1.0p63;
    }

    static void swap(long[] data, int i, int j) {
        long t = data[i];
        data[i] = data[j];
        data[j] = t;
    }

    static long sum(long[] data) {
        long sum = 0L;
        for (long value : data) {
            sum += value;
        }
        return sum;
    }

    static long xor(long[] data) {
        long xor = 0L;
        for (long value : data) {
            xor ^= value;
        }
        return xor;
    }

    static double elapsed(long start) {
        return (System.nanoTime() - start) / 1e9;
    }

    static long alignment() {
        boolean isApexHardware =
                System.getProperty("os.arch").contains("amd64") &&
                        Runtime.getRuntime().availableProcessors() >= 32;
        return isApexHardware ? 2L * 1024L * 1024L : 64L;
    }

    static int checkedArrayLength(long records) {
        if (records > Integer.MAX_VALUE - 8L) {
            throw new IllegalArgumentException("Comparison key baselines need a Java long[]; records too large: " + records);
        }
        return (int) records;
    }

    static DataMode parseMode(String value) {
        return DataMode.valueOf(value.trim().toUpperCase(Locale.ROOT).replace('-', '_'));
    }

    static Config parseConfig(String value) {
        String[] parts = value.split(",");
        if (parts.length != 3) {
            throw new IllegalArgumentException("config must be msd,lsd,tiny");
        }
        return new Config(parsePositiveInt(parts[0]), parsePositiveInt(parts[1]), parsePositiveInt(parts[2]));
    }

    static boolean parseBoolean(String value) {
        String v = value.trim().toLowerCase(Locale.ROOT);
        return v.equals("true") || v.equals("yes") || v.equals("y") || v.equals("1") || v.equals("on");
    }

    static int parsePositiveInt(String value) {
        int n = Integer.parseInt(value.trim().replace("_", ""));
        if (n <= 0) {
            throw new IllegalArgumentException("Expected positive integer: " + value);
        }
        return n;
    }

    static int parseNonNegativeInt(String value) {
        int n = Integer.parseInt(value.trim().replace("_", ""));
        if (n < 0) {
            throw new IllegalArgumentException("Expected non-negative integer: " + value);
        }
        return n;
    }

    static long parseCount(String value) {
        String v = value.trim().replace("_", "").toLowerCase(Locale.ROOT);
        long multiplier = 1L;

        if (v.endsWith("k")) {
            multiplier = 1_000L;
            v = v.substring(0, v.length() - 1);
        } else if (v.endsWith("m")) {
            multiplier = 1_000_000L;
            v = v.substring(0, v.length() - 1);
        } else if (v.endsWith("g")) {
            multiplier = 1_000_000_000L;
            v = v.substring(0, v.length() - 1);
        }

        return Long.parseLong(v) * multiplier;
    }
}
