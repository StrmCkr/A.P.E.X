package io.github.strmckr.apex.benchmark;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;

import io.github.strmckr.apex.config.Configurations;
import io.github.strmckr.apex.config.SortConfig;
import io.github.strmckr.apex.config.RunOptions;
import io.github.strmckr.apex.generator.DataGenerator;
import io.github.strmckr.apex.generator.DataMode;
import io.github.strmckr.apex.generator.DataParser;
import io.github.strmckr.apex.generator.DataInitializer;
import io.github.strmckr.apex.engine.ApexEngine;
import io.github.strmckr.apex.reporting.Reporters;
import io.github.strmckr.apex.tools.Tools;
import io.github.strmckr.apex.tools.Verifier;


public final class SortBenchmark {
    static final int DEFAULT_RUNS = 21;
    static final int DEFAULT_WARMUPS = 3;
    static final long UNLIMITED = Long.MAX_VALUE;
    static final int INSERTION_THRESHOLD = 64;
    static final DateTimeFormatter REPORT_TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

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
    
    static final DataMode[] MODES_TO_RUN = {
    	    DataMode.RANDOM,   
    	    DataMode.LOW_BITS_ONLY,
    	    DataMode.HIGH_BITS_ONLY,
    	    DataMode.DUPLICATES,
    	    DataMode.ZIPFIANISH,   	      	   
    	    DataMode.SPARSE_ENTROPY_EXPLOSION
    	};
    
    static final long[] SIZES_TO_RUN = { 	
    	    		
    	    1_000_000L,
    	    10_000_000,
    	   100_000_000L,
    	 //   500_000_000L
    	};

    static final DataMode[] SMOKE_MODES = {
            DataMode.RANDOM,
            DataMode.DUPLICATES
    };

    static final long[] SMOKE_SIZES = {
            1_000_000L
    };

    static ArrayList<Benchmark> selectedBenchmarksCurated() {
        Map<String, Benchmark> available = new HashMap<>();

        // === APEX ===
        add(available, new ApexBenchmark());
        add(available, new ApexKeyBenchmark());

        // === JDK BASELINES ===       
        add(available, new KeyBenchmark(new JdkSortUnsigned()));
        add(available, new KeyBenchmark(new JdkParallelSortUnsigned()));      

        // === MODERN HIGH-PERFORMANCE ===
        add(available, new KeyBenchmark(new FastutilParallelRadixUnsigned()));        
      

        return new ArrayList<>(available.values());
    }    
    

    record Result(
    	    String name,
    	    String kind,
    	    long records,
    	    double min,
    	    double median,
    	    double mean,
    	    double stddev,
    	    double max,
    	    double CV,
    	    double p95,
    	    double p99
    	  
    	) {}

    static final class Args {
        DataMode[] modes = MODES_TO_RUN;
        long[] recordsList = SIZES_TO_RUN;
        int runs = DEFAULT_RUNS;
        int warmups = DEFAULT_WARMUPS;
        int threads = Integer.getInteger("apex.threads", Runtime.getRuntime().availableProcessors());
        int workStealBatch = Integer.getInteger("apex.workBatch", Math.max(4, threads / 2));
        boolean descendingScatterFastPath = Boolean.parseBoolean(System.getProperty("apex.descendingScatter", "true"));
        int tupleBits = Integer.getInteger("apex.tupleBits", 9);
        int contiguousTupleBits = Integer.getInteger("apex.contiguousTupleBits", ApexEngine.DIRECT_TUPLE_CONTIGUOUS_BITS);
        int directTupleInPlaceMaxRecords = Integer.getInteger(
                "apex.directTupleInPlaceMaxRecords",
                ApexEngine.DEFAULT_DIRECT_TUPLE_IN_PLACE_MAX_RECORDS
        );
        int directTupleManyPartitionMin = Integer.getInteger(
                "apex.directTupleManyPartitions",
                ApexEngine.DEFAULT_DIRECT_TUPLE_MANY_PARTITION_MIN
        );
        int lsdHeapUnroll = Integer.getInteger("apex.lsdHeapUnroll", 0);
        int lsdHeapUnrollMinRecords = Integer.getInteger("apex.lsdHeapUnrollMinRecords", 4_096);
        int heapScratchRecords = Integer.getInteger("apex.heapScratchRecords", 1_048_576);
        int localMsdBits = Integer.getInteger("apex.localMsdBits", 0);
        int localMsdMaxChildren = Integer.getInteger("apex.localMsdMaxChildren", ApexEngine.LOCAL_MSD_MAX_CHILDREN);
        boolean dominantCoreFastPath = Boolean.parseBoolean(System.getProperty("apex.dominantCore", Boolean.toString(ApexEngine.DOMINANT_CORE_FAST_PATH)));
        int dominantCoreSampleRecords = Integer.getInteger("apex.dominantCoreSampleRecords", ApexEngine.DOMINANT_CORE_SAMPLE_RECORDS);
        int dominantCoreCandidates = Integer.getInteger("apex.dominantCoreCandidates", ApexEngine.DOMINANT_CORE_CANDIDATES);
        int dominantCoreMinSharePercent = Integer.getInteger("apex.dominantCoreMinShare", ApexEngine.DOMINANT_CORE_MIN_SHARE_PERCENT);
        int dominantKeyMinShareDivisor = Integer.getInteger("apex.dominantKeyMinShareDivisor", ApexEngine.DOMINANT_KEY_MIN_SHARE_DIVISOR);
        int largePermits = 0;
        boolean orderFastPath = Boolean.parseBoolean(System.getProperty("apex.orderFastPath", "false"));
        boolean signedKeys = Boolean.parseBoolean(System.getProperty("apex.signedKeys", "false"));
        boolean lsdWorkStealing = true;
        boolean tuplePacking = Boolean.getBoolean("apex.tuplePacking");
        boolean staggerTupleCycles = Boolean.parseBoolean(System.getProperty("apex.staggerTuples", "true"));
        boolean staggerTupleCostModel = Boolean.parseBoolean(System.getProperty("apex.staggerTupleCostModel", "true"));
        int staggerTupleBits = Integer.getInteger("apex.staggerTupleBits", 16);
        int staggerTupleMinRecords = Integer.getInteger("apex.staggerTupleMinRecords", 0);
        String reportingMode = Boolean.getBoolean("apex.quiet") || Boolean.getBoolean("apex.silent")
                ? "off"
                : normalizeBenchmarkReporting(System.getProperty("apex.reporting", "summary"));
        String reportStamp = reportTimestamp();
        String reportDirectory = System.getProperty("apex.reportDir", defaultBenchmarkReportDirectory());
        String reportFile = System.getProperty(
                "apex.reportFile",
                defaultReportPath(reportDirectory, reportStamp, ".txt")
        );
        String csvFile = System.getProperty(
                "apex.csvFile",
                defaultReportPath(reportDirectory, reportStamp, ".csv")
        );
        boolean reportFileExplicit = System.getProperty("apex.reportFile") != null;
        boolean csvFileExplicit = System.getProperty("apex.csvFile") != null;
        boolean curated = true;
        SortConfig config = Configurations.defaultConfig();
        String algos = "records";
    }

    public static void main(String[] rawArgs) throws Exception {
        Args args = parseArgs(rawArgs);
        BenchmarkCsvReport csvReport = BenchmarkCsvReport.disabled();

        try {
            Reporters.configure(args.reportingMode, args.reportFile);
            csvReport = BenchmarkCsvReport.open(args.csvFile);
            configureApex(args);
            printReportLocations(args, csvReport);

            for (DataMode mode : args.modes) {
                for (long size : args.recordsList) {

                    int n = checkedArrayLength(size);
                    long alignment = alignment();

                    long[] source = generateKeys(n, mode, size);
                    long expectedSum = sum(source);
                    long expectedXor = xor(source);
                                       

                    ArrayList<Benchmark> benchmarks =
                    	    args.curated ? selectedBenchmarksCurated()
                    	                 : selectedBenchmarks(args.algos);

                    //ArrayList<Benchmark> benchmarks = selectedBenchmarks(args.algos);
                    ArrayList<Result> results = new ArrayList<>();

                    Reporters.println("=== APEX SORT BENCHMARK ===");
                    Reporters.println("Mode: " + mode);
                    Reporters.println("Records: " + size);
                    Reporters.println("Runs: " + args.runs + " warmups=" + args.warmups);
                    Reporters.println("Threads: " + ApexEngine.THREADS);
                    Reporters.println("Apex config: " + args.config);
                    Reporters.println();

                    for (Benchmark benchmark : benchmarks) {
                        if (size > benchmark.maxRecords()) {
                        	results.add(new Result(
                        	        benchmark.name(),
                        	        benchmark.kind(),
                        	        size,                        	        
                        	        Double.NaN,
                        	        Double.NaN,
                        	        Double.NaN,
                        	        Double.NaN,
                        	        Double.NaN,
                        	        Double.NaN,
                        	        Double.NaN,
                        	        Double.NaN
                        	       
                        	));
                            continue;
                        }

                        try {
                            for (int i = 0; i < args.warmups; i++) {
                                benchmark.run(source, expectedSum, expectedXor, mode, size, args.config, alignment);
                            }

                            double[] measured = new double[args.runs];
                            for (int i = 0; i < args.runs; i++) {
                                measured[i] = benchmark.run(
                                        source,
                                        expectedSum,
                                        expectedXor,
                                        mode,
                                        size,
                                        args.config,
                                        alignment
                                );
                            }

                            Arrays.sort(measured);

                           
                            double min = measured[0];
                            double max = measured[measured.length - 1];
                            
                            double median;

                            if ((measured.length & 1) == 0) {
                                int m = measured.length >>> 1;
                                median = (measured[m - 1] + measured[m]) * 0.5;
                            } else {
                                median = measured[measured.length >>> 1];
                            }
                           
                            double mean = mean(measured);
                            double stddev = stddev(measured, mean);
                            double cv = (stddev / mean) * 100.0;
                            
                            double p95 = measured[(int) Math.floor(0.95 * (measured.length - 1))];
                            double p99 = measured[(int) Math.floor(0.99 * (measured.length - 1))];

                            results.add(new Result(
                                    benchmark.name(),
                                    benchmark.kind(),
                                    size,
                                   min,
                                    median,
                                    mean,
                                    stddev,
                                   max,
                                    cv,
                                    p95,
                                    p99
                                    
                            ));
                        } catch (Throwable ex) {
                        	results.add(new Result(
                        	        benchmark.name(),
                        	        benchmark.kind(),
                        	        size,                        	        
                        	        Double.NaN,
                        	        Double.NaN,
                        	        Double.NaN,
                        	        Double.NaN,
                        	        Double.NaN,
                        	        Double.NaN,
                        	        Double.NaN,
                        	        Double.NaN
                        	        
                        	));
                        }
                    }

                    printResults(results);
                    csvReport.write(mode, size, results);
                    Reporters.println(); // blank line between mode/size tables
                }
            }

        } finally {
            if (ApexEngine.POOL != null) {
                ApexEngine.POOL.shutdown();
            }
            csvReport.close();
            Reporters.close();
        }
    }

    static void printReportLocations(Args args, BenchmarkCsvReport csvReport) {
        boolean hasTextReport = !Reporters.isDisabledFile(args.reportFile);
        boolean hasCsvReport = csvReport.isEnabled();

        if (!hasTextReport && !hasCsvReport) {
            return;
        }

        if (hasTextReport) {
            Reporters.println("Report file: " + Path.of(args.reportFile).toAbsolutePath().normalize());
        }
        if (hasCsvReport) {
            Reporters.println("CSV file: " + csvReport.absolutePath());
        }
        Reporters.println();
    }

    static String reportTimestamp() {
        return LocalDateTime.now().format(REPORT_TIMESTAMP_FORMAT);
    }

    static String defaultReportPath(String directory, String stamp, String extension) {
        return Path.of(directory, "apex-benchmark-" + stamp + extension).toString();
    }

    static String defaultBenchmarkReportDirectory() {
        Path moduleDirectory = Path.of("A.P.E.X-benchmark");
        if (Files.isDirectory(moduleDirectory.resolve("src"))) {
            return moduleDirectory.resolve("target").resolve("benchmark-reports").toString();
        }
        return Path.of("target", "benchmark-reports").toString();
    }

    static String normalizeOptionalFile(String value) {
        return Reporters.isDisabledFile(value) ? "" : value.trim();
    }

    static final class BenchmarkCsvReport {
        private final Path path;
        private final BufferedWriter writer;

        private BenchmarkCsvReport(Path path, BufferedWriter writer) {
            this.path = path;
            this.writer = writer;
        }

        static BenchmarkCsvReport disabled() {
            return new BenchmarkCsvReport(null, null);
        }

        static BenchmarkCsvReport open(String file) {
            if (Reporters.isDisabledFile(file)) {
                return disabled();
            }

            Path path = Path.of(file.trim()).toAbsolutePath().normalize();

            try {
                Path parent = path.getParent();
                if (parent != null) {
                    Files.createDirectories(parent);
                }

                BufferedWriter writer = Files.newBufferedWriter(
                        path,
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE
                );
                BenchmarkCsvReport report = new BenchmarkCsvReport(path, writer);
                report.writeLine(
                        "mode",
                        "records",
                        "algorithm",
                        "kind",
                        "status",
                        "min_s",
                        "median_s",
                        "mean_s",
                        "stddev_s",
                        "max_s",
                        "cv_percent",
                        "p95_s",
                        "p99_s",
                        "mrec_per_s"
                );
                return report;
            } catch (IOException ex) {
                throw new UncheckedIOException("Unable to open CSV benchmark report: " + path, ex);
            }
        }

        boolean isEnabled() {
            return writer != null;
        }

        Path absolutePath() {
            return path;
        }

        void write(DataMode mode, long records, ArrayList<Result> results) {
            if (!isEnabled()) {
                return;
            }

            try {
                for (Result result : results) {
                    writeLine(
                            mode.name(),
                            Long.toString(records),
                            result.name(),
                            result.kind(),
                            Double.isNaN(result.median()) ? "skipped" : "ok",
                            number(result.min()),
                            number(result.median()),
                            number(result.mean()),
                            number(result.stddev()),
                            number(result.max()),
                            number(result.CV()),
                            number(result.p95()),
                            number(result.p99()),
                            throughput(result)
                    );
                }
                writer.flush();
            } catch (IOException ex) {
                throw new UncheckedIOException("Unable to write CSV benchmark report: " + path, ex);
            }
        }

        void close() {
            if (!isEnabled()) {
                return;
            }

            try {
                writer.close();
            } catch (IOException ex) {
                throw new UncheckedIOException("Unable to close CSV benchmark report: " + path, ex);
            }
        }

        private void writeLine(String... cells) throws IOException {
            for (int i = 0; i < cells.length; i++) {
                if (i > 0) {
                    writer.write(',');
                }
                writer.write(csv(cells[i]));
            }
            writer.newLine();
        }

        private static String throughput(Result result) {
            if (Double.isNaN(result.median())) {
                return "";
            }

            return number((result.records() / result.median()) / 1e6);
        }

        private static String number(double value) {
            return Double.isNaN(value) ? "" : String.format(Locale.ROOT, "%.9f", value);
        }

        private static String csv(String value) {
            String text = value == null ? "" : value;
            boolean quote = text.indexOf(',') >= 0 ||
                    text.indexOf('"') >= 0 ||
                    text.indexOf('\n') >= 0 ||
                    text.indexOf('\r') >= 0;

            if (!quote) {
                return text;
            }

            return "\"" + text.replace("\"", "\"\"") + "\"";
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
                SortConfig cfg,
                long alignment
        ) throws Exception;
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
                SortConfig cfg,
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
                SortConfig cfg,
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
        add(available, new ApexKeyBenchmark());
        
        add(available, new JdkObjectRecordBenchmark(false));
        add(available, new JdkObjectRecordBenchmark(true));
        add(available, new KeyBenchmark(new JdkSortUnsigned()));
        add(available, new KeyBenchmark(new JdkParallelSortUnsigned()));

   
        add(available, new RecordBenchmark(new RecordLsdRadixUnsigned("record-lsd-radix-16", 16)));     
        add(available, new KeyBenchmark(new LsdRadixUnsigned("lsd-radix-16", 16)));      
        
        
        add(available, new RecordBenchmark(new RecordMsdRadix8Unsigned()));
        add(available, new KeyBenchmark(new MsdRadix8Unsigned()));

        add(available, new RecordBenchmark(new RecordAmericanFlagUnsigned()));
        add(available, new KeyBenchmark(new AmericanFlagUnsigned()));  
        
        add(available, new KeyBenchmark(new FastutilRadixUnsigned()));
        add(available, new KeyBenchmark(new FastutilParallelRadixUnsigned()));        


        String normalized = spec.trim().toLowerCase(Locale.ROOT);
        String[] recordNames = new String[] {
        	    "apex-records",        	   
        	    "jdk-object-arrays-sort",
        	    "jdk-object-parallel-sort",        	 
        	    "record-lsd-radix-16",
        	    "record-msd-radix-8",
        	    "record-american-flag",  };
        String[] keyNames = new String[] {
        	    "apex-keys",
        	    "jdk-arrays-sort",
        	    "jdk-parallel-sort",      	  
           	    "lsd-radix-16",
        	    "msd-radix-8",  
        	    "american-flag",
        	    "fastutil-radix",
        	    "fastutil-parallel-radix",        	 
        	   
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
                throw new IllegalArgumentException("Unknown benchmark algo: " + name +
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
                SortConfig cfg,
                long alignment
        ) throws Exception {
            try (Arena arena = Arena.ofShared()) {
                long bytes = Tools.bytesForRecords(records);
                MemorySegment src = arena.allocate(bytes, alignment);
                MemorySegment dst = arena.allocate(bytes, alignment);

                DataInitializer.initData(src, records, mode);

                long start = System.nanoTime();
                MemorySegment sorted = ApexEngine.sortPipeline(src, dst, records, cfg);
                double seconds = elapsed(start);
                Verifier.verify(sorted, records, mode, false);
                return seconds;
            }
        }
    }

    static final class ApexKeyBenchmark implements Benchmark {
        @Override
        public String name() {
            return "apex-keys";
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
        public double run(
                long[] source,
                long expectedSum,
                long expectedXor,
                DataMode mode,
                long records,
                SortConfig cfg,
                long alignment
        ) throws Exception {
            try (Arena arena = Arena.ofShared()) {
                long bytes = Tools.bytesForRecords(records);
                MemorySegment src = arena.allocate(bytes, alignment);
                MemorySegment dst = arena.allocate(bytes, alignment);

                for (int i = 0; i < source.length; i++) {
                    long p = (long) i << 4;
                    src.set(ApexEngine.LONG, p, source[i]);
                    src.set(ApexEngine.LONG, p + 8, i);
                }

                long start = System.nanoTime();
                MemorySegment sorted = ApexEngine.sortPipeline(src, dst, records, cfg);
                double seconds = elapsed(start);
                verifySegmentKeySort(sorted, records, expectedSum, expectedXor);
                return seconds;
            }
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
            if (!ApexEngine.SIGNED_KEYS) {
                flipSignBit(data);
            }
            Arrays.sort(data);
            if (!ApexEngine.SIGNED_KEYS) {
                flipSignBit(data);
            }
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
            if (!ApexEngine.SIGNED_KEYS) {
                flipSignBit(data);
            }
            Arrays.parallelSort(data);
            if (!ApexEngine.SIGNED_KEYS) {
                flipSignBit(data);
            }
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
                SortConfig cfg,
                long alignment
        ) {
            RecordItem[] data = new RecordItem[source.length];
            for (int i = 0; i < source.length; i++) {
                data[i] = new RecordItem(source[i], i);
            }

            long start = System.nanoTime();
            if (parallel) {
                Arrays.parallelSort(data, SortBenchmark::compareRecordItems);
            } else {
                Arrays.sort(data, SortBenchmark::compareRecordItems);
            }
            double seconds = elapsed(start);
            verifyObjectRecordSort(data, expectedSum, expectedXor);
            return seconds;
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
            int n = data.length;
            if (n <= 1) {
                return;
            }

            long[] aux = new long[n];
            // no unsigned transform here
            sort(data, aux, 0, n, 56);
        }

        static void sort(long[] a, long[] aux, int lo, int hi, int shift) {

            int size = hi - lo;
            if (size <= 1 || shift < 0) {
                return;
            }

            if (size <= INSERTION_THRESHOLD) {
                // your insertion: uses Long.compareUnsigned
                insertion(a, lo, hi);
                return;
            }

            // counting
            int[] count = new int[RADIX];
            for (int i = lo; i < hi; i++) {
                int d = Tools.shiftedDigit(a[i], shift, MASK);
                count[d]++;
            }

            // prefix sums (absolute starts)
            int[] start = new int[RADIX];
            start[0] = lo;
            for (int r = 1; r < RADIX; r++) {
                start[r] = start[r - 1] + count[r - 1];
            }

            // working cursors
            int[] next = Arrays.copyOf(start, RADIX);

            // distribute (absolute indices)
            for (int i = lo; i < hi; i++) {
                int d = Tools.shiftedDigit(a[i], shift, MASK);
                aux[next[d]++] = a[i];
            }

            // copy back
            System.arraycopy(aux, lo, a, lo, size);

            // recurse
            for (int r = 0; r < RADIX; r++) {
                int l = start[r];
                int h = (r == RADIX - 1) ? hi : start[r + 1];
                if (h - l > 1) {
                    sort(a, aux, l, h, shift - BITS);
                }
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
            long[] aux = new long[n << 1];
            sort(records, aux, 0, n, 56);
        }

        static void sort(long[] r, long[] aux, int lo, int hi, int shift) {

            int size = hi - lo;
            if (size <= 1 || shift < 0) {
                return;
            }

            if (size <= INSERTION_THRESHOLD) {
                recordInsertion(r, lo, hi);   // already unsigned‑correct
                return;
            }

            // - Counting-
            int[] count = new int[RADIX];
            for (int i = lo; i < hi; i++) {
                long key = recordKey(r, i);
                int d = Tools.shiftedDigit(key, shift, MASK);
                count[d]++;
            }

            // - Prefix sums (absolute positions)-
            int[] start = new int[RADIX];
            start[0] = lo;
            for (int i = 1; i < RADIX; i++) {
                start[i] = start[i - 1] + count[i - 1];
            }

            // - Next write positions-
            int[] next = Arrays.copyOf(start, RADIX);

            // - Scatter into aux (0‑based window)-
            for (int i = lo; i < hi; i++) {
                long key = recordKey(r, i);
                int d = Tools.shiftedDigit(key, shift, MASK);

                int pos = next[d]++ - lo;   // convert absolute → window index

                int src = i << 1;
                int dst = pos << 1;

                aux[dst]     = r[src];
                aux[dst + 1] = r[src + 1];
            }

            // - Copy back into r-
            System.arraycopy(aux, 0, r, lo << 1, size << 1);

            // - Recurse into buckets-
            for (int d = 0; d < RADIX; d++) {
                int l = start[d];
                int h = (d == RADIX - 1) ? hi : start[d + 1];

                if (h - l > 1) {
                    sort(r, aux, l, h, shift - BITS);
                }
            }
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
                    count[Tools.shiftedDigit(value, shift, mask)]++;
                }

                int sum = 0;
                for (int i = 0; i < radix; i++) {
                    int c = count[i];
                    count[i] = sum;
                    sum += c;
                }

                for (long value : in) {
                    int digit = Tools.shiftedDigit(value, shift, mask);
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
                    count[Tools.shiftedDigit(recordKey(in, i), shift, mask)]++;
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
                    int digit = Tools.shiftedDigit(key, shift, mask);
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

    static final class RecordAmericanFlagUnsigned implements RecordSorter {
        static final int BITS = 8;
        static final int RADIX = 1 << BITS;
        static final int MASK = RADIX - 1;
        static final int INSERTION_THRESHOLD = 64;

        @Override
        public String name() {
            return "record-american-flag";
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
            sort(records, 0, n, 56);
        }

        static void sort(long[] records, int start, int end, int shift) {
            int size = end - start;
            if (size <= 1 || shift < 0) {
                return;
            }

            if (size <= INSERTION_THRESHOLD) {
                recordInsertion(records, start, end);
                return;
            }

            int[] count = new int[RADIX];
            for (int i = start; i < end; i++) {
                count[Tools.shiftedDigit(recordKey(records, i), shift, MASK)]++;
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
                    long key = recordKey(records, next[b]);
                    int digit = Tools.shiftedDigit(key, shift, MASK);

                    if (digit == b) {
                        next[b]++;
                        continue;
                    }

                    swapRecord(records, next[b], next[digit]++);
                }
            }

            if (shift == 0) {
                return;
            }

            for (int b = 0; b < RADIX; b++) {
                int lo = begin[b];
                int hi = lo + count[b];
                if (hi - lo > 1) {
                    sort(records, lo, hi, shift - BITS);
                }
            }
        }
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
            return UNLIMITED;
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
                int digit = Tools.shiftedDigit(data[i], shift, MASK);
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
                    int digit = Tools.shiftedDigit(value, shift, MASK);

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
    
    static final class FastutilRadixUnsigned implements Sorter {

        @Override
        public String name() {
            return "fastutil-radix";
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

            if (!ApexEngine.SIGNED_KEYS) {
                flipSignBit(data);
            }
            try {
                Class<?> longArrays = Class.forName("it.unimi.dsi.fastutil.longs.LongArrays");
                longArrays.getMethod("radixSort", long[].class).invoke(null, (Object) data);
            } catch (ReflectiveOperationException ex) {
                throw new IllegalStateException("FastUtil LongArrays.radixSort is not on the classpath", ex);
            } finally {
                if (!ApexEngine.SIGNED_KEYS) {
                    flipSignBit(data);
                }
            }
        }
    }
    
    static final class FastutilParallelRadixUnsigned implements Sorter {

        @Override
        public String name() {
            return "fastutil-parallel-radix";
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

            if (!ApexEngine.SIGNED_KEYS) {
                flipSignBit(data);
            }
            try {
                Class<?> longArrays = Class.forName("it.unimi.dsi.fastutil.longs.LongArrays");
                longArrays.getMethod("parallelRadixSort", long[].class).invoke(null, (Object) data);
            } catch (ReflectiveOperationException ex) {
                throw new IllegalStateException("FastUtil LongArrays.parallelRadixSort is not on the classpath", ex);
            } finally {
                if (!ApexEngine.SIGNED_KEYS) {
                    flipSignBit(data);
                }
            }
        }
    }


    static void configureApex(Args args) {
        RunOptions.applyApexSettings(
                args.threads,
                args.orderFastPath,
                args.signedKeys,
                args.lsdWorkStealing,
                args.workStealBatch,
                args.descendingScatterFastPath,
                args.tuplePacking,
                args.tupleBits,
                args.directTupleInPlaceMaxRecords,
                args.directTupleManyPartitionMin,
                args.staggerTupleCycles,
                args.staggerTupleCostModel,
                args.staggerTupleBits,
                args.staggerTupleMinRecords,
                args.lsdHeapUnroll,
                args.lsdHeapUnrollMinRecords,
                args.heapScratchRecords,
                args.localMsdBits,
                args.largePermits
        );
        ApexEngine.DIRECT_TUPLE_CONTIGUOUS_BITS = args.contiguousTupleBits;
        ApexEngine.LOCAL_MSD_MAX_CHILDREN = args.localMsdMaxChildren;
        ApexEngine.DOMINANT_CORE_FAST_PATH = args.dominantCoreFastPath;
        ApexEngine.DOMINANT_CORE_SAMPLE_RECORDS = args.dominantCoreSampleRecords;
        ApexEngine.DOMINANT_CORE_CANDIDATES = args.dominantCoreCandidates;
        ApexEngine.DOMINANT_CORE_MIN_SHARE_PERCENT = args.dominantCoreMinSharePercent;
        ApexEngine.DOMINANT_KEY_MIN_SHARE_DIVISOR = args.dominantKeyMinShareDivisor;
        ApexEngine.POOL = Executors.newFixedThreadPool(ApexEngine.THREADS);
    }

    static Args parseArgs(String[] rawArgs) {
        Args args = new Args();
        boolean workStealBatchExplicit = false;
        boolean directTupleManyPartitionExplicit = false;

        for (String raw : rawArgs) {
            String arg = raw.trim();
            if (arg.isEmpty()) {
                continue;
            }

            int eq = arg.indexOf('=');
            if (eq < 0) {
                args.modes = new DataMode[] { DataParser.parseMode(arg) };
                continue;
            }

            String key = arg.substring(0, eq).trim().toLowerCase(Locale.ROOT).replace("-", "");
            String value = arg.substring(eq + 1).trim();

            switch (key) {
            case "curated": args.curated = RunOptions.parseBoolean(value); break;
            case "triage":
            case "preset":
            case "benchmarkpreset":
                applyTriagePreset(args, value);
                break;
            case "reporting":
            case "reporter":
            case "report":
            case "standardreporting":
                args.reportingMode = normalizeBenchmarkReporting(value);
                break;
            case "reportfile":
            case "reportpath":
            case "reportoutput":
            case "logfile":
            case "logpath":
                args.reportFile = normalizeOptionalFile(value);
                args.reportFileExplicit = true;
                break;
            case "csvfile":
            case "csvpath":
            case "resultsfile":
            case "resultfile":
                args.csvFile = normalizeOptionalFile(value);
                args.csvFileExplicit = true;
                break;
            case "reportdir":
            case "logdir":
            case "outputdir":
                args.reportDirectory = normalizeOptionalFile(value);
                args.reportFile = defaultReportPath(args.reportDirectory, args.reportStamp, ".txt");
                args.csvFile = defaultReportPath(args.reportDirectory, args.reportStamp, ".csv");
                args.reportFileExplicit = true;
                args.csvFileExplicit = true;
                break;
            case "quiet":
            case "silent":
                args.reportingMode = RunOptions.parseBoolean(value) ? "off" : normalizeBenchmarkReporting("summary");
                break;
            case "signed":
            case "signedkeys":
            case "signedkey":
                args.signedKeys = RunOptions.parseBoolean(value);
                break;
            case "keyorder": {
                String v = value.trim().toLowerCase(Locale.ROOT);
                if (v.equals("signed") || v.equals("signedlong") || v.equals("long")) {
                    args.signedKeys = true;
                } else if (v.equals("unsigned") || v.equals("unsignedlong")) {
                    args.signedKeys = false;
                } else {
                    args.signedKeys = RunOptions.parseBoolean(value);
                }
                break;
            }
                case "descendingscatter":
                case "descscatter":
                case "globaldescendingscatter":
                    args.descendingScatterFastPath = RunOptions.parseBoolean(value);
                    break;
                case "mode":
                case "modes":
                    args.modes = DataParser.parseModes(value).toArray(new DataMode[0]);
                    break;
                case "records":
                case "record":
                case "n":
                    args.recordsList = RunOptions.parseLongListOrRange(value);
                    break;
                case "runs":
                    args.runs = RunOptions.parsePositiveInt(value);
                    break;
                case "warmup":
                case "warmups":
                    args.warmups = RunOptions.parseNonNegativeInt(value);
                    break;
                case "threads":
                    args.threads = RunOptions.parseThreads(value);
                    break;
                case "config":
                    args.config = RunOptions.parseConfig(value);
                    break;
                case "algos":
                case "algo":
                    args.algos = value;
                    break;
                case "tuplebits":
                    args.tupleBits = RunOptions.parseNonNegativeInt(value);
                    break;
                case "contiguoustuplebits":
                case "directtuplecontiguousbits":
                case "contiguousdirecttuplebits":
                    args.contiguousTupleBits = RunOptions.parseNonNegativeInt(value);
                    break;
                case "directtupleinplacemax":
                case "directtupleinplacemaxrecords":
                case "tupleinplacemax":
                case "tupleinplacemaxrecords":
                    args.directTupleInPlaceMaxRecords = RunOptions.parseIntCount(value, "directTupleInPlaceMaxRecords");
                    break;
                case "directtuplemanypartitions":
                case "directtuplemanypartitionmin":
                case "tuplemanypartitions":
                case "tuplemanypartitionmin":
                    args.directTupleManyPartitionMin = RunOptions.parseNonNegativeInt(value);
                    directTupleManyPartitionExplicit = true;
                    break;
                case "staggertuples":
                case "staggertuplecycles":
                    args.staggerTupleCycles = RunOptions.parseBoolean(value);
                    break;
                case "staggertuplecostmodel":
                case "staggercostmodel":
                    args.staggerTupleCostModel = RunOptions.parseBoolean(value);
                    break;
                case "staggertuplebits":
                    args.staggerTupleBits = RunOptions.parsePositiveInt(value);
                    break;
                case "staggertuplemin":
                case "staggertupleminrecords":
                    args.staggerTupleMinRecords = RunOptions.parseNonNegativeInt(value);
                    break;
                case "lsdheapunroll":
                case "heapunroll":
                    args.lsdHeapUnroll = RunOptions.parseNonNegativeInt(value);
                    break;
                case "lsdheapunrollmin":
                case "heapunrollmin":
                case "lsdheapunrollminrecords":
                    args.lsdHeapUnrollMinRecords = RunOptions.parsePositiveInt(value);
                    break;
                case "heapscratch":
                case "heapscratchrecords":
                    args.heapScratchRecords = RunOptions.parsePositiveInt(value);
                    break;
                case "localmsdbits":
                case "secondarymsdbits":
                case "submsdbits":
                    args.localMsdBits = RunOptions.parseNonNegativeInt(value);
                    break;
                case "localmsdmaxchildren":
                case "localmsdchildren":
                case "maxlocalmsdchildren":
                    args.localMsdMaxChildren = RunOptions.parseNonNegativeInt(value);
                    break;
                case "dominantcore":
                case "dominantcorefastpath":
                    args.dominantCoreFastPath = RunOptions.parseBoolean(value);
                    break;
                case "dominantcoresamplerecords":
                case "dominantcoresample":
                    args.dominantCoreSampleRecords = RunOptions.parseIntCount(value, "dominantCoreSampleRecords");
                    break;
                case "dominantcorecandidates":
                    args.dominantCoreCandidates = RunOptions.parsePositiveInt(value);
                    break;
                case "dominantcoreminshare":
                case "dominantcoreminsharepercent":
                    args.dominantCoreMinSharePercent = RunOptions.parsePositiveInt(value);
                    break;
                case "dominantkeyminsharedivisor":
                case "dominantkeysharedivisor":
                    args.dominantKeyMinShareDivisor = RunOptions.parsePositiveInt(value);
                    break;
                case "largepermits":
                case "largepartitionpermits":
                    args.largePermits = RunOptions.parsePositiveInt(value);
                    break;                 
                case "orderfastpath":
                case "inputorderfastpath":
                case "prescan":
                    args.orderFastPath = RunOptions.parseBoolean(value);
                    break;
                case "workbatch":
                case "stealbatch":
                case "workstealbatch":
                    args.workStealBatch = RunOptions.parsePositiveInt(value);
                    workStealBatchExplicit = true;
                    break;
                case "workstealing":
                case "lsdworkstealing":
                    args.lsdWorkStealing = RunOptions.parseBoolean(value);
                    break;
                case "tuplepacking":
                    args.tuplePacking = RunOptions.parseBoolean(value);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown benchmark option: " + raw);
            }
        }

        for (long records : args.recordsList) {
            if (records < 0) {
                throw new IllegalArgumentException("records must be non-negative");
            }
        }
        if (args.modes.length == 0) {
            throw new IllegalArgumentException("At least one benchmark mode is required");
        }
        if (!workStealBatchExplicit) {
            args.workStealBatch = Integer.getInteger("apex.workBatch", Math.max(4, args.threads / 2));
        }
        if (!directTupleManyPartitionExplicit) {
            args.directTupleManyPartitionMin = Integer.getInteger(
                    "apex.directTupleManyPartitions",
                    ApexEngine.DEFAULT_DIRECT_TUPLE_MANY_PARTITION_MIN
            );
        }
        if ("off".equals(args.reportingMode)) {
            if (!args.reportFileExplicit) {
                args.reportFile = "";
            }
            if (!args.csvFileExplicit) {
                args.csvFile = "";
            }
        }
        if (args.tupleBits > ApexEngine.MAX_DIRECT_TUPLE_BITS) {
            throw new IllegalArgumentException("tupleBits must be <= " + ApexEngine.MAX_DIRECT_TUPLE_BITS);
        }
        if (args.contiguousTupleBits > ApexEngine.MAX_DIRECT_TUPLE_BITS) {
            throw new IllegalArgumentException("contiguousTupleBits must be <= " + ApexEngine.MAX_DIRECT_TUPLE_BITS);
        }
        if (args.directTupleInPlaceMaxRecords < 0) {
            throw new IllegalArgumentException("directTupleInPlaceMaxRecords must be non-negative");
        }
        if (args.directTupleManyPartitionMin < 0) {
            throw new IllegalArgumentException("directTupleManyPartitions must be non-negative");
        }
        if (args.staggerTupleBits <= 0 || args.staggerTupleBits > ApexEngine.MAX_DIRECT_TUPLE_BITS) {
            throw new IllegalArgumentException("staggerTupleBits must be in 1.." + ApexEngine.MAX_DIRECT_TUPLE_BITS);
        }
        if (args.staggerTupleMinRecords < 0) {
            throw new IllegalArgumentException("staggerTupleMinRecords must be non-negative");
        }
        if (args.localMsdBits > 0) {
            RunOptions.validateBitRange("Local MSD", args.localMsdBits, args.localMsdBits);
        }
        if (args.localMsdMaxChildren < 0) {
            throw new IllegalArgumentException("localMsdMaxChildren must be non-negative");
        }
        if (args.dominantCoreSampleRecords <= 0) {
            throw new IllegalArgumentException("dominantCoreSampleRecords must be positive");
        }
        if (args.dominantCoreCandidates <= 0) {
            throw new IllegalArgumentException("dominantCoreCandidates must be positive");
        }
        if (args.dominantCoreMinSharePercent <= 0 || args.dominantCoreMinSharePercent > 100) {
            throw new IllegalArgumentException("dominantCoreMinShare must be in 1..100");
        }
        if (args.dominantKeyMinShareDivisor <= 0) {
            throw new IllegalArgumentException("dominantKeyMinShareDivisor must be positive");
        }
        RunOptions.validateConfig(args.config);
        return args;
    }

    static void applyTriagePreset(Args args, String value) {
        String preset = value.trim().toLowerCase(Locale.ROOT).replace("-", "").replace("_", "");

        switch (preset) {
            case "full", "curated", "standard", "default" -> {
                args.curated = true;
                args.modes = MODES_TO_RUN.clone();
                args.recordsList = SIZES_TO_RUN.clone();
                args.runs = DEFAULT_RUNS;
                args.warmups = DEFAULT_WARMUPS;
            }
            case "smoke", "quick", "sanity" -> {
                args.curated = true;
                args.modes = SMOKE_MODES.clone();
                args.recordsList = SMOKE_SIZES.clone();
                args.runs = 5;
                args.warmups = 1;
            }
            case "expanded", "all" -> {
                args.curated = false;
                args.algos = "all";
                args.modes = MODES_TO_RUN.clone();
                args.recordsList = SIZES_TO_RUN.clone();
                args.runs = DEFAULT_RUNS;
                args.warmups = DEFAULT_WARMUPS;
            }
            default -> throw new IllegalArgumentException(
                    "Unknown triage preset: " + value + " (expected full, smoke, or expanded)"
            );
        }
    }

    static String normalizeBenchmarkReporting(String mode) {
        String normalized = mode == null
                ? "summary"
                : mode.trim().toLowerCase(Locale.ROOT).replace("-", "").replace("_", "");
        if (normalized.isEmpty() ||
                normalized.equals("summary") ||
                normalized.equals("compact") ||
                normalized.equals("brief")) {
            return "standard";
        }

        return Reporters.normalizeMode(mode);
    }

  static void printResults(ArrayList<Result> results) {

    double apexMedian = Double.NaN;

    for (Result r : results) {
        if (r.name.equals("apex-records")
                && !Double.isNaN(r.median)) {

            apexMedian = r.median;
            break;
        }
    }

    Reporters.printf(
    		"%-28s %-16s %12s %12s %12s %12s %12s %12s %12s%n",
        "algorithm",
        "kind",
        
        "median(s)",
        "mean(s)",
        "stddev(s)",
        
        "CV(%)",         
        
        "p95(s)",
		"p99(s)",
		
	    "M rec/s"
       
    );
    Reporters.println(
    	    "-------------------------------------------------------------------------------------------------------------------------------------");

    for (Result r : results) {

        if (Double.isNaN(r.median)) {

            Reporters.printf(
            		"%-28s %-16s %12s %12s  %12s %12s %12s %12s %12s%n",
                r.name,
                r.kind,
                "-",
                "-",
                "-",
                "-",
                "-",                 
                "-",
                "-",
                "-"
            );

            continue;
        }

        double throughput =
            (r.records / r.median) / 1e6;


        Reporters.printf(
            Locale.ROOT,
            "%-28s %-16s  %12.4f %12.4f %12.4f %12.4f %12.4f %12.2f %12.2f%n",
            r.name,
            r.kind,
           
            r.median,
            r.mean,
            r.stddev,
            
            r.CV,
            
            r.p95,            
            r.p99,
            throughput           
        );
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
            if (i > 0 && Tools.compareKeys(previous, value) > 0) {
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

    static void verifySegmentKeySort(MemorySegment data, long records, long expectedSum, long expectedXor) {
        long sum = 0L;
        long xor = 0L;
        long previous = 0L;

        for (long i = 0; i < records; i++) {
            long key = data.get(ApexEngine.LONG, i << 4);
            if (i > 0 && Tools.compareKeys(previous, key) > 0) {
                throw new RuntimeException("APEX KEY ORDER FAIL at " + i);
            }

            sum += key;
            xor ^= key;
            previous = key;
        }

        if (sum != expectedSum) {
            throw new RuntimeException("APEX KEY SUM FAIL");
        }

        if (xor != expectedXor) {
            throw new RuntimeException("APEX KEY XOR FAIL");
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

            if (i > 0 && Tools.compareKeys(previous, key) > 0) {
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

        if (valueSum != Tools.triangularZeroToNMinusOne(n) ||
                valueXor != Tools.xorZeroToNMinusOne(n)) {
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

            if (i > 0 && Tools.compareKeys(previous, item.key) > 0) {
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

        if (valueSum != Tools.triangularZeroToNMinusOne(data.length) ||
                valueXor != Tools.xorZeroToNMinusOne(data.length)) {
            throw new RuntimeException("OBJECT RECORD VALUE CONTENT FAIL");
        }
    }

    static int compareRecordItems(RecordItem left, RecordItem right) {
        return Tools.compareKeys(left.key, right.key);
    }

    static void insertion(long[] data, int lo, int hi) {
        for (int i = lo + 1; i < hi; i++) {
            long key = data[i];
            int j = i - 1;
            while (j >= lo && Tools.compareKeys(data[j], key) > 0) {
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

            while (j >= lo && Tools.compareKeys(recordKey(records, j), key) > 0) {
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
        if (Tools.compareKeys(a, b) > 0) {
            long t = a;
            a = b;
            b = t;
        }
        if (Tools.compareKeys(b, c) > 0) {
            long t = b;
            b = c;
            c = t;
        }
        if (Tools.compareKeys(a, b) > 0) {
            b = a;
        }
        return b;
    }

    static long ninther(long[] data, int lo, int hi) {
        int step = Math.max(1, (hi - lo) / 8);
        long a = medianOfThree(data[lo], data[Math.min(hi, lo + step)], data[Math.min(hi, lo + (step << 1))]);
        long b = medianOfThree(data[Math.max(lo, ((lo + hi) >>> 1) - step)], data[(lo + hi) >>> 1],
                data[Math.min(hi, ((lo + hi) >>> 1) + step)]);
        long c = medianOfThree(data[Math.max(lo, hi - (step << 1))], data[Math.max(lo, hi - step)], data[hi]);
        return medianOfThree(a, b, c);
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
            throw new IllegalArgumentException("Benchmark key baselines need a Java long[]; records too large: " + records);
        }
        return (int) records;
    }

    static double mean(double[] values) {
        double sum = 0.0;

        for (double v : values) {
            sum += v;
        }

        return sum / values.length;
    }

    static double stddev(double[] values, double mean) {

        if (values.length <= 1) {
            return 0.0;
        }

        double sum = 0.0;

        for (double v : values) {
            double d = v - mean;
            sum += d * d;
        }

        return Math.sqrt(sum / (values.length - 1));
    }
}
