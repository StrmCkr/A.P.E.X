package config;

import java.util.ArrayList;
import java.util.Locale;
import java.util.concurrent.Semaphore;

import config.configurations.Config;
import generator.DataMode;
import generator.dataparser;
import generator.recordcountformode;
import main.Apex;

public class runoptions {

    public static class Options {
    	public  ArrayList<DataMode> modes = new ArrayList<>();
        public  long[] recordsList;
        public long tuneRecords = Apex.TUNE_RECORDS;
        public  long warmupRecords = Apex.WARMUP_RECORDS;
        public  long sweepRecords = 100_000_000L;
        public boolean sweep = false;
        public  int threads = Integer.getInteger("apex.threads", Runtime.getRuntime().availableProcessors());
        public  int largePartitionPermits = 0;
        public  boolean orderFastPath = Boolean.parseBoolean(System.getProperty("apex.orderFastPath", "false"));
        public  boolean lsdWorkStealing = true;
        public  int workStealBatch = Integer.getInteger("apex.workBatch", Math.max(4, threads / 2));
        public  boolean packedTupleCycles = Boolean.getBoolean("apex.tuplePacking");
        public  int directTupleBits = Integer.getInteger("apex.tupleBits", 9);
        public  boolean staggerTupleCycles = Boolean.parseBoolean(System.getProperty("apex.staggerTuples", "true"));
        public  boolean staggerTupleCostModel = Boolean.parseBoolean(System.getProperty("apex.staggerTupleCostModel", "true"));
        public  int staggerTupleBits = Integer.getInteger("apex.staggerTupleBits", 16);
        public  int staggerTupleMinRecords = Integer.getInteger("apex.staggerTupleMinRecords", 0);
        public  int lsdHeapUnroll = Integer.getInteger("apex.lsdHeapUnroll", 0);
        public  int lsdHeapUnrollMinRecords = Integer.getInteger("apex.lsdHeapUnrollMinRecords", 4_096);
        public  int heapScratchRecords = Integer.getInteger("apex.heapScratchRecords", 1_048_576);       
        public  int localMsdBits = Integer.getInteger("apex.localMsdBits", 0);
        public  int localMsdMaxChildren = Integer.getInteger("apex.localMsdMaxChildren", Apex.LOCAL_MSD_MAX_CHILDREN);
        public  boolean dominantCoreFastPath = Boolean.parseBoolean(System.getProperty("apex.dominantCore", Boolean.toString(Apex.DOMINANT_CORE_FAST_PATH)));
        public  int dominantCoreSampleRecords = Integer.getInteger("apex.dominantCoreSampleRecords", Apex.DOMINANT_CORE_SAMPLE_RECORDS);
        public  int dominantCoreCandidates = Integer.getInteger("apex.dominantCoreCandidates", Apex.DOMINANT_CORE_CANDIDATES);
        public  int dominantCoreMinSharePercent = Integer.getInteger("apex.dominantCoreMinShare", Apex.DOMINANT_CORE_MIN_SHARE_PERCENT);
        public  int dominantKeyMinShareDivisor = Integer.getInteger("apex.dominantKeyMinShareDivisor", Apex.DOMINANT_KEY_MIN_SHARE_DIVISOR);
        public  int minMsdBits = 11;
        public  int maxMsdBits = 13;
        public  int minLsdBits = 12;
        public  int maxLsdBits = 17;
        public  int minTiny = 32;
        public  int maxTiny = 1024;
        public Config lockedConfig;
    }

    public static void validateOptions(Options options) {
        if (options.threads <= 0) {
            throw new IllegalArgumentException("threads must be positive");
        }

        if (options.largePartitionPermits < 0) {
            throw new IllegalArgumentException("largePermits must be positive or omitted for auto");
        }

        if (options.directTupleBits > Apex.MAX_DIRECT_TUPLE_BITS) {
            throw new IllegalArgumentException("tupleBits must be <= " +  Apex.MAX_DIRECT_TUPLE_BITS);
        }

        if (options.staggerTupleBits <= 0 || options.staggerTupleBits > Apex.MAX_DIRECT_TUPLE_BITS) {
            throw new IllegalArgumentException("staggerTupleBits must be in 1.." + Apex.MAX_DIRECT_TUPLE_BITS);
        }

        if (options.staggerTupleMinRecords < 0) {
            throw new IllegalArgumentException("staggerTupleMinRecords must be non-negative");
        }

        if (options.heapScratchRecords <= 0) {
            throw new IllegalArgumentException("heapScratch must be positive");
        }

        if (options.workStealBatch <= 0) {
            throw new IllegalArgumentException("workBatch must be positive");
        }

        validateBitRange("MSD", options.minMsdBits, options.maxMsdBits);
        validateBitRange("LSD", options.minLsdBits, options.maxLsdBits);
        if (options.localMsdBits > 0) {
            validateBitRange("Local MSD", options.localMsdBits, options.localMsdBits);
        }

        if (options.localMsdMaxChildren < 0) {
            throw new IllegalArgumentException("localMsdMaxChildren must be non-negative");
        }

        if (options.dominantCoreSampleRecords <= 0) {
            throw new IllegalArgumentException("dominantCoreSampleRecords must be positive");
        }

        if (options.dominantCoreCandidates <= 0) {
            throw new IllegalArgumentException("dominantCoreCandidates must be positive");
        }

        if (options.dominantCoreMinSharePercent <= 0 || options.dominantCoreMinSharePercent > 100) {
            throw new IllegalArgumentException("dominantCoreMinShare must be in 1..100");
        }

        if (options.dominantKeyMinShareDivisor <= 0) {
            throw new IllegalArgumentException("dominantKeyMinShareDivisor must be positive");
        }

        if (options.minTiny <= 0 || options.maxTiny <= 0) {
            throw new IllegalArgumentException("Tiny range must be positive");
        }

        if (options.minTiny > options.maxTiny) {
            throw new IllegalArgumentException("Tiny min must be <= max");
        }

        if (options.lockedConfig != null) {
            validateConfig(options.lockedConfig);
        }
    } 

    public static void applyApexSettings(Options options) {
        applyApexSettings(
                options.threads,
                options.orderFastPath,
                options.lsdWorkStealing,
                options.workStealBatch,
                options.packedTupleCycles,
                options.directTupleBits,
                options.staggerTupleCycles,
                options.staggerTupleCostModel,
                options.staggerTupleBits,
                options.staggerTupleMinRecords,
                options.lsdHeapUnroll,
                options.lsdHeapUnrollMinRecords,
                options.heapScratchRecords,
                options.localMsdBits,
                options.largePartitionPermits
        );
        Apex.LOCAL_MSD_MAX_CHILDREN = options.localMsdMaxChildren;
        Apex.DOMINANT_CORE_FAST_PATH = options.dominantCoreFastPath;
        Apex.DOMINANT_CORE_SAMPLE_RECORDS = options.dominantCoreSampleRecords;
        Apex.DOMINANT_CORE_CANDIDATES = options.dominantCoreCandidates;
        Apex.DOMINANT_CORE_MIN_SHARE_PERCENT = options.dominantCoreMinSharePercent;
        Apex.DOMINANT_KEY_MIN_SHARE_DIVISOR = options.dominantKeyMinShareDivisor;
    }

    public static void applyApexSettings(
            int threads,
            boolean orderFastPath,
            boolean lsdWorkStealing,
            int workStealBatch,
            boolean packedTupleCycles,
            int directTupleBits,
            boolean staggerTupleCycles,
            boolean staggerTupleCostModel,
            int staggerTupleBits,
            int staggerTupleMinRecords,
            int lsdHeapUnroll,
            int lsdHeapUnrollMinRecords,
            int heapScratchRecords,
            int localMsdBits,
            int largePartitionPermits
    ) {
        Apex.THREADS = threads;
        Apex.ORDER_FAST_PATH = orderFastPath;
        Apex.LSD_WORK_STEALING = lsdWorkStealing;
        Apex.WORK_STEAL_BATCH = workStealBatch;
        Apex.PACKED_TUPLE_CYCLES = packedTupleCycles;
        Apex.DIRECT_TUPLE_BITS = directTupleBits;
        Apex.STAGGER_TUPLE_CYCLES = staggerTupleCycles;
        Apex.STAGGER_TUPLE_COST_MODEL = staggerTupleCostModel;
        Apex.STAGGER_TUPLE_BITS = staggerTupleBits;
        Apex.STAGGER_TUPLE_MIN_RECORDS = staggerTupleMinRecords;
        Apex.LSD_HEAP_UNROLL = lsdHeapUnroll;
        Apex.LSD_HEAP_UNROLL_MIN_RECORDS = lsdHeapUnrollMinRecords;
        Apex.MAX_HEAP_SCRATCH_RECORDS = heapScratchRecords;
        Apex.LOCAL_MSD_BITS = localMsdBits;
        Apex.LOCAL_MSD_MIN_SHARE_DIVISOR = Integer.getInteger(
                "apex.localMsdMinShareDivisor",
                Math.max(2, threads / 2)
        );

        Apex.LARGE_PARTITION_PERMIT_COUNT = largePartitionPermits > 0
                ? largePartitionPermits
                : Math.max(1, threads / 8);
        Apex.LARGE_PARTITION_PERMITS = new Semaphore(Apex.LARGE_PARTITION_PERMIT_COUNT);
    }
    
    public  static void validateBitRange(String label, int min, int max) {
        if (min <= 0 || max >= 31 || min > max) {
            throw new IllegalArgumentException(label + " bits must be in 1..30 and min <= max");
        }
    }

    public static void validateConfig(Config cfg) {
        validateBitRange("Locked MSD", cfg.msdBits, cfg.msdBits);
        validateBitRange("Locked LSD", cfg.lsdBits, cfg.lsdBits);

        if (cfg.tinyPartitionThreshold <= 0) {
            throw new IllegalArgumentException("Locked config tiny threshold must be positive");
        }
    } 
    
    public static Options parseOptions(String[] args) {
        Options options = new Options();
        ArrayList<String> positional = new ArrayList<>();
        boolean workStealBatchExplicit = false;

        for (String raw : args) {
            String arg = raw.trim();

            if (arg.isEmpty()) {
                continue;
            }

            int eq = arg.indexOf('=');

            if (eq < 0) {
                positional.add(arg);
                continue;
            }

            String key = arg.substring(0, eq).trim().toLowerCase(Locale.ROOT).replace("-", "");
            String value = arg.substring(eq + 1).trim();

            switch (key) {
                case "mode":
                case "modes":
                    options.modes = dataparser.parseModes(value);
                    break;
                case "records":
                case "record":
                case "n":
                    options.recordsList = parseLongListOrRange(value);
                    break;
                case "tunerecords":
                case "tune":
                    options.tuneRecords = parseCount(value);
                    break;
                case "warmuprecords":
                case "warmup":
                    options.warmupRecords = parseCount(value);
                    break;
                case "sweeprecords":
                case "sweepn":
                    options.sweepRecords = parseCount(value);
                    break;
                case "sweep":
                    options.sweep = parseBoolean(value);
                    break;
                case "threads":
                    options.threads = parseThreads(value);
                    break;
                case "largepermits":
                case "largepartitionpermits":
                case "largepartitions":
                    options.largePartitionPermits = parsePositiveInt(value);
                    break;
                case "orderfastpath":
                case "inputorderfastpath":
                case "prescan":
                    options.orderFastPath = parseBoolean(value);
                    break;
                case "workstealing":
                case "lsdworkstealing":
                case "steal":
                    options.lsdWorkStealing = parseBoolean(value);
                    break;
                case "workbatch":
                case "stealbatch":
                case "workstealbatch":
                    options.workStealBatch = parsePositiveInt(value);
                    workStealBatchExplicit = true;
                    break;
                case "tuplepacking":
                case "packedtuples":
                case "tuplecycles":
                case "tuples":
                    options.packedTupleCycles = parseBoolean(value);
                    break;
                case "tuplebits":
                case "tuplecap":
                case "tuplecapbits":
                case "directtuplebits":
                    options.directTupleBits = parseNonNegativeInt(value);
                    break;
                case "staggertuples":
                case "staggertuplecycles":
                    options.staggerTupleCycles = parseBoolean(value);
                    break;
                case "staggertuplecostmodel":
                case "staggercostmodel":
                    options.staggerTupleCostModel = parseBoolean(value);
                    break;
                case "staggertuplebits":
                    options.staggerTupleBits = parsePositiveInt(value);
                    break;
                case "staggertuplemin":
                case "staggertupleminrecords":
                    options.staggerTupleMinRecords = parseNonNegativeInt(value);
                    break;
                case "lsdheapunroll":
                case "heapunroll":
                    options.lsdHeapUnroll = parseNonNegativeInt(value);
                    break;
                case "lsdheapunrollmin":
                case "heapunrollmin":
                case "lsdheapunrollminrecords":
                    options.lsdHeapUnrollMinRecords = parsePositiveInt(value);
                    break;
                case "heapscratch":
                case "heapscratchrecords":
                case "maxheapscratch":
                    options.heapScratchRecords = parsePositiveInt(value);
                    break;
                case "localmsdbits":
                case "secondarymsdbits":
                case "submsdbits":
                    options.localMsdBits = parseNonNegativeInt(value);
                    break;
                case "localmsdmaxchildren":
                case "localmsdchildren":
                case "maxlocalmsdchildren":
                    options.localMsdMaxChildren = parseNonNegativeInt(value);
                    break;
                case "dominantcore":
                case "dominantcorefastpath":
                    options.dominantCoreFastPath = parseBoolean(value);
                    break;
                case "dominantcoresamplerecords":
                case "dominantcoresample":
                    options.dominantCoreSampleRecords = parseIntCount(value, "dominantCoreSampleRecords");
                    break;
                case "dominantcorecandidates":
                    options.dominantCoreCandidates = parsePositiveInt(value);
                    break;
                case "dominantcoreminshare":
                case "dominantcoreminsharepercent":
                    options.dominantCoreMinSharePercent = parsePositiveInt(value);
                    break;
                case "dominantkeyminsharedivisor":
                case "dominantkeysharedivisor":
                    options.dominantKeyMinShareDivisor = parsePositiveInt(value);
                    break;
               
                case "msd":
                case "msdrange": {
                    int[] range = parseIntRange(value);
                    options.minMsdBits = range[0];
                    options.maxMsdBits = range[1];
                    break;
                }
                case "lsd":
                case "lsdrange": {
                    int[] range = parseIntRange(value);
                    options.minLsdBits = range[0];
                    options.maxLsdBits = range[1];
                    break;
                }
                case "tiny":
                case "tinyrange": {
                    int[] range = parseIntRange(value);
                    options.minTiny = range[0];
                    options.maxTiny = range[1];
                    break;
                }
                case "config":
                case "locked":
                    options.lockedConfig = parseConfig(value);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown option: " + raw);
            }
        }

        if (!positional.isEmpty() && options.modes.isEmpty()) {
            options.modes.add(dataparser.parseMode(positional.get(0)));
        }

        if (positional.size() > 1 && options.recordsList == null) {
            options.recordsList = new long[] { parseCount(positional.get(1)) };
        }

        if (positional.size() >= 5 && options.lockedConfig == null) {
            options.lockedConfig = new Config(
                    parsePositiveInt(positional.get(2)),
                    parsePositiveInt(positional.get(3)),
                    parsePositiveInt(positional.get(4))
            );
        }

        if (options.modes.isEmpty()) {
            options.modes.add(DataMode.RANDOM);
        }

        if (options.recordsList == null) {
            options.recordsList = new long[] { recordcountformode.recordCountForMode(options.modes.get(0)) };
        }

        if (!workStealBatchExplicit) {
            options.workStealBatch = Integer.getInteger("apex.workBatch", Math.max(4, options.threads / 2));
        }

        validateOptions(options);

        return options;
    }
    
    public static int[] parseIntRange(String value) {
        String v = value.trim();
        int range = v.indexOf("..");

        if (range >= 0) {
            int start = parsePositiveInt(v.substring(0, range));
            int end = parsePositiveInt(v.substring(range + 2));
            return new int[] { Math.min(start, end), Math.max(start, end) };
        }

        int exact = parsePositiveInt(v);
        return new int[] { exact, exact };
    }

    public static Config parseConfig(String value) {
        String[] parts = value.split(",");

        if (parts.length != 3) {
            throw new IllegalArgumentException("Config must be msd,lsd,tiny. Got: " + value);
        }

        return new Config(
                parsePositiveInt(parts[0]),
                parsePositiveInt(parts[1]),
                parsePositiveInt(parts[2])
        );
    }

    public static int parseThreads(String value) {
        if (value.equalsIgnoreCase("auto")) {
            return Runtime.getRuntime().availableProcessors();
        }

        return parsePositiveInt(value);
    }

    public static boolean parseBoolean(String value) {
        String v = value.trim().toLowerCase(Locale.ROOT);
        return v.equals("true") || v.equals("yes") || v.equals("y") || v.equals("1") || v.equals("on");
    }

    public static int parsePositiveInt(String value) {
        int n = Integer.parseInt(value.trim().replace("_", ""));

        if (n <= 0) {
            throw new IllegalArgumentException("Expected positive integer: " + value);
        }

        return n;
    }

    public static int parseNonNegativeInt(String value) {
        int n = Integer.parseInt(value.trim().replace("_", ""));

        if (n < 0) {
            throw new IllegalArgumentException("Expected non-negative integer: " + value);
        }

        return n;
    }

    public static long parseCount(String value) {
        String v = value.trim().replace("_", "").toLowerCase(Locale.ROOT);
        long multiplier = 1;

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

        long n = Long.parseLong(v) * multiplier;

        if (n < 0) {
            throw new IllegalArgumentException("Expected non-negative count: " + value);
        }

        return n;
    }

    public static int parseIntCount(String value, String label) {
        long n = parseCount(value);

        if (n > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(label + " must fit in a 32-bit integer: " + value);
        }

        return (int) n;
    }
  
    public static long[] parseLongListOrRange(String value) {
        String v = value.trim();
        int range = v.indexOf("..");

        if (range >= 0) {
            long start = parseCount(v.substring(0, range));
            String rest = v.substring(range + 2);
            long step = 0;
            int colon = rest.indexOf(':');

            if (colon >= 0) {
                step = parseCount(rest.substring(colon + 1));
                rest = rest.substring(0, colon);
            }

            long end = parseCount(rest);

            if (step <= 0) {
                return start == end ? new long[] { start } : new long[] { start, end };
            }

            ArrayList<Long> values = new ArrayList<>();

            if (start <= end) {
                for (long n = start; n <= end; n += step) {
                    values.add(n);
                }
            } else {
                for (long n = start; n >= end; n -= step) {
                    values.add(n);
                }
            }

            long[] out = new long[values.size()];
            for (int i = 0; i < values.size(); i++) {
                out[i] = values.get(i);
            }
            return out;
        }

        String[] parts = v.split(",");
        long[] out = new long[parts.length];

        for (int i = 0; i < parts.length; i++) {
            out[i] = parseCount(parts[i]);
        }

        return out;
    }
    
    
    
}
