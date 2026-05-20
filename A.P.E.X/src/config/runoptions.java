package config;

import java.util.ArrayList;
import java.util.Locale;

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
        public  boolean lsdWorkStealing = true;
        public  int workStealBatch = Integer.getInteger("apex.workBatch", 4);
        public  boolean packedTupleCycles = Boolean.getBoolean("apex.tuplePacking");
        public  boolean inPlaceMsdScatter = Boolean.parseBoolean(System.getProperty("apex.inPlaceMsd", "false"));
        public  int inPlaceTileRecords = Integer.getInteger("apex.inPlaceTileRecords", 64);
        public  int directTupleBits = Integer.getInteger("apex.tupleBits", 9);
        public  int heapScratchRecords = Integer.getInteger("apex.heapScratchRecords", 1_048_576);
        public  int minMsdBits = 12;
        public  int maxMsdBits = 13;
        public  int minLsdBits = 12;
        public   int maxLsdBits = 17;
        public   int minTiny = 32;
        public    int maxTiny = 1024;
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

        if (options.heapScratchRecords <= 0) {
            throw new IllegalArgumentException("heapScratch must be positive");
        }

        if (options.workStealBatch <= 0) {
            throw new IllegalArgumentException("workBatch must be positive");
        }

        if (options.inPlaceTileRecords <= 0) {
            throw new IllegalArgumentException("inPlaceTileRecords must be positive");
        }

        validateBitRange("MSD", options.minMsdBits, options.maxMsdBits);
        validateBitRange("LSD", options.minLsdBits, options.maxLsdBits);

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
                case "workstealing":
                case "lsdworkstealing":
                case "steal":
                    options.lsdWorkStealing = parseBoolean(value);
                    break;
                case "workbatch":
                case "stealbatch":
                case "workstealbatch":
                    options.workStealBatch = parsePositiveInt(value);
                    break;
                case "tuplepacking":
                case "packedtuples":
                case "tuplecycles":
                case "tuples":
                    options.packedTupleCycles = parseBoolean(value);
                    break;
                case "inplace":
                case "inplacemsd":
                case "inplacescatter":
                case "inplacemsdscatter":
                    options.inPlaceMsdScatter = parseBoolean(value);
                    break;
                case "inplacetile":
                case "inplacetiles":
                case "inplacetilerecords":
                    options.inPlaceTileRecords = parsePositiveInt(value);
                    break;
                case "tuplebits":
                case "tuplecap":
                case "tuplecapbits":
                case "directtuplebits":
                    options.directTupleBits = parseNonNegativeInt(value);
                    break;
                case "heapscratch":
                case "heapscratchrecords":
                case "maxheapscratch":
                    options.heapScratchRecords = parsePositiveInt(value);
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

        validateOptions(options);

        return options;
    }
    
    static int[] parseIntRange(String value) {
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

    static Config parseConfig(String value) {
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

    static int parseThreads(String value) {
        if (value.equalsIgnoreCase("auto")) {
            return Runtime.getRuntime().availableProcessors();
        }

        return parsePositiveInt(value);
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
  
    static long[] parseLongListOrRange(String value) {
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
