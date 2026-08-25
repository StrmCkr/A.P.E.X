package io.github.strmckr.apex;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.concurrent.Executors;

import io.github.strmckr.apex.engine.ApexEngine;
import io.github.strmckr.apex.generator.DataGenerator;
import io.github.strmckr.apex.generator.DataMode;
import io.github.strmckr.apex.generator.DataParser;
import io.github.strmckr.apex.reporting.Reporter;
import io.github.strmckr.apex.reporting.Reporters;
import io.github.strmckr.apex.tools.Tools;
import io.github.strmckr.apex.tools.Verifier;

public final class Apex {
    public static final int RECORD_BYTES = ApexEngine.RECORD_BYTES;

    private Apex() {
    }

    public static void main(String[] args) throws Exception {
        String[] cliArgs = args.length == 0
                ? new String[] {
                        "records=10000000",
                        "modes=RANDOM",
                        "threads=auto",
                        "msd=12",
                        "lsd=13",
                        "tiny=128",
                        "tune=10000000",
                        "tuplePacking=true",
                        "tupleBits=16",
                        "reporting=summary"
                }
                : args;
        ApexEngine.main(cliArgs);
    }

    public static Reporter reporter() {
        return Reporters.current();
    }

    public static void setReporter(Reporter reporter) {
        Reporters.set(reporter);
    }

    public static Result sort(Data data) throws Exception {
        return sort(data, Configuration.defaultConfiguration());
    }

    public static Result sort(Data data, Configuration configuration) throws Exception {
        Objects.requireNonNull(data, "data");
        Objects.requireNonNull(configuration, "configuration");

        configuration.apply();
        boolean ownedPool = ensurePool();
        try {
            MemorySegment sorted = data.records() == 0
                    ? data.source()
                    : ApexEngine.sortPipeline(
                            data.source(),
                            data.destination(),
                            data.records(),
                            configuration.config()
                    );
            return new Result(sorted, data.records(), data.generatedMode());
        } finally {
            shutdownOwnedPool(ownedPool);
        }
    }

    public static Result sortAndVerify(Data data, Configuration configuration) throws Exception {
        Result result = sort(data, configuration);
        result.verifyIfGenerated(false);
        return result;
    }

    public static final class Data {
        private final MemorySegment source;
        private final MemorySegment destination;
        private final long records;
        private final DataMode generatedMode;

        private Data(MemorySegment source, MemorySegment destination, long records, DataMode generatedMode) {
            this.source = Objects.requireNonNull(source, "source");
            this.destination = Objects.requireNonNull(destination, "destination");
            this.records = records;
            this.generatedMode = generatedMode;
            validateRecords(records);
            requirePackedRecordSegment("source", source, records);
            requirePackedRecordSegment("destination", destination, records);
            requireSeparateSegments(source, destination, records);
        }

        public static Data of(MemorySegment source, MemorySegment destination, long records) {
            return ofPackedRecords(source, destination, records);
        }

        public static Data ofPackedRecords(MemorySegment source, MemorySegment destination, long records) {
            return new Data(source, destination, records, null);
        }

        public static Data generated(DataMode mode, long records, Arena arena) throws Exception {
            Objects.requireNonNull(mode, "mode");
            Objects.requireNonNull(arena, "arena");
            if (mode == DataMode.EMPTY && records != 0) {
                throw new IllegalArgumentException("EMPTY mode must use 0 records");
            }

            long bytes = Tools.bytesForRecords(records);
            long alignment = alignment();
            MemorySegment source = bytes == 0 ? MemorySegment.NULL : arena.allocate(bytes, alignment);
            MemorySegment destination = bytes == 0 ? MemorySegment.NULL : arena.allocate(bytes, alignment);

            for (long i = 0; i < records; i++) {
                writeRecord(source, i, DataGenerator.keyForMode(i, records, mode), i);
            }

            return new Data(source, destination, records, mode);
        }

        public static Data generated(String mode, long records, Arena arena) throws Exception {
            return generated(DataParser.parseMode(mode), records, arena);
        }

        public static Data fromKeys(long[] keys, Arena arena) {
            Objects.requireNonNull(keys, "keys");
            return fromKeyValueArrays(keys, null, arena);
        }

        public static Data fromKeyValueArrays(long[] keys, long[] values, Arena arena) {
            Objects.requireNonNull(keys, "keys");
            Objects.requireNonNull(arena, "arena");
            if (values != null && values.length != keys.length) {
                throw new IllegalArgumentException("values length must match keys length");
            }

            long records = keys.length;
            long bytes = Tools.bytesForRecords(records);
            long alignment = alignment();
            MemorySegment source = bytes == 0 ? MemorySegment.NULL : arena.allocate(bytes, alignment);
            MemorySegment destination = bytes == 0 ? MemorySegment.NULL : arena.allocate(bytes, alignment);

            for (int i = 0; i < keys.length; i++) {
                writeRecord(source, i, keys[i], values == null ? i : values[i]);
            }

            return new Data(source, destination, records, null);
        }

        public static Data fromPackedLongPairs(long[] keyValuePairs, Arena arena) {
            Objects.requireNonNull(keyValuePairs, "keyValuePairs");
            Objects.requireNonNull(arena, "arena");
            if ((keyValuePairs.length & 1) != 0) {
                throw new IllegalArgumentException("keyValuePairs must contain key,value pairs");
            }

            long records = keyValuePairs.length / 2L;
            long bytes = Tools.bytesForRecords(records);
            long alignment = alignment();
            MemorySegment source = bytes == 0 ? MemorySegment.NULL : arena.allocate(bytes, alignment);
            MemorySegment destination = bytes == 0 ? MemorySegment.NULL : arena.allocate(bytes, alignment);

            for (int i = 0; i < keyValuePairs.length; i += 2) {
                writeRecord(source, i / 2L, keyValuePairs[i], keyValuePairs[i + 1]);
            }

            return new Data(source, destination, records, null);
        }

        public MemorySegment source() {
            return source;
        }

        public MemorySegment destination() {
            return destination;
        }

        public long records() {
            return records;
        }

        public DataMode generatedMode() {
            return generatedMode;
        }
    }

    public record Result(MemorySegment records, long recordCount, DataMode generatedMode) {
        public boolean hasGeneratedMode() {
            return generatedMode != null;
        }

        public void verifyIfGenerated() {
            verifyIfGenerated(true);
        }

        public void verifyIfGenerated(boolean announce) {
            if (generatedMode != null) {
                verify(generatedMode, announce);
            }
        }

        public void verify(DataMode mode) {
            verify(mode, true);
        }

        public void verify(DataMode mode, boolean announce) {
            boolean ownedPool = ensurePool();
            try {
                Verifier.verify(records, recordCount, Objects.requireNonNull(mode, "mode"), announce);
            } finally {
                shutdownOwnedPool(ownedPool);
            }
        }
    }

    private static boolean ensurePool() {
        if (ApexEngine.POOL == null || ApexEngine.POOL.isShutdown()) {
            ApexEngine.POOL = Executors.newFixedThreadPool(ApexEngine.THREADS);
            return true;
        }
        return false;
    }

    private static void shutdownOwnedPool(boolean ownedPool) {
        if (ownedPool && ApexEngine.POOL != null) {
            ApexEngine.POOL.shutdown();
            ApexEngine.POOL = null;
        }
    }

    private static long alignment() {
        boolean isApexHardware =
                System.getProperty("os.arch").contains("amd64")
                        && Runtime.getRuntime().availableProcessors() >= 32;
        return isApexHardware ? 2L * 1024L * 1024L : 64L;
    }

    private static void validateRecords(long records) {
        Tools.bytesForRecords(records);
    }

    public static long bytesForRecordCount(long records) {
        return Tools.bytesForRecords(records);
    }

    public static long keyAt(MemorySegment records, long index) {
        return records.get(ApexEngine.LONG, checkedRecordOffset(records, index));
    }

    public static long valueAt(MemorySegment records, long index) {
        return records.get(ApexEngine.LONG, checkedRecordOffset(records, index) + Long.BYTES);
    }

    public static void writeRecord(MemorySegment records, long index, long key, long value) {
        long offset = checkedRecordOffset(records, index);
        records.set(ApexEngine.LONG, offset, key);
        records.set(ApexEngine.LONG, offset + Long.BYTES, value);
    }

    private static long checkedRecordOffset(MemorySegment records, long index) {
        Objects.requireNonNull(records, "records");
        if (index < 0) {
            throw new IllegalArgumentException("record index must be non-negative");
        }

        long offset = Math.multiplyExact(index, RECORD_BYTES);
        if (records.byteSize() < RECORD_BYTES || offset > records.byteSize() - RECORD_BYTES) {
            throw new IllegalArgumentException("record index is outside the segment: " + index);
        }
        return offset;
    }

    private static void requirePackedRecordSegment(String label, MemorySegment segment, long records) {
        long required = Tools.bytesForRecords(records);

        if (records > 0 && segment == MemorySegment.NULL) {
            throw new IllegalArgumentException(label + " segment must not be MemorySegment.NULL for non-empty data");
        }

        if (segment.byteSize() != required) {
            throw new IllegalArgumentException(label + " segment must be exactly " + required +
                    " bytes for " + records + " packed 16-byte records; slice wider row buffers first");
        }

        if (records > 0 && segment.isReadOnly()) {
            throw new IllegalArgumentException(label + " segment must be writable because A.P.E.X may sort in-place");
        }

        requireAccessibleByWorkerThreads(label, segment, records);
    }

    private static void requireAccessibleByWorkerThreads(String label, MemorySegment segment, long records) {
        if (records == 0) {
            return;
        }

        if (!segment.isAccessibleBy(Thread.currentThread())) {
            throw new IllegalArgumentException(label + " segment is not accessible by the calling thread");
        }

        boolean[] accessible = new boolean[1];
        Thread probe = new Thread(
                () -> accessible[0] = segment.isAccessibleBy(Thread.currentThread()),
                "apex-segment-access-check"
        );
        probe.start();
        try {
            probe.join();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalArgumentException("Interrupted while checking " + label + " segment accessibility", ex);
        }

        if (!accessible[0]) {
            throw new IllegalArgumentException(label +
                    " segment must be accessible by worker threads; use Arena.ofShared() for A.P.E.X data");
        }
    }

    private static void requireSeparateSegments(MemorySegment source, MemorySegment destination, long records) {
        if (records == 0) {
            return;
        }

        if (source.equals(destination) || source.asOverlappingSlice(destination).isPresent()) {
            throw new IllegalArgumentException("source and destination segments must not overlap");
        }
    }
}
