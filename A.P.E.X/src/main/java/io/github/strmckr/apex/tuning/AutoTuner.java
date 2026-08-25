package io.github.strmckr.apex.tuning;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Arrays;

import io.github.strmckr.apex.config.Configurations;
import io.github.strmckr.apex.config.RunOptions.Options;
import io.github.strmckr.apex.config.SortConfig;
import io.github.strmckr.apex.generator.DataInitializer;
import io.github.strmckr.apex.generator.DataMode;
import io.github.strmckr.apex.engine.ApexEngine;
import io.github.strmckr.apex.reporting.Reporters;
import io.github.strmckr.apex.tools.Tools;
import io.github.strmckr.apex.tools.Verifier;
import io.github.strmckr.apex.tuples.Tuples;

public final class AutoTuner {

    private AutoTuner() {
    }

    public static SortConfig selectRunConfig(long alignment, Options options) throws Exception {
        if (options.lockedConfig != null) {
            return options.lockedConfig;
        }

        DataMode tuneMode = Tools.firstNonEmptyMode(options.modes);
        long recordBasis = Tools.firstPositiveRecord(options.recordsList);

        if (options.sweep) {
            recordBasis = Tools.firstPositive(recordBasis, options.sweepRecords);
            if (tuneMode == null) {
                tuneMode = DataMode.RANDOM;
            }
        }

        if (tuneMode == null || recordBasis <= 0) {
            SortConfig fallback = Configurations.defaultConfig();
            Reporters.println("Auto-tune skipped (no non-empty run requested). Using " + fallback);
            return fallback;
        }

        if (Reporters.isDetailEnabled()) {
            Reporters.println("Auto-tune mode: " + tuneMode);
            Reporters.println("Auto-tune record basis: " + recordBasis);
        } else if (Reporters.isSummary()) {
            Reporters.println("Auto-tune: mode=" + tuneMode + " records=" + recordBasis);
        }
        return autoTune(alignment, tuneMode, recordBasis, options);
    }

    public static SortConfig autoTune(long alignment, DataMode mode, long records, Options options) throws Exception {
        if (mode == DataMode.EMPTY || records <= 0) {
            Reporters.println("Auto-tune skipped (EMPTY/no-record mode)");
            return Configurations.defaultConfig();
        }

        SortConfig[] candidates = buildConfigCandidates(
                options.minMsdBits,
                options.maxMsdBits,
                options.minLsdBits,
                options.maxLsdBits,
                options.minTiny,
                options.maxTiny
        );

        long testN = Math.min(records, options.tuneRecords);
        warmUp("Auto-tune warmup", Configurations.defaultConfig(), testN, alignment, mode);

        if (Reporters.isDetailEnabled()) {
            Reporters.println("Auto-tuning on subset: " + testN + " / full=" + records);
            Reporters.println("Auto-tune repeats: warmups=" + ApexEngine.TUNE_WARMUPS +
                    " runs=" + ApexEngine.TUNE_RUNS + " score=median");
        } else if (Reporters.isSummary()) {
            Reporters.println("Auto-tune subset: " + testN + " records");
        }

        SortConfig best = candidates[0];
        double bestSec = Double.POSITIVE_INFINITY;
        boolean measuredAny = false;

        for (SortConfig cfg : candidates) {
            if (!memoryLooksReasonable(cfg)) {
                if (Reporters.isDetailEnabled()) {
                    Reporters.println("SKIP " + cfg + " memory estimate too high");
                }
                continue;
            }

            double sec = benchmarkCandidateMedian(cfg, testN, alignment, mode);
            measuredAny = true;
            if (Reporters.isDetailEnabled()) {
                Reporters.printf("TUNE %-42s median %.3f sec | %.2f M rec/sec (subset=%d)%n",
                        cfg, sec, (testN / sec) / 1e6, testN);
            }

            if (sec < bestSec) {
                bestSec = sec;
                best = cfg;
            }
        }

        if (!measuredAny) {
            best = Configurations.defaultConfig();
            Reporters.println("Auto-tune found no memory-safe candidates. Using " + best);
        }

        Reporters.println("Auto-tune picked: " + best);
        return best;
    }

    public static void warmUp(String label, SortConfig cfg, long warmN, long alignment, DataMode mode)
            throws Exception {
        if (warmN <= 0 || mode == DataMode.EMPTY) {
            return;
        }

        if (Reporters.isDetailEnabled()) {
            Reporters.println(label + " on " + warmN + " records using " + cfg + "...");
        }
        double sec = benchmarkCandidate(cfg, warmN, alignment, mode, false);
        if (Reporters.isDetailEnabled()) {
            Reporters.printf("%s %.3f sec | %.2f M/sec%n", label, sec, (warmN / sec) / 1e6);
        } else if (Reporters.isSummary()) {
            Reporters.printf("%s: %.3f sec | %.2f M/sec%n", label, sec, (warmN / sec) / 1e6);
        }
    }

    public static SortConfig[] buildConfigCandidates(
            int minMsdBits,
            int maxMsdBits,
            int minLsdBits,
            int maxLsdBits,
            int minTinyPartitionThreshold,
            int maxTinyPartitionThreshold
    ) {
        ArrayList<SortConfig> candidates = new ArrayList<>();

        for (int msdBits = minMsdBits; msdBits <= maxMsdBits; msdBits++) {
            for (int lsdBits = minLsdBits; lsdBits <= maxLsdBits; lsdBits++) {
                for (int tiny = minTinyPartitionThreshold; tiny <= maxTinyPartitionThreshold; tiny <<= 1) {
                    candidates.add(new SortConfig(msdBits, lsdBits, tiny));
                }
            }
        }

        return candidates.toArray(new SortConfig[0]);
    }

    public static boolean memoryLooksReasonable(SortConfig cfg) {
        long heapMax = Runtime.getRuntime().maxMemory();
        long bucketThreadCells = (long) ApexEngine.THREADS * cfg.msdBucketCount;

        long histBytes = bucketThreadCells * Integer.BYTES;
        long masksBytes = bucketThreadCells * 2L * Long.BYTES;
        long scatterOffsetBytes = bucketThreadCells * Integer.BYTES;
        long countsBytes = (long) ApexEngine.THREADS *
                Math.max(cfg.lsdRadix, Tuples.directTupleRadixCap()) * Integer.BYTES;
        long planBytes = (long) cfg.msdBucketCount * (
                Long.BYTES +
                Integer.BYTES +
                Byte.BYTES +
                Long.BYTES +
                Long.BYTES +
                Long.BYTES +
                Integer.BYTES +
                4L * Long.BYTES
        );
        long scratchBytes = (long) ApexEngine.THREADS * ApexEngine.MAX_HEAP_SCRATCH_RECORDS * 4L * Long.BYTES;

        long estimate = histBytes + masksBytes + scatterOffsetBytes + countsBytes + planBytes + scratchBytes;

        return estimate < heapMax / 3;
    }

    static double benchmarkCandidateMedian(SortConfig cfg, long testN, long alignment, DataMode mode)
            throws Exception {
        return benchmarkCandidateMedian(cfg, testN, alignment, mode, ApexEngine.TUNE_WARMUPS, ApexEngine.TUNE_RUNS);
    }

    static double benchmarkCandidateMedian(
            SortConfig cfg,
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

    static double benchmarkCandidate(
            SortConfig cfg,
            long testN,
            long alignment,
            DataMode mode,
            boolean announceVerify
    ) throws Exception {
        try (Arena arena = Arena.ofShared()) {
            long bytes = Tools.bytesForRecords(testN);
            MemorySegment src = arena.allocate(bytes, alignment);
            MemorySegment dst = arena.allocate(bytes, alignment);

            DataInitializer.initData(src, testN, mode);

            long start = System.nanoTime();
            MemorySegment sorted = ApexEngine.sortPipeline(src, dst, testN, cfg);

            double sec = elapsed(start);

            Verifier.verify(sorted, testN, mode, announceVerify);

            return sec;
        }
    }

    static double elapsed(long start) {
        return (System.nanoTime() - start) / 1e9;
    }
}
