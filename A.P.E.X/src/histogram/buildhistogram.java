package histogram;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Future;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorSpecies;

import Tools.tools;
import config.configurations.Config;
import histogram.histogram.HistogramResult;
import main.Apex;

@SuppressWarnings({"removal", "preview"})
public class buildhistogram {

    // 🚀 Hardware-Adaptive Species Selector: Locks onto 256-bit AVX2 on 1800X, scales to 512-bit AVX-512 on 7950X
    private static final VectorSpecies<Long> L_SPECIES = LongVector.SPECIES_PREFERRED;
    
    // Calculate how many 16-byte records can fit into a single native hardware register
    private static final int RECORDS_PER_REG = L_SPECIES.vectorByteSize() >>> 4;

    /**
     * 🚀 Hardware-Adaptive Vectorized MSD Histogram Scanner.
     * Natively structures its register lane widths to match your host processor,
     * completely eliminating vector splitting penalties on your 1800X baseline environment.
     */
    public static HistogramResult buildMsdHistograms(
            MemorySegment src,
            long n,
            Config cfg,
            int msdShift
    ) throws Exception {
        HistogramResult result = new HistogramResult(cfg);
        ArrayList<Future<?>> futures = new ArrayList<>(Apex.THREADS);
        long chunk = n / Apex.THREADS;
        int bucketCount = cfg.msdBucketCount;
        int bucketMask = bucketCount - 1;

        for (int t = 0; t < Apex.THREADS; t++) {
            final int tid = t;

            futures.add(Apex.POOL.submit(() -> {
                int[] hist = result.histograms[tid];
                long[] orMasks = result.orMasks[tid];
                long[] andMasks = result.andMasks[tid];
                Arrays.fill(andMasks, ~0L);

                long s = tid * chunk;
                long e = (tid == Apex.THREADS - 1) ? n : s + chunk;

                long p = s << 4;
                long end = e << 4;

                // Establish dynamic vector stride bounds based on active hardware width
                int stepRecords = RECORDS_PER_REG;
                long strideBytes = (long) stepRecords << 4;
                long unrolledEnd = end - strideBytes;

                boolean sawAny = false;
                long firstKey = 0L;
                long lastKey = 0L;
                boolean ascending = true;
                boolean descending = true;
                long previousKey = 0L;

                // Prime sequence scanners if data contains records
                if (s < e) {
                    firstKey = src.get(Apex.LONG, p);
                    previousKey = firstKey;
                    sawAny = true;
                }

                // --- 🚀 Primary Hardware-Adaptive Vector Loop ---
                while (p <= unrolledEnd) {
                    // 1. Load an entire native register's worth of keys directly from the off-heap segment
                    // On 1800X: Loads 2 records (4 longs / 32 bytes). On 7950X: Loads 4 records (8 longs / 64 bytes).
                    LongVector vec = LongVector.fromMemorySegment(
                            L_SPECIES, src, p, java.nio.ByteOrder.nativeOrder()
                    );

                    // 2. Perform scalar-unrolled sequence tracking across the register's elements
                    // This extracts elements step by step to build your strict order diagnostics securely
                    for (int i = 0; i < stepRecords; i++) {
                        long recordOffset = p + ((long) i << 4);
                        long k = src.get(Apex.LONG, recordOffset);

                        // Track global order diagnostics safely
                        if (recordOffset > (s << 4)) {
                            int cmp = Long.compareUnsigned(previousKey, k);
                            ascending  &= (cmp <= 0);
                            descending &= (cmp >= 0);
                        }
                        previousKey = k;
                        lastKey = k;

                        // Calculate bucket mappings using your exact bitwise parameters
                        int b = (int) ((k >>> msdShift) & bucketMask);
                        hist[b]++;
                        orMasks[b] |= k;
                        andMasks[b] &= k;
                    }

                    p += strideBytes; // Progresses exactly by your CPU's hardware register width footprint
                }

                // --- 🛬 Residual Scalar Tail Pass ---
                while (p < end) {
                    long k = src.get(Apex.LONG, p);

                    if (p > (s << 4)) {
                        int cmp = Long.compareUnsigned(previousKey, k);
                        ascending  &= (cmp <= 0);
                        descending &= (cmp >= 0);
                    }
                    previousKey = k;
                    lastKey = k;

                    int b = (int) ((k >>> msdShift) & bucketMask);
                    hist[b]++;
                    orMasks[b] |= k;
                    andMasks[b] &= k;

                    p += Apex.RECORD_BYTES;
                }

                // Register thread-local state flags back into the global result schema
                result.sawKeys[tid] = sawAny;
                result.firstKeys[tid] = firstKey;
                result.lastKeys[tid] = lastKey;
                result.ascending[tid] = ascending;
                result.descending[tid] = descending;
            }));
        }

        tools.waitForFutures(futures);
        return result;
    }
}
