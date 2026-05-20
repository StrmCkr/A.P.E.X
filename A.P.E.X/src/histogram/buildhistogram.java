package histogram;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.concurrent.Future;

// Unlocks native 7950X hardware registers under JDK 25
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorSpecies;
import jdk.incubator.vector.VectorOperators;

import Tools.tools;
import config.configurations.Config;
import histogram.histogram.HistogramResult;
import main.Apex;

public class buildhistogram {
    
    // Unlocks 512-bit registers (Holds 8 separate 64-bit longs)
    private static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_512;

    public static HistogramResult buildMsdHistograms(
            MemorySegment src,
            long n,
            Config cfg,
            int msdShift
    ) throws Exception {
        HistogramResult result = new HistogramResult(cfg);

        ArrayList<Future<?>> futures = new ArrayList<>(Apex.THREADS);

        long chunk = n / Apex.THREADS;
        int bucketMask = cfg.msdBucketCount - 1;
        
        for (int t = 0; t < Apex.THREADS; t++) {
            final int tid = t;
            long[] orMask = result.orMasks[tid];
            long[] andMask = result.andMasks[tid];
            futures.add(Apex.POOL.submit(() -> {
                long s = tid * chunk;
                long e = (tid == Apex.THREADS - 1) ? n : s + chunk;

                long p = s << 4;
                long end = e << 4;
                
                // Vector step width: 8 records * 16 bytes per record = 128 bytes per loop pass
                long vectorStrideBytes = 8L * Apex.RECORD_BYTES;
                long vectorizedEnd = end - vectorStrideBytes;

                int[] histogram = result.histograms[tid];
                boolean saw = false;
                boolean ascending = true;
                boolean descending = true;
                long first = 0L;
                long previous = 0L;

                // --- 🚀 Primary 512-Bit Vector Processing Block ---
                while (p <= vectorizedEnd) {
                    
                    // Native Vector Gather: Pull keys directly via 16-byte address stride adjustments
                    long k0 = src.get(Apex.LONG, p);
                    long k1 = src.get(Apex.LONG, p + 16);
                    long k2 = src.get(Apex.LONG, p + 32);
                    long k3 = src.get(Apex.LONG, p + 48);
                    long k4 = src.get(Apex.LONG, p + 64);
                    long k5 = src.get(Apex.LONG, p + 80);
                    long k6 = src.get(Apex.LONG, p + 96);
                    long k7 = src.get(Apex.LONG, p + 112);

                    // Load elements cleanly into 512-bit register lanes
                    LongVector keys = LongVector.fromArray(SPECIES, new long[]{k0, k1, k2, k3, k4, k5, k6, k7}, 0);

                    // Monotonic Ordering Micro-Check Verification
                    if (ascending || descending) {
                        if (!saw) {
                            first = k0;
                            saw = true;
                        } else {
                            int cmp = Long.compareUnsigned(previous, k0);
                            ascending &= cmp <= 0;
                            descending &= cmp >= 0;
                        }

                        int cmp01 = Long.compareUnsigned(k0, k1);
                        int cmp12 = Long.compareUnsigned(k1, k2);
                        int cmp23 = Long.compareUnsigned(k2, k3);
                        int cmp34 = Long.compareUnsigned(k3, k4);
                        int cmp45 = Long.compareUnsigned(k4, k5);
                        int cmp56 = Long.compareUnsigned(k5, k6);
                        int cmp67 = Long.compareUnsigned(k6, k7);

                        ascending &= (cmp01 <= 0 && cmp12 <= 0 && cmp23 <= 0 && cmp34 <= 0 && cmp45 <= 0 && cmp56 <= 0 && cmp67 <= 0);
                        descending &= (cmp01 >= 0 && cmp12 >= 0 && cmp23 >= 0 && cmp34 >= 0 && cmp45 >= 0 && cmp56 >= 0 && cmp67 >= 0);
                        previous = k7;
                    }

                    // Vectorized Radix Computation: (keys >>> msdShift) & bucketMask
                    LongVector bucketIndices = keys.lanewise(VectorOperators.LSHR, msdShift)
                                                   .lanewise(VectorOperators.AND, bucketMask);

                    // Drain vector registers into metrics tracking maps without atomic bottlenecks
                    int b0 = (int) bucketIndices.lane(0);
                    int b1 = (int) bucketIndices.lane(1);
                    int b2 = (int) bucketIndices.lane(2);
                    int b3 = (int) bucketIndices.lane(3);
                    int b4 = (int) bucketIndices.lane(4);
                    int b5 = (int) bucketIndices.lane(5);
                    int b6 = (int) bucketIndices.lane(6);
                    int b7 = (int) bucketIndices.lane(7);

                    histogram[b0]++; orMask[b0] |= k0; andMask[b0] &= k0;
                    histogram[b1]++; orMask[b1] |= k1; andMask[b1] &= k1;
                    histogram[b2]++; orMask[b2] |= k2; andMask[b2] &= k2;
                    histogram[b3]++; orMask[b3] |= k3; andMask[b3] &= k3;
                    histogram[b4]++; orMask[b4] |= k4; andMask[b4] &= k4;
                    histogram[b5]++; orMask[b5] |= k5; andMask[b5] &= k5;
                    histogram[b6]++; orMask[b6] |= k6; andMask[b6] &= k6;
                    histogram[b7]++; orMask[b7] |= k7; andMask[b7] &= k7;

                    p += vectorStrideBytes;
                }

                // --- 🛬 Tail Clean-up Block: Process remaining records ---
                while (p < end) {
                    long k = src.get(Apex.LONG, p);

                    if (ascending || descending) {
                        if (!saw) {
                            first = k;
                            saw = true;
                        } else {
                            int cmp = Long.compareUnsigned(previous, k);
                            ascending &= cmp <= 0;
                            descending &= cmp >= 0;
                        }
                        previous = k;
                    }

                    int b = (int) ((k >>> msdShift) & bucketMask);

                    histogram[b]++;
                    orMask[b] |= k;
                    andMask[b] &= k;

                    p += Apex.RECORD_BYTES;
                }

                result.sawKeys[tid] = saw;
                result.firstKeys[tid] = first;
                result.lastKeys[tid] = previous;
                result.ascending[tid] = ascending;
                result.descending[tid] = descending;
            }));
        }

        tools.waitForFutures(futures);

        return result;
    }
}
