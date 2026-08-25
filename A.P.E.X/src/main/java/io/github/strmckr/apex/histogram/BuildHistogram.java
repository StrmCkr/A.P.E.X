package io.github.strmckr.apex.histogram;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.concurrent.Future;

import io.github.strmckr.apex.config.SortConfig;
import io.github.strmckr.apex.histogram.Histogram.HistogramResult;
import io.github.strmckr.apex.engine.ApexEngine;
import io.github.strmckr.apex.tools.Tools;

@SuppressWarnings({"removal", "preview"})
public class BuildHistogram {
    
    /**
     * Vectorized MSD histogram scanner.
     * Natively structures its register lane widths to match your host processor,
     * completely eliminating vector splitting penalties on your 1800X baseline environment.
     */
    public static HistogramResult buildMsdHistograms(
            MemorySegment src,
            long n,
            SortConfig cfg,
            int msdShift
    ) throws Exception {
        HistogramResult result = new HistogramResult(cfg);
        ArrayList<Future<?>> futures = new ArrayList<>(ApexEngine.THREADS);
        long chunk = n / ApexEngine.THREADS;
        int bucketCount = cfg.msdBucketCount;
        int bucketMask = bucketCount - 1;

        for (int t = 0; t < ApexEngine.THREADS; t++) {
            final int tid = t;

            futures.add(ApexEngine.POOL.submit(() -> {
                int[] hist = result.histograms[tid];
                long[] orMasks = result.orMasks[tid];
                long[] andMasks = result.andMasks[tid];
                long[] bucketFirstKeys = result.bucketFirstKeys[tid];
                long[] bucketLastKeys = result.bucketLastKeys[tid];
                boolean[] bucketSawKeys = result.bucketSawKeys[tid];
                boolean[] bucketAscending = result.bucketAscending[tid];
                boolean[] bucketDescending = result.bucketDescending[tid];
                long keyOrderXor = ApexEngine.KEY_ORDER_XOR;

                long s = tid * chunk;
                long e = (tid == ApexEngine.THREADS - 1) ? n : s + chunk;

                long p = s << 4;
                long end = e << 4;
                long threadStart = p;

                // Establish dynamic vector stride bounds based on active hardware width
                int requestedStepRecords = Integer.getInteger(
                        "apex.histogramStepRecords",
                        Math.max(4, io.github.strmckr.apex.engine.ApexEngine.RECORDS_PER_REG)
                );
                int stepRecords = Math.max(1, Math.min(16, requestedStepRecords));
                long strideBytes = (long) stepRecords << 4;
                long unrolledEnd = end - strideBytes;

                boolean sawAny = false;
                long firstKey = 0L;
                long lastKey = 0L;
                boolean ascending = true;
                boolean descending = true;
                long previousKey = 0L;

                if (p < end) {
                    firstKey = src.get(ApexEngine.LONG, p);
                    previousKey = firstKey;
                    sawAny = true;
                }


                // Primary vector loop
                while (p <= unrolledEnd) {
                    for (int i = 0; i < stepRecords; i++) {
                        long recordOffset = p + ((long) i << 4);
                        long k = src.get(ApexEngine.LONG, recordOffset);

                        if (recordOffset > threadStart && (ascending || descending)) {
                            int cmp = Long.compareUnsigned(previousKey ^ keyOrderXor, k ^ keyOrderXor);
                            ascending &= cmp <= 0;
                            descending &= cmp >= 0;
                        }
                        previousKey = k;
                        lastKey = k;

                        // Calculate bucket mappings using your exact bitwise parameters
                        int b = (int) (((k ^ keyOrderXor) >>> msdShift) & bucketMask);
                        if (bucketSawKeys[b]) {
                            if (bucketAscending[b] || bucketDescending[b]) {
                                int bucketCmp = Long.compareUnsigned(bucketLastKeys[b] ^ keyOrderXor, k ^ keyOrderXor);
                                bucketAscending[b] &= bucketCmp <= 0;
                                bucketDescending[b] &= bucketCmp >= 0;
                                bucketLastKeys[b] = k;
                            }
                        } else {
                            bucketFirstKeys[b] = k;
                            bucketSawKeys[b] = true;
                            bucketLastKeys[b] = k;
                        }
                        hist[b]++;
                        orMasks[b] |= k;
                        andMasks[b] &= k;
                    }

                    p += strideBytes; // Progresses exactly by your CPU's hardware register width footprint
                }

                // 🛬 Residual Scalar Tail Pass
                while (p < end) {
                    long k = src.get(ApexEngine.LONG, p);

                    if (p > threadStart && (ascending || descending)) {
                        int cmp = Long.compareUnsigned(previousKey ^ keyOrderXor, k ^ keyOrderXor);
                        ascending &= cmp <= 0;
                        descending &= cmp >= 0;
                    }
                    previousKey = k;
                    lastKey = k;

                    int b = (int) (((k ^ keyOrderXor) >>> msdShift) & bucketMask);
                    if (bucketSawKeys[b]) {
                        if (bucketAscending[b] || bucketDescending[b]) {
                            int bucketCmp = Long.compareUnsigned(bucketLastKeys[b] ^ keyOrderXor, k ^ keyOrderXor);
                            bucketAscending[b] &= bucketCmp <= 0;
                            bucketDescending[b] &= bucketCmp >= 0;
                            bucketLastKeys[b] = k;
                        }
                    } else {
                        bucketFirstKeys[b] = k;
                        bucketSawKeys[b] = true;
                        bucketLastKeys[b] = k;
                    }
                    hist[b]++;
                    orMasks[b] |= k;
                    andMasks[b] &= k;

                    p += ApexEngine.RECORD_BYTES;
                }

                result.sawKeys[tid] = sawAny;
                result.firstKeys[tid] = firstKey;
                result.lastKeys[tid] = lastKey;
                result.ascending[tid] = ascending;
                result.descending[tid] = descending;
            }));
        }

        Tools.waitForFutures(futures);
        return result;
    }
}
