package histogram;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.concurrent.Future;

import Tools.tools;
import config.configurations.Config;
import histogram.histogram.HistogramResult;
import main.Apex;

public class buildhistogram {
	public  static HistogramResult buildMsdHistograms(
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
                long unrolledEnd = end - (4L * Apex.RECORD_BYTES);

                int[] histogram = result.histograms[tid];
                boolean saw = false;
                boolean ascending = true;
                boolean descending = true;
                long first = 0L;
                long previous = 0L;

                while (p <= unrolledEnd) {
                    long k0 = src.get(Apex.LONG, p);
                    long k1 = src.get(Apex.LONG, p + 16);
                    long k2 = src.get(Apex.LONG, p + 32);
                    long k3 = src.get(Apex.LONG, p + 48);

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
                        ascending &= cmp01 <= 0 && cmp12 <= 0 && cmp23 <= 0;
                        descending &= cmp01 >= 0 && cmp12 >= 0 && cmp23 >= 0;
                        previous = k3;
                    }

                    int b0 = (int) ((k0 >>> msdShift) & bucketMask);
                    int b1 = (int) ((k1 >>> msdShift) & bucketMask);
                    int b2 = (int) ((k2 >>> msdShift) & bucketMask);
                    int b3 = (int) ((k3 >>> msdShift) & bucketMask);

                    histogram[b0]++;
                    orMask[b0] |= k0;
                    andMask[b0] &= k0;

                    histogram[b1]++;
                    orMask[b1] |= k1;
                    andMask[b1] &= k1;

                    histogram[b2]++;
                    orMask[b2] |= k2;
                    andMask[b2] &= k2;

                    histogram[b3]++;
                    orMask[b3] |= k3;
                    andMask[b3] &= k3;

                    p += 4L * Apex.RECORD_BYTES;
                }

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
