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

                int[] histogram = result.histograms[tid];

                while (p < end) {
                    long k = src.get(Apex.LONG, p);

                    int b = (int) ((k >>> msdShift) & bucketMask);

                    histogram[b]++;

                    orMask[b] |= k;
                    andMask[b] &= k;

                    p += Apex.RECORD_BYTES;
                }
            }));
        }

        tools.waitForFutures(futures);

        return result;
    }
}
