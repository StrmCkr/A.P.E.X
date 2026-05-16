package scatter;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.concurrent.Future;

import MSD.msdbucketplan.MsdBucketPlan;
import Tools.tools;
import config.configurations.Config;
import main.Apex;

public class scattered {
    public static void scatterIntoMsdBuckets(
            MemorySegment src,
            MemorySegment dst,
            long n,
            MsdBucketPlan plan,
            Config cfg
    ) throws Exception {
        ArrayList<Future<?>> futures = new ArrayList<>(Apex.THREADS);
        long chunk = n / Apex.THREADS;
        int bucketMask = cfg.msdBucketCount - 1;

        for (int t = 0; t < Apex.THREADS; t++) {
            final int tid = t;

            futures.add(Apex.POOL.submit(() -> {
                int[] out = plan.threadScatterOffsets[tid];

                long s = tid * chunk;
                long e = (tid == Apex.THREADS - 1) ? n : s + chunk;

                long p = s << 4;
                long end = e << 4;
                
                // Calculate the safety boundary for 4x unrolling (4 records * 16 bytes = 64 bytes)
                long unrolledEnd = end - 64;

                // Phase 1: High-throughput 4x unrolled processing
                while (p < unrolledEnd) {
                    // Coalesced sequential reads (exactly 1 cache line)
                    long k0 = src.get(Apex.LONG, p);      long v0 = src.get(Apex.LONG, p + 8);
                    long k1 = src.get(Apex.LONG, p + 16);     long v1 = src.get(Apex.LONG, p + 24);
                    long k2 = src.get(Apex.LONG, p + 32);     long v2 = src.get(Apex.LONG, p + 40);
                    long k3 = src.get(Apex.LONG, p + 48);     long v3 = src.get(Apex.LONG, p + 56);

                    // Parallel address bit extractions
                    int b0 = (int) ((k0 >>> plan.msdShift) & bucketMask);
                    int b1 = (int) ((k1 >>> plan.msdShift) & bucketMask);
                    int b2 = (int) ((k2 >>> plan.msdShift) & bucketMask);
                    int b3 = (int) ((k3 >>> plan.msdShift) & bucketMask);

                    // Independent parallel writes
                    dst.set(Apex.LONG, (plan.starts[b0] + out[b0]++) << 4, k0);
                    dst.set(Apex.LONG, ((plan.starts[b0] + out[b0] - 1) << 4) + 8, v0);

                    dst.set(Apex.LONG, (plan.starts[b1] + out[b1]++) << 4, k1);
                    dst.set(Apex.LONG, ((plan.starts[b1] + out[b1] - 1) << 4) + 8, v1);

                    dst.set(Apex.LONG, (plan.starts[b2] + out[b2]++) << 4, k2);
                    dst.set(Apex.LONG, ((plan.starts[b2] + out[b2] - 1) << 4) + 8, v2);

                    dst.set(Apex.LONG, (plan.starts[b3] + out[b3]++) << 4, k3);
                    dst.set(Apex.LONG, ((plan.starts[b3] + out[b3] - 1) << 4) + 8, v3);

                    p += 64; // Advance by exactly one 64-byte cache line
                }

                // Phase 2: Safe sequential cleanup loop for remaining fractional elements
                while (p < end) {
                    long k = src.get(Apex.LONG, p);
                    long v = src.get(Apex.LONG, p + 8);

                    int b = (int) ((k >>> plan.msdShift) & bucketMask);
                    long q = (plan.starts[b] + out[b]++) << 4;

                    dst.set(Apex.LONG, q, k);
                    dst.set(Apex.LONG, q + 8, v);

                    p += Apex.RECORD_BYTES;
                }
            }));
        }

        tools.waitForFutures(futures);
    }
}