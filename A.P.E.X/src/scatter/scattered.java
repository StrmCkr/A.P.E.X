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
        long[] bucketStarts = plan.starts;
        int[] bucketSizes = plan.sizes;
        byte[] bucketFlags = plan.bucketFlags;
        int msdShift = plan.msdShift;
        boolean hasLocalMsd = plan.hasLocalMsd;
        int[] localMsdShifts = plan.localMsdShifts;
        long[][] localStarts = plan.localStarts;
        int[][] localSizes = plan.localSizes;
        boolean[][] localAscending = plan.localAscending;
        boolean[][] localDescending = plan.localDescending;
        int[][][] localThreadOffsets = plan.localThreadScatterOffsets;
        boolean hasDescendingScatter = hasDescendingScatterWork(plan, cfg);

        for (int t = 0; t < Apex.THREADS; t++) {
            final int tid = t;

            futures.add(Apex.POOL.submit(() -> {
                int[] out = plan.threadScatterOffsets[tid];
                long keyOrderXor = Apex.KEY_ORDER_XOR;

                long s = tid * chunk;
                long e = (tid == Apex.THREADS - 1) ? n : s + chunk;

                long p = s << 4;
                long end = e << 4;
                long unrolledEnd = end - (4L * Apex.RECORD_BYTES);

                if (!hasLocalMsd) {
                    if (hasDescendingScatter) {
                        while (p <= unrolledEnd) {
                            long k0 = src.get(Apex.LONG, p);      long v0 = src.get(Apex.LONG, p + 8);
                            long k1 = src.get(Apex.LONG, p + 16); long v1 = src.get(Apex.LONG, p + 24);
                            long k2 = src.get(Apex.LONG, p + 32); long v2 = src.get(Apex.LONG, p + 40);
                            long k3 = src.get(Apex.LONG, p + 48); long v3 = src.get(Apex.LONG, p + 56);

                            int b0 = (int) (((k0 ^ keyOrderXor) >>> msdShift) & bucketMask);
                            int b1 = (int) (((k1 ^ keyOrderXor) >>> msdShift) & bucketMask);
                            int b2 = (int) (((k2 ^ keyOrderXor) >>> msdShift) & bucketMask);
                            int b3 = (int) (((k3 ^ keyOrderXor) >>> msdShift) & bucketMask);

                            long q0 = bucketFlags[b0] == Apex.BUCKET_DESCENDING
                                    ? (bucketStarts[b0] + bucketSizes[b0] - 1L - out[b0]) << 4
                                    : (bucketStarts[b0] + out[b0]) << 4;
                            out[b0]++;
                            dst.set(Apex.LONG, q0, k0); dst.set(Apex.LONG, q0 + 8, v0);

                            long q1 = bucketFlags[b1] == Apex.BUCKET_DESCENDING
                                    ? (bucketStarts[b1] + bucketSizes[b1] - 1L - out[b1]) << 4
                                    : (bucketStarts[b1] + out[b1]) << 4;
                            out[b1]++;
                            dst.set(Apex.LONG, q1, k1); dst.set(Apex.LONG, q1 + 8, v1);

                            long q2 = bucketFlags[b2] == Apex.BUCKET_DESCENDING
                                    ? (bucketStarts[b2] + bucketSizes[b2] - 1L - out[b2]) << 4
                                    : (bucketStarts[b2] + out[b2]) << 4;
                            out[b2]++;
                            dst.set(Apex.LONG, q2, k2); dst.set(Apex.LONG, q2 + 8, v2);

                            long q3 = bucketFlags[b3] == Apex.BUCKET_DESCENDING
                                    ? (bucketStarts[b3] + bucketSizes[b3] - 1L - out[b3]) << 4
                                    : (bucketStarts[b3] + out[b3]) << 4;
                            out[b3]++;
                            dst.set(Apex.LONG, q3, k3); dst.set(Apex.LONG, q3 + 8, v3);

                            p += 4L * Apex.RECORD_BYTES;
                        }

                        while (p < end) {
                            long k = src.get(Apex.LONG, p);
                            long v = src.get(Apex.LONG, p + 8);
                            int b = (int) (((k ^ keyOrderXor) >>> msdShift) & bucketMask);
                            long q = bucketFlags[b] == Apex.BUCKET_DESCENDING
                                    ? (bucketStarts[b] + bucketSizes[b] - 1L - out[b]) << 4
                                    : (bucketStarts[b] + out[b]) << 4;
                            out[b]++;
                            dst.set(Apex.LONG, q, k);
                            dst.set(Apex.LONG, q + 8, v);
                            p += Apex.RECORD_BYTES;
                        }
                        return;
                    }

                    long unrolledEnd8 = end - (8L * Apex.RECORD_BYTES);
                    while (p <= unrolledEnd8) {
                        long k0 = src.get(Apex.LONG, p);      long v0 = src.get(Apex.LONG, p + 8);
                        long k1 = src.get(Apex.LONG, p + 16); long v1 = src.get(Apex.LONG, p + 24);
                        long k2 = src.get(Apex.LONG, p + 32); long v2 = src.get(Apex.LONG, p + 40);
                        long k3 = src.get(Apex.LONG, p + 48); long v3 = src.get(Apex.LONG, p + 56);
                        long k4 = src.get(Apex.LONG, p + 64); long v4 = src.get(Apex.LONG, p + 72);
                        long k5 = src.get(Apex.LONG, p + 80); long v5 = src.get(Apex.LONG, p + 88);
                        long k6 = src.get(Apex.LONG, p + 96); long v6 = src.get(Apex.LONG, p + 104);
                        long k7 = src.get(Apex.LONG, p + 112); long v7 = src.get(Apex.LONG, p + 120);

                        int b0 = (int) (((k0 ^ keyOrderXor) >>> msdShift) & bucketMask);
                        int b1 = (int) (((k1 ^ keyOrderXor) >>> msdShift) & bucketMask);
                        int b2 = (int) (((k2 ^ keyOrderXor) >>> msdShift) & bucketMask);
                        int b3 = (int) (((k3 ^ keyOrderXor) >>> msdShift) & bucketMask);
                        int b4 = (int) (((k4 ^ keyOrderXor) >>> msdShift) & bucketMask);
                        int b5 = (int) (((k5 ^ keyOrderXor) >>> msdShift) & bucketMask);
                        int b6 = (int) (((k6 ^ keyOrderXor) >>> msdShift) & bucketMask);
                        int b7 = (int) (((k7 ^ keyOrderXor) >>> msdShift) & bucketMask);

                        long q0 = (bucketStarts[b0] + out[b0]) << 4; out[b0]++;
                        dst.set(Apex.LONG, q0, k0); dst.set(Apex.LONG, q0 + 8, v0);

                        long q1 = (bucketStarts[b1] + out[b1]) << 4; out[b1]++;
                        dst.set(Apex.LONG, q1, k1); dst.set(Apex.LONG, q1 + 8, v1);

                        long q2 = (bucketStarts[b2] + out[b2]) << 4; out[b2]++;
                        dst.set(Apex.LONG, q2, k2); dst.set(Apex.LONG, q2 + 8, v2);

                        long q3 = (bucketStarts[b3] + out[b3]) << 4; out[b3]++;
                        dst.set(Apex.LONG, q3, k3); dst.set(Apex.LONG, q3 + 8, v3);

                        long q4 = (bucketStarts[b4] + out[b4]) << 4; out[b4]++;
                        dst.set(Apex.LONG, q4, k4); dst.set(Apex.LONG, q4 + 8, v4);

                        long q5 = (bucketStarts[b5] + out[b5]) << 4; out[b5]++;
                        dst.set(Apex.LONG, q5, k5); dst.set(Apex.LONG, q5 + 8, v5);

                        long q6 = (bucketStarts[b6] + out[b6]) << 4; out[b6]++;
                        dst.set(Apex.LONG, q6, k6); dst.set(Apex.LONG, q6 + 8, v6);

                        long q7 = (bucketStarts[b7] + out[b7]) << 4; out[b7]++;
                        dst.set(Apex.LONG, q7, k7); dst.set(Apex.LONG, q7 + 8, v7);

                        p += 8L * Apex.RECORD_BYTES;
                    }

                    while (p < end) {
                        long k = src.get(Apex.LONG, p);
                        long v = src.get(Apex.LONG, p + 8);
                        int b = (int) (((k ^ keyOrderXor) >>> msdShift) & bucketMask);
                        long q = (bucketStarts[b] + out[b]) << 4;
                        out[b]++;
                        dst.set(Apex.LONG, q, k);
                        dst.set(Apex.LONG, q + 8, v);
                        p += Apex.RECORD_BYTES;
                    }
                    return;
                }

                while (p < end) {
                    long k = src.get(Apex.LONG, p);
                    long v = src.get(Apex.LONG, p + 8);
                    int b = (int) (((k ^ keyOrderXor) >>> msdShift) & bucketMask);
                    int localShift = localMsdShifts[b];
                    long q;
                    if (localShift >= 0) {
                        int child = (int) (((k ^ keyOrderXor) >>> localShift) & (localSizes[b].length - 1));
                        int[] childOut = localThreadOffsets[b][tid];
                        if (localDescending[b] != null && localDescending[b][child] &&
                                (localAscending[b] == null || !localAscending[b][child])) {
                            q = (localStarts[b][child] + localSizes[b][child] - 1L - childOut[child]) << 4;
                            childOut[child]++;
                        } else {
                            q = (localStarts[b][child] + childOut[child]) << 4;
                            childOut[child]++;
                        }
                    } else if (bucketFlags[b] == Apex.BUCKET_DESCENDING) {
                        q = (bucketStarts[b] + bucketSizes[b] - 1L - out[b]) << 4;
                        out[b]++;
                    } else {
                        q = (bucketStarts[b] + out[b]) << 4;
                        out[b]++;
                    }
                    dst.set(Apex.LONG, q, k);
                    dst.set(Apex.LONG, q + 8, v);
                    p += Apex.RECORD_BYTES;
                }
            }));
        }

        tools.waitForFutures(futures);
        if (hasDescendingScatter) {
            markDescendingScatterNormalized(plan, cfg);
        }
    }

    private static boolean hasDescendingScatterWork(MsdBucketPlan plan, Config cfg) {
        for (int b = 0; b < cfg.msdBucketCount; b++) {
            if (plan.bucketFlags[b] == Apex.BUCKET_DESCENDING) {
                return true;
            }

            boolean[] local = plan.localDescending[b];
            if (local == null) {
                continue;
            }

            for (int child = 0; child < local.length; child++) {
                if (local[child] &&
                        (plan.localAscending[b] == null || !plan.localAscending[b][child])) {
                    return true;
                }
            }
        }

        return false;
    }

    private static void markDescendingScatterNormalized(MsdBucketPlan plan, Config cfg) {
        for (int b = 0; b < cfg.msdBucketCount; b++) {
            if (plan.localMsdShifts[b] >= 0 && plan.localDescending[b] != null) {
                boolean[] localAscending = plan.localAscending[b];
                boolean[] localDescending = plan.localDescending[b];

                for (int child = 0; child < localDescending.length; child++) {
                    if (!localDescending[child] ||
                            (localAscending != null && localAscending[child])) {
                        continue;
                    }

                    if (localAscending != null) {
                        localAscending[child] = true;
                    }
                    localDescending[child] = false;
                }
            }

            if (plan.bucketFlags[b] == Apex.BUCKET_DESCENDING) {
                plan.bucketDescending[b] = false;
                plan.bucketAscending[b] = true;
                plan.bucketFlags[b] = Apex.BUCKET_ASCENDING;
            }
        }
    }
}
