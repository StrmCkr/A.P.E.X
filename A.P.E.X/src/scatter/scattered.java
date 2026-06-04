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

                long s = tid * chunk;
                long e = (tid == Apex.THREADS - 1) ? n : s + chunk;

                long p = s << 4;
                long end = e << 4;
                long unrolledEnd = end - (4L * Apex.RECORD_BYTES);

                // --- 🛡️ 64-Byte Software Write-Combining Buffers ---
                // Private thread-local matrices capture elements before hitting the memory bus
                int numBuckets = cfg.msdBucketCount;
                long[] bufferK = new long[numBuckets << 2];
                long[] bufferV = new long[numBuckets << 2];
                int[] bufferCounts = new int[numBuckets];

                // --- 🚀 Stride Phase: Global MSD Out-of-Place Shuffling Track ---
                if (!hasLocalMsd) {
                    if (hasDescendingScatter) {
                        while (p < end) {
                            long k = src.get(Apex.LONG, p);
                            long v = src.get(Apex.LONG, p + 8);
                            int b = (int) ((k >>> msdShift) & bucketMask);
                            long q;

                            if (bucketFlags[b] == Apex.BUCKET_DESCENDING) {
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
                        return;
                    }

                    while (p <= unrolledEnd) {
                        long k0 = src.get(Apex.LONG, p);          long v0 = src.get(Apex.LONG, p + 8);
                        long k1 = src.get(Apex.LONG, p + 16);     long v1 = src.get(Apex.LONG, p + 24);
                        long k2 = src.get(Apex.LONG, p + 32);     long v2 = src.get(Apex.LONG, p + 40);
                        long k3 = src.get(Apex.LONG, p + 48);     long v3 = src.get(Apex.LONG, p + 56);

                        int b0 = (int) ((k0 >>> msdShift) & bucketMask);
                        int b1 = (int) ((k1 >>> msdShift) & bucketMask);
                        int b2 = (int) ((k2 >>> msdShift) & bucketMask);
                        int b3 = (int) ((k3 >>> msdShift) & bucketMask);

                        // Stage Record 0 inside the local buffer
                        int idx0 = (b0 << 2) + bufferCounts[b0];
                        bufferK[idx0] = k0; bufferV[idx0] = v0;
                        if (++bufferCounts[b0] == 4) {
                            long q = (bucketStarts[b0] + out[b0]) << 4; out[b0] += 4;
                            int slot = b0 << 2;
                            dst.set(Apex.LONG, q,      bufferK[slot]);     dst.set(Apex.LONG, q + 8,   bufferV[slot]);
                            dst.set(Apex.LONG, q + 16, bufferK[slot + 1]); dst.set(Apex.LONG, q + 24,  bufferV[slot + 1]);
                            dst.set(Apex.LONG, q + 32, bufferK[slot + 2]); dst.set(Apex.LONG, q + 40,  bufferV[slot + 2]);
                            dst.set(Apex.LONG, q + 48, bufferK[slot + 3]); dst.set(Apex.LONG, q + 56,  bufferV[slot + 3]);
                            bufferCounts[b0] = 0;
                        }

                        // Stage Record 1 inside the local buffer
                        int idx1 = (b1 << 2) + bufferCounts[b1];
                        bufferK[idx1] = k1; bufferV[idx1] = v1;
                        if (++bufferCounts[b1] == 4) {
                            long q = (bucketStarts[b1] + out[b1]) << 4; out[b1] += 4;
                            int slot = b1 << 2;
                            dst.set(Apex.LONG, q,      bufferK[slot]);     dst.set(Apex.LONG, q + 8,   bufferV[slot]);
                            dst.set(Apex.LONG, q + 16, bufferK[slot + 1]); dst.set(Apex.LONG, q + 24,  bufferV[slot + 1]);
                            dst.set(Apex.LONG, q + 32, bufferK[slot + 2]); dst.set(Apex.LONG, q + 40,  bufferV[slot + 2]);
                            dst.set(Apex.LONG, q + 48, bufferK[slot + 3]); dst.set(Apex.LONG, q + 56,  bufferV[slot + 3]);
                            bufferCounts[b1] = 0;
                        }

                        // Stage Record 2 inside the local buffer
                        int idx2 = (b2 << 2) + bufferCounts[b2];
                        bufferK[idx2] = k2; bufferV[idx2] = v2;
                        if (++bufferCounts[b2] == 4) {
                            long q = (bucketStarts[b2] + out[b2]) << 4; out[b2] += 4;
                            int slot = b2 << 2;
                            dst.set(Apex.LONG, q,      bufferK[slot]);     dst.set(Apex.LONG, q + 8,   bufferV[slot]);
                            dst.set(Apex.LONG, q + 16, bufferK[slot + 1]); dst.set(Apex.LONG, q + 24,  bufferV[slot + 1]);
                            dst.set(Apex.LONG, q + 32, bufferK[slot + 2]); dst.set(Apex.LONG, q + 40,  bufferV[slot + 2]);
                            dst.set(Apex.LONG, q + 48, bufferK[slot + 3]); dst.set(Apex.LONG, q + 56,  bufferV[slot + 3]);
                            bufferCounts[b2] = 0;
                        }

                        // Stage Record 3 inside the local buffer
                        int idx3 = (b3 << 2) + bufferCounts[b3];
                        bufferK[idx3] = k3; bufferV[idx3] = v3;
                        if (++bufferCounts[b3] == 4) {
                            long q = (bucketStarts[b3] + out[b3]) << 4; out[b3] += 4;
                            int slot = b3 << 2;
                            dst.set(Apex.LONG, q,      bufferK[slot]);     dst.set(Apex.LONG, q + 8,   bufferV[slot]);
                            dst.set(Apex.LONG, q + 16, bufferK[slot + 1]); dst.set(Apex.LONG, q + 24,  bufferV[slot + 1]);
                            dst.set(Apex.LONG, q + 32, bufferK[slot + 2]); dst.set(Apex.LONG, q + 40,  bufferV[slot + 2]);
                            dst.set(Apex.LONG, q + 48, bufferK[slot + 3]); dst.set(Apex.LONG, q + 56,  bufferV[slot + 3]);
                            bufferCounts[b3] = 0;
                        }

                        p += 4L * Apex.RECORD_BYTES;
                    }

                    // Process trailing cleanup records using the same buffer staging layers
                    while (p < end) {
                        long k = src.get(Apex.LONG, p); long v = src.get(Apex.LONG, p + 8);
                        int b = (int) ((k >>> msdShift) & bucketMask);
                        int idx = (b << 2) + bufferCounts[b];
                        bufferK[idx] = k; bufferV[idx] = v;
                        if (++bufferCounts[b] == 4) {
                            long q = (bucketStarts[b] + out[b]) << 4; out[b] += 4;
                            int slot = b << 2;
                            dst.set(Apex.LONG, q,      bufferK[slot]);     dst.set(Apex.LONG, q + 8,   bufferV[slot]);
                            dst.set(Apex.LONG, q + 16, bufferK[slot + 1]); dst.set(Apex.LONG, q + 24,  bufferV[slot + 1]);
                            dst.set(Apex.LONG, q + 32, bufferK[slot + 2]); dst.set(Apex.LONG, q + 40,  bufferV[slot + 2]);
                            dst.set(Apex.LONG, q + 48, bufferK[slot + 3]); dst.set(Apex.LONG, q + 56,  bufferV[slot + 3]);
                            bufferCounts[b] = 0;
                        }
                        p += Apex.RECORD_BYTES;
                    }

                    // --- 🧹 FINAL FLUSH PHASE: Flush any remaining partially filled buffers out to dst ---
                    for (int b = 0; b < numBuckets; b++) {
                        int remaining = bufferCounts[b];
                        int slot = b << 2;
                        for (int i = 0; i < remaining; i++) {
                            long q = (bucketStarts[b] + out[b]) << 4; out[b]++;
                            dst.set(Apex.LONG, q, bufferK[slot + i]);
                            dst.set(Apex.LONG, q + 8, bufferV[slot + i]);
                        }
                    }
                    return;
                }

                // --- 🚀 Slower Local MSD Hierarchical Child Track (Fallback Scalar Channels) ---
                while (p < end) {
                    long k = src.get(Apex.LONG, p); long v = src.get(Apex.LONG, p + 8);
                    int b = (int) ((k >>> msdShift) & bucketMask);
                    int localShift = localMsdShifts[b];
                    long q;
                    if (localShift >= 0) {
                        int child = (int) ((k >>> localShift) & (localSizes[b].length - 1));
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
                    dst.set(Apex.LONG, q, k); dst.set(Apex.LONG, q + 8, v);
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
  
   
