package scatter;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Future;

import MSD.msdbucketplan.MsdBucketPlan;
import Tools.tools;
import config.configurations.Config;
import main.Apex;

public class scattered {
    static final class InPlaceLayout {
        final long[] starts;
        final long[] ends;
        final long[] next;
        final int[] parentPartitions;
        final int[][] localChildPartitions;
        final int partitionCount;

        InPlaceLayout(
                long[] starts,
                long[] ends,
                int[] parentPartitions,
                int[][] localChildPartitions,
                int partitionCount
        ) {
            this.starts = starts;
            this.ends = ends;
            this.next = Arrays.copyOf(starts, starts.length);
            this.parentPartitions = parentPartitions;
            this.localChildPartitions = localChildPartitions;
            this.partitionCount = partitionCount;
        }
    }

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
        int msdShift = plan.msdShift;
        boolean hasLocalMsd = plan.hasLocalMsd;
        int[] localMsdShifts = plan.localMsdShifts;
        long[][] localStarts = plan.localStarts;
        int[][][] localThreadOffsets = plan.localThreadScatterOffsets;

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
                long[][] bufferK = new long[numBuckets][4];
                long[][] bufferV = new long[numBuckets][4];
                int[] bufferCounts = new int[numBuckets];

                // --- 🚀 Stride Phase: Global MSD Out-of-Place Shuffling Track ---
                if (!hasLocalMsd) {
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
                        int idx0 = bufferCounts[b0];
                        bufferK[b0][idx0] = k0; bufferV[b0][idx0] = v0;
                        if (++bufferCounts[b0] == 4) {
                            long q = (bucketStarts[b0] + out[b0]) << 4; out[b0] += 4;
                            dst.set(Apex.LONG, q,      bufferK[b0][0]); dst.set(Apex.LONG, q + 8,   bufferV[b0][0]);
                            dst.set(Apex.LONG, q + 16, bufferK[b0][1]); dst.set(Apex.LONG, q + 24,  bufferV[b0][1]);
                            dst.set(Apex.LONG, q + 32, bufferK[b0][2]); dst.set(Apex.LONG, q + 40,  bufferV[b0][2]);
                            dst.set(Apex.LONG, q + 48, bufferK[b0][3]); dst.set(Apex.LONG, q + 56,  bufferV[b0][3]);
                            bufferCounts[b0] = 0;
                        }

                        // Stage Record 1 inside the local buffer
                        int idx1 = bufferCounts[b1];
                        bufferK[b1][idx1] = k1; bufferV[b1][idx1] = v1;
                        if (++bufferCounts[b1] == 4) {
                            long q = (bucketStarts[b1] + out[b1]) << 4; out[b1] += 4;
                            dst.set(Apex.LONG, q,      bufferK[b1][0]); dst.set(Apex.LONG, q + 8,   bufferV[b1][0]);
                            dst.set(Apex.LONG, q + 16, bufferK[b1][1]); dst.set(Apex.LONG, q + 24,  bufferV[b1][1]);
                            dst.set(Apex.LONG, q + 32, bufferK[b1][2]); dst.set(Apex.LONG, q + 40,  bufferV[b1][2]);
                            dst.set(Apex.LONG, q + 48, bufferK[b1][3]); dst.set(Apex.LONG, q + 56,  bufferV[b1][3]);
                            bufferCounts[b1] = 0;
                        }

                        // Stage Record 2 inside the local buffer
                        int idx2 = bufferCounts[b2];
                        bufferK[b2][idx2] = k2; bufferV[b2][idx2] = v2;
                        if (++bufferCounts[b2] == 4) {
                            long q = (bucketStarts[b2] + out[b2]) << 4; out[b2] += 4;
                            dst.set(Apex.LONG, q,      bufferK[b2][0]); dst.set(Apex.LONG, q + 8,   bufferV[b2][0]);
                            dst.set(Apex.LONG, q + 16, bufferK[b2][1]); dst.set(Apex.LONG, q + 24,  bufferV[b2][1]);
                            dst.set(Apex.LONG, q + 32, bufferK[b2][2]); dst.set(Apex.LONG, q + 40,  bufferV[b2][2]);
                            dst.set(Apex.LONG, q + 48, bufferK[b2][3]); dst.set(Apex.LONG, q + 56,  bufferV[b2][3]);
                            bufferCounts[b2] = 0;
                        }

                        // Stage Record 3 inside the local buffer
                        int idx3 = bufferCounts[b3];
                        bufferK[b3][idx3] = k3; bufferV[b3][idx3] = v3;
                        if (++bufferCounts[b3] == 4) {
                            long q = (bucketStarts[b3] + out[b3]) << 4; out[b3] += 4;
                            dst.set(Apex.LONG, q,      bufferK[b3][0]); dst.set(Apex.LONG, q + 8,   bufferV[b3][0]);
                            dst.set(Apex.LONG, q + 16, bufferK[b3][1]); dst.set(Apex.LONG, q + 24,  bufferV[b3][1]);
                            dst.set(Apex.LONG, q + 32, bufferK[b3][2]); dst.set(Apex.LONG, q + 40,  bufferV[b3][2]);
                            dst.set(Apex.LONG, q + 48, bufferK[b3][3]); dst.set(Apex.LONG, q + 56,  bufferV[b3][3]);
                            bufferCounts[b3] = 0;
                        }

                        p += 4L * Apex.RECORD_BYTES;
                    }

                    // Process trailing cleanup records using the same buffer staging layers
                    while (p < end) {
                        long k = src.get(Apex.LONG, p); long v = src.get(Apex.LONG, p + 8);
                        int b = (int) ((k >>> msdShift) & bucketMask);
                        int idx = bufferCounts[b];
                        bufferK[b][idx] = k; bufferV[b][idx] = v;
                        if (++bufferCounts[b] == 4) {
                            long q = (bucketStarts[b] + out[b]) << 4; out[b] += 4;
                            dst.set(Apex.LONG, q,      bufferK[b][0]); dst.set(Apex.LONG, q + 8,   bufferV[b][0]);
                            dst.set(Apex.LONG, q + 16, bufferK[b][1]); dst.set(Apex.LONG, q + 24,  bufferV[b][1]);
                            dst.set(Apex.LONG, q + 32, bufferK[b][2]); dst.set(Apex.LONG, q + 40,  bufferV[b][2]);
                            dst.set(Apex.LONG, q + 48, bufferK[b][3]); dst.set(Apex.LONG, q + 56,  bufferV[b][3]);
                            bufferCounts[b] = 0;
                        }
                        p += Apex.RECORD_BYTES;
                    }

                    // --- 🧹 FINAL FLUSH PHASE: Flush any remaining partially filled buffers out to dst ---
                    for (int b = 0; b < numBuckets; b++) {
                        int remaining = bufferCounts[b];
                        for (int i = 0; i < remaining; i++) {
                            long q = (bucketStarts[b] + out[b]) << 4; out[b]++;
                            dst.set(Apex.LONG, q, bufferK[b][i]);
                            dst.set(Apex.LONG, q + 8, bufferV[b][i]);
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
                        int child = (int) ((k >>> localShift) & bucketMask);
                        int[] childOut = localThreadOffsets[b][tid];
                        q = (localStarts[b][child] + childOut[child]) << 4; childOut[child]++;
                    } else {
                        q = (bucketStarts[b] + out[b]) << 4; out[b]++;
                    }
                    dst.set(Apex.LONG, q, k); dst.set(Apex.LONG, q + 8, v);
                    p += Apex.RECORD_BYTES;
                }
            }));
        }

        tools.waitForFutures(futures);
    }


    static void settleRecord(
            MemorySegment data,
            long currentIndex,
            long key,
            long value,
            int target,
            int currentPartition,
            MsdBucketPlan plan,
            Config cfg,
            InPlaceLayout layout
    ) {
        while (target != currentPartition) {
            if (target < 0 || target >= layout.partitionCount) {
                throw new IllegalStateException("Invalid in-place scatter target partition: " + target);
            }

            long targetIndex = layout.next[target]++;
            if (targetIndex >= layout.ends[target]) {
                throw new IllegalStateException("In-place scatter target overflow: " + target);
            }

            long q = targetIndex << 4;
            long displacedKey = data.get(Apex.LONG, q);
            long displacedValue = data.get(Apex.LONG, q + 8);

            data.set(Apex.LONG, q, key);
            data.set(Apex.LONG, q + 8, value);

            key = displacedKey;
            value = displacedValue;
            target = targetPartition(key, plan, cfg, layout);
        }

        long p = currentIndex << 4;
        data.set(Apex.LONG, p, key);
        data.set(Apex.LONG, p + 8, value);
    }

    static int targetPartition(
            long key,
            MsdBucketPlan plan,
            Config cfg,
            InPlaceLayout layout
    ) {
        int bucketMask = cfg.msdBucketCount - 1;
        int parent = (int) ((key >>> plan.msdShift) & bucketMask);
        int[] childPartitions = layout.localChildPartitions[parent];

        if (childPartitions != null) {
            int child = (int) ((key >>> plan.localMsdShifts[parent]) & bucketMask);
            return childPartitions[child];
        }

        return layout.parentPartitions[parent];
    }

    /**
     * 🛡️ Master Class Orchestration Interface.
     * Drop-in gateway connecting directly to your updated out-of-place execution pipelines.
     */
    public static void runStandardMsdScatter(
            MemorySegment src,
            MemorySegment dst,
            MsdBucketPlan plan,
            Config cfg
    ) throws Exception {
        scatterIntoMsdBuckets(src, dst, plan.totalRecords, plan, cfg);
    }}

   


