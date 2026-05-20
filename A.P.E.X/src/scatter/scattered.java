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

                if (!hasLocalMsd) {
                while (p <= unrolledEnd) {
                    long k0 = src.get(Apex.LONG, p);      long v0 = src.get(Apex.LONG, p + 8);
                    long k1 = src.get(Apex.LONG, p + 16);     long v1 = src.get(Apex.LONG, p + 24);
                    long k2 = src.get(Apex.LONG, p + 32);     long v2 = src.get(Apex.LONG, p + 40);
                    long k3 = src.get(Apex.LONG, p + 48);     long v3 = src.get(Apex.LONG, p + 56);

                    int b0 = (int) ((k0 >>> msdShift) & bucketMask);
                    int b1 = (int) ((k1 >>> msdShift) & bucketMask);
                    int b2 = (int) ((k2 >>> msdShift) & bucketMask);
                    int b3 = (int) ((k3 >>> msdShift) & bucketMask);

                    long q0 = (bucketStarts[b0] + out[b0]) << 4;
                    out[b0]++;
                    dst.set(Apex.LONG, q0, k0);
                    dst.set(Apex.LONG, q0 + 8, v0);

                    long q1 = (bucketStarts[b1] + out[b1]) << 4;
                    out[b1]++;
                    dst.set(Apex.LONG, q1, k1);
                    dst.set(Apex.LONG, q1 + 8, v1);

                    long q2 = (bucketStarts[b2] + out[b2]) << 4;
                    out[b2]++;
                    dst.set(Apex.LONG, q2, k2);
                    dst.set(Apex.LONG, q2 + 8, v2);

                    long q3 = (bucketStarts[b3] + out[b3]) << 4;
                    out[b3]++;
                    dst.set(Apex.LONG, q3, k3);
                    dst.set(Apex.LONG, q3 + 8, v3);

                    p += 4L * Apex.RECORD_BYTES;
                }

                while (p < end) {
                    long k = src.get(Apex.LONG, p);
                    long v = src.get(Apex.LONG, p + 8);

                    int b = (int) ((k >>> msdShift) & bucketMask);
                    long q = (bucketStarts[b] + out[b]) << 4;
                    out[b]++;

                    dst.set(Apex.LONG, q, k);
                    dst.set(Apex.LONG, q + 8, v);

                    p += Apex.RECORD_BYTES;
                }
                    return;
                }

                while (p <= unrolledEnd) {
                    long k0 = src.get(Apex.LONG, p);      long v0 = src.get(Apex.LONG, p + 8);
                    long k1 = src.get(Apex.LONG, p + 16);     long v1 = src.get(Apex.LONG, p + 24);
                    long k2 = src.get(Apex.LONG, p + 32);     long v2 = src.get(Apex.LONG, p + 40);
                    long k3 = src.get(Apex.LONG, p + 48);     long v3 = src.get(Apex.LONG, p + 56);

                    int b0 = (int) ((k0 >>> msdShift) & bucketMask);
                    int b1 = (int) ((k1 >>> msdShift) & bucketMask);
                    int b2 = (int) ((k2 >>> msdShift) & bucketMask);
                    int b3 = (int) ((k3 >>> msdShift) & bucketMask);

                    int localShift0 = localMsdShifts[b0];
                    long q0;
                    if (localShift0 >= 0) {
                        int child = (int) ((k0 >>> localShift0) & bucketMask);
                        int[] childOut = localThreadOffsets[b0][tid];
                        q0 = (localStarts[b0][child] + childOut[child]) << 4;
                        childOut[child]++;
                    } else {
                        q0 = (bucketStarts[b0] + out[b0]) << 4;
                        out[b0]++;
                    }
                    dst.set(Apex.LONG, q0, k0);
                    dst.set(Apex.LONG, q0 + 8, v0);

                    int localShift1 = localMsdShifts[b1];
                    long q1;
                    if (localShift1 >= 0) {
                        int child = (int) ((k1 >>> localShift1) & bucketMask);
                        int[] childOut = localThreadOffsets[b1][tid];
                        q1 = (localStarts[b1][child] + childOut[child]) << 4;
                        childOut[child]++;
                    } else {
                        q1 = (bucketStarts[b1] + out[b1]) << 4;
                        out[b1]++;
                    }
                    dst.set(Apex.LONG, q1, k1);
                    dst.set(Apex.LONG, q1 + 8, v1);

                    int localShift2 = localMsdShifts[b2];
                    long q2;
                    if (localShift2 >= 0) {
                        int child = (int) ((k2 >>> localShift2) & bucketMask);
                        int[] childOut = localThreadOffsets[b2][tid];
                        q2 = (localStarts[b2][child] + childOut[child]) << 4;
                        childOut[child]++;
                    } else {
                        q2 = (bucketStarts[b2] + out[b2]) << 4;
                        out[b2]++;
                    }
                    dst.set(Apex.LONG, q2, k2);
                    dst.set(Apex.LONG, q2 + 8, v2);

                    int localShift3 = localMsdShifts[b3];
                    long q3;
                    if (localShift3 >= 0) {
                        int child = (int) ((k3 >>> localShift3) & bucketMask);
                        int[] childOut = localThreadOffsets[b3][tid];
                        q3 = (localStarts[b3][child] + childOut[child]) << 4;
                        childOut[child]++;
                    } else {
                        q3 = (bucketStarts[b3] + out[b3]) << 4;
                        out[b3]++;
                    }
                    dst.set(Apex.LONG, q3, k3);
                    dst.set(Apex.LONG, q3 + 8, v3);

                    p += 4L * Apex.RECORD_BYTES;
                }

                while (p < end) {
                    long k = src.get(Apex.LONG, p);
                    long v = src.get(Apex.LONG, p + 8);

                    int b = (int) ((k >>> msdShift) & bucketMask);
                    int localShift = localMsdShifts[b];
                    long q;

                    if (localShift >= 0) {
                        int child = (int) ((k >>> localShift) & bucketMask);
                        int[] childOut = localThreadOffsets[b][tid];
                        q = (localStarts[b][child] + childOut[child]) << 4;
                        childOut[child]++;
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

   


