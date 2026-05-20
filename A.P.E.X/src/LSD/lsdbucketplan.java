package LSD;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import MSD.msdbucketplan;
import MSD.msdbucketplan.MsdBucketPlan;
import Tools.tools;
import Tuples.tuples;
import config.configurations.Config;
import main.Apex;
import main.Apex.Scratch;
import tinysorts.tinysort;

public class lsdbucketplan {
	  static final class PartitionWork {
	        final int bucket;
	        final long startPos;
	        final int size;
	        final long variableMask;
	        final int remainingBits;
	        final boolean tinySort;

	        PartitionWork(int bucket, long startPos, int size, long variableMask, int remainingBits, boolean tinySort) {
	            this.bucket = bucket;
	            this.startPos = startPos;
	            this.size = size;
	            this.variableMask = variableMask;
	            this.remainingBits = remainingBits;
	            this.tinySort = tinySort;
	        }

	        static PartitionWork bucket(int bucket, MsdBucketPlan plan) {
	            return new PartitionWork(bucket, plan.starts[bucket], plan.sizes[bucket], 0L, 0, false);
	        }

	        static PartitionWork tiny(long startPos, int size) {
	            return new PartitionWork(-1, startPos, size, 0L, 0, true);
	        }

	        static PartitionWork partition(long startPos, int size, long variableMask, int remainingBits) {
	            return new PartitionWork(-1, startPos, size, variableMask, remainingBits, false);
	        }
	    }

	  public static int buildLsdCyclePlan(
	            long variableMask,
	            Config cfg,
	            int remainingBits,
	            int[] cycleShifts,
	            int[] cycleMasks,
	            long[] cycleBitMasks,
	            long[] cycleTuplePlans
	    ) {
	        variableMask &= tools.lowBitsMask(remainingBits);

	        int contiguousCycles = buildContiguousLsdCyclePlan(variableMask, cfg, remainingBits,
	                cycleShifts, cycleMasks, cycleBitMasks, cycleTuplePlans);
	        int packedCycles = packedTupleCycleCount(variableMask, cfg);

	        if (packedCycles < contiguousCycles || Apex.PACKED_TUPLE_CYCLES) {
	            return tuples.buildPackedTupleLsdCyclePlan(variableMask, cfg,
	                    cycleShifts, cycleMasks, cycleBitMasks, cycleTuplePlans);
	        }

	        return contiguousCycles;
	    }

	  public  static int buildContiguousLsdCyclePlan(
	            long variableMask,
	            Config cfg,
	            int remainingBits,
	            int[] cycleShifts,
	            int[] cycleMasks,
	            long[] cycleBitMasks,
	            long[] cycleTuplePlans
	    ) {
	        int cycles = 0;
	        int bit = 0;

	        while (bit < remainingBits) {
	            while (bit < remainingBits && ((variableMask >>> bit) & 1L) == 0L) {
	                bit++;
	            }

	            int runStart = bit;

	            while (bit < remainingBits && ((variableMask >>> bit) & 1L) != 0L) {
	                bit++;
	            }

	            int runEnd = bit;

	            for (int shift = runStart; shift < runEnd; shift += cfg.lsdBits) {
	                int bitsThisCycle = Math.min(cfg.lsdBits, runEnd - shift);
	                long bitMask = tools.lowBitsMask(bitsThisCycle) << shift;

	                cycleShifts[cycles] = shift;
	                cycleMasks[cycles] = tools.lowIntMask(bitsThisCycle);
	                cycleBitMasks[cycles] = bitMask;
	                cycleTuplePlans[cycles] = 0L;

	                cycles++;
	            }
	        }

	        return cycles;
	    }

	  static int packedTupleCycleCount(long variableMask, Config cfg) {
	        int variableBits = Long.bitCount(variableMask);
	        return variableBits == 0 ? 0 : (variableBits + cfg.lsdBits - 1) / cfg.lsdBits;
	    }
	  public static void sortMsdBucketsWithLsdRadix(
	            MemorySegment scratch,
	            MemorySegment dst,
	            MsdBucketPlan plan,
	            Config cfg
	    ) throws Exception {
	        ArrayList<Future<?>> futures = new ArrayList<>(Apex.THREADS);
	        ThreadLocal<Scratch> tls = ThreadLocal.withInitial(() ->
	                new Scratch(Math.max(cfg.lsdRadix, tuples.directTupleRadixCap())));
	        PartitionWork[] localWorkItems = buildLocalMsdWorkItems(plan, cfg);
	        final int workStealBatch = Math.max(1, Apex.WORK_STEAL_BATCH);

	        if (localWorkItems != null) {
	            if (localWorkItems.length == 0) {
	                return;
	            }

	            AtomicInteger nextWork = new AtomicInteger();

	            for (int t = 0; t < Apex.THREADS; t++) {
	                futures.add(Apex.POOL.submit(() -> {
	                    try {
	                        Scratch sc = tls.get();

	                        for (;;) {
	                            int startWork = nextWork.getAndAdd(workStealBatch);
	                            if (startWork >= localWorkItems.length) {
	                                break;
	                            }

	                            int endWork = Math.min(startWork + workStealBatch, localWorkItems.length);
	                            for (int workIndex = startWork; workIndex < endWork; workIndex++) {
	                                sortPartitionWork(scratch, dst, plan, cfg, sc, localWorkItems[workIndex]);
	                            }
	                        }
	                    } finally {
	                        tls.remove();
	                    }
	                }));
	            }

	            tools.waitForFutures(futures);
	            return;
	        }

	        if (Apex.LSD_WORK_STEALING) {
	            int[] workBuckets = msdbucketplan.buildLsdWorkBucketsByDescendingSize(plan, cfg);
	            if (workBuckets.length == 0) {
	                return;
	            }

	            AtomicInteger nextBucket = new AtomicInteger();

	            for (int t = 0; t < Apex.THREADS; t++) {
	                futures.add(Apex.POOL.submit(() -> {
	                    try {
	                        Scratch sc = tls.get();

	                        for (;;) {
	                            int startWork = nextBucket.getAndAdd(workStealBatch);
	                            if (startWork >= workBuckets.length) {
	                                break;
	                            }

	                            int endWork = Math.min(startWork + workStealBatch, workBuckets.length);
	                            for (int workIndex = startWork; workIndex < endWork; workIndex++) {
	                                sortOneMsdBucketWithLsdRadix(scratch, dst, plan, cfg, sc, workBuckets[workIndex]);
	                            }
	                        }
	                    } finally {
	                        tls.remove();
	                    }
	                }));
	            }
	        } else {
	            for (int t = 0; t < Apex.THREADS; t++) {
	                final int tid = t;

	                futures.add(Apex.POOL.submit(() -> {
	                    try {
	                        Scratch sc = tls.get();

	                        for (int b = tid; b < cfg.msdBucketCount; b += Apex.THREADS) {
	                            sortOneMsdBucketWithLsdRadix(scratch, dst, plan, cfg, sc, b);
	                        }
	                    } finally {
	                        tls.remove();
	                    }
	                }));
	            }
	        }

	        tools.waitForFutures(futures);
	    }

	    static void sortOneMsdBucketWithLsdRadix(
	            MemorySegment scratch,
	            MemorySegment dst,
	            MsdBucketPlan plan,
	            Config cfg,
	            Scratch sc,
	            int b
	    ) {
	        int size = plan.sizes[b];
	        int cycles = plan.cycleCounts[b];
	        long tupleTailMask = plan.tupleTailMasks[b];
	        long tupleTailPlan = plan.tupleTailPlans[b];

	        if (!bucketHasLsdWork(plan, cfg, b)) {
	            return;
	        }

	        long startPos = plan.starts[b];

	        if (size < cfg.tinyPartitionThreshold) {
	            tinysort.tinyPartitionBitSort(dst, startPos, size, sc);
	            return;
	        }

	        if (cycles == 0 && tuples.tryDirectTupleSpaceSort(
	                scratch, dst, startPos, size, sc, tupleTailMask, tupleTailPlan
	        )) {
	            return;
	        }

	        if (size <= Apex.MAX_HEAP_SCRATCH_RECORDS) {
	            lsdRadixSortPartition(
	                    dst,
	                    startPos,
	                    size,
	                    sc,
	                    cfg,
	                    cycles,
	                    plan.cycleShifts[b],
	                    plan.cycleMasks[b],
	                    plan.cycleBitMasks[b],
	                    plan.cycleTuplePlans[b],
	                    tupleTailMask,
	                    tupleTailPlan
	            );
	        } else {
	            lsdRadixSortPartitionOffHeap(
	                    scratch,
	                    dst,
	                    startPos,
	                    size,
	                    sc,
	                    cfg,
	                    cycles,
	                    plan.cycleShifts[b],
	                    plan.cycleMasks[b],
	                    plan.cycleBitMasks[b],
	                    plan.cycleTuplePlans[b],
	                    tupleTailMask,
	                    tupleTailPlan
	            );
	        }
	    }

	 
	  

	    static void lsdRadixSortPartition(
	            MemorySegment dst,
	            long startPos,
	            int size,
	            Scratch sc,
	            Config cfg,
	            int cycles,
	            int[] cycleShifts,
	            int[] cycleMasks,
	            long[] cycleBitMasks,
	            long[] cycleTuplePlans,
	            long tupleTailMask,
	            long tupleTailPlan
	    ) {
	        if (size <= 1 || (cycles == 0 && tupleTailMask == 0L)) {
	            return;
	        }

	        sc.ensure(size);

	        long base = startPos << 4;

	        long[] currentKeys = sc.k1;
	        long[] currentValues = sc.v1;

	        long[] nextKeys = sc.k2;
	        long[] nextValues = sc.v2;

	        long p = base;
	        boolean ascending = true;
	        boolean descending = true;
	        long previous = 0L;

	        for (int i = 0; i < size; i++) {
	            long key = dst.get(Apex.LONG, p);
	            currentKeys[i] = key;
	            currentValues[i] = dst.get(Apex.LONG, p + 8);
	            if ((ascending || descending) && i > 0) {
	                int cmp = Long.compareUnsigned(previous, key);
	                ascending &= cmp <= 0;
	                descending &= cmp >= 0;
	            }
	            previous = key;
	            p += Apex.RECORD_BYTES;
	        }

	        if (ascending) {
	            return;
	        }

	        if (descending) {
	            tools.reverseRecordsInPlace(dst, base, size);
	            return;
	        }

	        for (int cycle = 0; cycle < cycles; cycle++) {
	            int shift = cycleShifts[cycle];
	            int mask = cycleMasks[cycle];
	            long bitMask = cycleBitMasks[cycle];
	            long smallTuplePlan = cycleTuplePlans[cycle];

	            int radixThisPass = mask + 1;

	            sc.ensureCounts(radixThisPass);
	            Arrays.fill(sc.counts, 0, radixThisPass, 0);

	            for (int i = 0; i < size; i++) {
	                int bin = tools.digit(currentKeys[i], shift, mask, bitMask, smallTuplePlan);
	                sc.counts[bin]++;
	            }

	            int sum = 0;

	            for (int i = 0; i < radixThisPass; i++) {
	                int c = sc.counts[i];
	                sc.counts[i] = sum;
	                sum += c;
	            }

	            for (int i = 0; i < size; i++) {
	                long k = currentKeys[i];
	                int bin = tools.digit(k, shift, mask, bitMask, smallTuplePlan);

	                int pos = sc.counts[bin]++;

	                nextKeys[pos] = k;
	                nextValues[pos] = currentValues[i];
	            }

	            long[] tk = currentKeys;
	            currentKeys = nextKeys;
	            nextKeys = tk;

	            long[] tv = currentValues;
	            currentValues = nextValues;
	            nextValues = tv;
	        }

	        if (tupleTailMask != 0L) {
	            tuples.tupleCountingPass(
	                    currentKeys,
	                    currentValues,
	                    nextKeys,
	                    nextValues,
	                    size,
	                    sc,
	                    tupleTailMask,
	                    tupleTailPlan
	            );

	            long[] tk = currentKeys;
	            currentKeys = nextKeys;
	            nextKeys = tk;

	            long[] tv = currentValues;
	            currentValues = nextValues;
	            nextValues = tv;
	        }

	        p = base;

	        for (int i = 0; i < size; i++) {
	            dst.set(Apex.LONG, p, currentKeys[i]);
	            dst.set(Apex.LONG, p + 8, currentValues[i]);
	            p += Apex.RECORD_BYTES;
	        }
	    }

	    static void lsdRadixSortPartitionOffHeap(
	            MemorySegment scratch,
	            MemorySegment dst,
	            long startPos,
	            int size,
	            Scratch sc,
	            Config cfg,
	            int cycles,
	            int[] cycleShifts,
	            int[] cycleMasks,
	            long[] cycleBitMasks,
	            long[] cycleTuplePlans,
	            long tupleTailMask,
	            long tupleTailPlan
	    ) {
	        if (size <= 1 || (cycles == 0 && tupleTailMask == 0L)) {
	            return;
	        }

	        try {
	            Apex.LARGE_PARTITION_PERMITS.acquire();
	        } catch (InterruptedException ex) {
	            Thread.currentThread().interrupt();
	            throw new RuntimeException(ex);
	        }

	        try {
	            sc.ensureCounts(Math.max(cfg.lsdRadix, tuples.directTupleRadixCap()));
	            int[] counts = sc.counts;

	            long dstBase = startPos << 4;
	            long scratchBase = dstBase;
	            boolean currentInDst = true;

	            for (int cycle = 0; cycle < cycles; cycle++) {
	                int shift;
	                int mask;
	                long bitMask;
	                long smallTuplePlan;

	                shift = cycleShifts[cycle];
	                mask = cycleMasks[cycle];
	                bitMask = cycleBitMasks[cycle];
	                smallTuplePlan = cycleTuplePlans[cycle];

	                int radixThisPass = mask + 1;
	                sc.ensureCounts(radixThisPass);
	                counts = sc.counts;

	                MemorySegment source = currentInDst ? dst : scratch;
	                long sourceBase = currentInDst ? dstBase : scratchBase;

	                MemorySegment target = currentInDst ? scratch : dst;
	                long targetBase = currentInDst ? scratchBase : dstBase;

	                tuples.tupleCountingPassSegments(
	                        source,
	                        sourceBase,
	                        target,
	                        targetBase,
	                        size,
	                        counts,
	                        shift,
	                        mask,
	                        bitMask,
	                        smallTuplePlan
	                );

	                currentInDst = !currentInDst;
	            }

	            if (tupleTailMask != 0L) {
	                int mask = tuples.tupleRadix(tupleTailMask) - 1;
	                int radixThisPass = mask + 1;
	                sc.ensureCounts(radixThisPass);
	                counts = sc.counts;

	                MemorySegment source = currentInDst ? dst : scratch;
	                long sourceBase = currentInDst ? dstBase : scratchBase;

	                MemorySegment target = currentInDst ? scratch : dst;
	                long targetBase = currentInDst ? scratchBase : dstBase;

	                tuples.tupleCountingPassSegments(
	                        source,
	                        sourceBase,
	                        target,
	                        targetBase,
	                        size,
	                        counts,
	                        -1,
	                        mask,
	                        tupleTailMask,
	                        tupleTailPlan
	                );

	                currentInDst = !currentInDst;
	            }

	            if (!currentInDst) {
	                try {
	                    tools.parallelBulkCopy(scratch, scratchBase, dst, dstBase, size);
	                } catch (Exception e) {
	                    throw new RuntimeException("Parallel off-heap blit failed", e);
	                }
	            }
	        } finally {
	            Apex.LARGE_PARTITION_PERMITS.release();
	        }
	    }
	    public static boolean bucketHasLsdWork(MsdBucketPlan plan, Config cfg, int b) {
	        return plan.bucketFlags[b] == Apex.BUCKET_MIXED &&
	                plan.sizes[b] > 1 &&
	                plan.variableMasks[b] != 0L &&
	                (plan.sizes[b] < cfg.tinyPartitionThreshold ||
	                        plan.cycleCounts[b] > 0 ||
	                        plan.tupleTailMasks[b] != 0L);
	    }

	    static PartitionWork[] buildLocalMsdWorkItems(
	            MsdBucketPlan plan,
	            Config cfg
	    ) {
	        if (!plan.hasLocalMsd) {
	            return null;
	        }

	        ArrayList<PartitionWork> workItems = new ArrayList<>();
	        int localMsdBuckets = 0;

	        for (int b = 0; b < cfg.msdBucketCount; b++) {
	            if (!bucketHasLsdWork(plan, cfg, b)) {
	                continue;
	            }

	            int localMsdShift = plan.localMsdShifts[b];
	            if (localMsdShift >= 0) {
	                long[] starts = plan.localStarts[b];
	                int[] sizes = plan.localSizes[b];
	                long[] variableMasks = plan.localVariableMasks[b];

	                for (int child = 0; child < cfg.msdBucketCount; child++) {
	                    int size = sizes[child];
	                    long variableMask = variableMasks[child];

	                    if (size > 1 && variableMask != 0L) {
	                        if (size < cfg.tinyPartitionThreshold) {
	                            workItems.add(PartitionWork.tiny(starts[child], size));
	                            continue;
	                        }

	                        workItems.add(PartitionWork.partition(
	                                starts[child],
	                                size,
	                                variableMask,
	                                localMsdShift
	                        ));
	                    }
	                }
	                localMsdBuckets++;
	            } else {
	                if (plan.sizes[b] < cfg.tinyPartitionThreshold) {
	                    workItems.add(PartitionWork.tiny(plan.starts[b], plan.sizes[b]));
	                } else {
	                    workItems.add(PartitionWork.bucket(b, plan));
	                }
	            }
	        }

	        if (localMsdBuckets == 0) {
	            return null;
	        }

	        workItems.sort((a, b) -> Integer.compare(b.size, a.size));
	        return workItems.toArray(new PartitionWork[0]);
	    }

	    static void sortPartitionWork(
	            MemorySegment scratch,
	            MemorySegment dst,
	            MsdBucketPlan plan,
	            Config cfg,
	            Scratch sc,
	            PartitionWork work
	    ) {
	        if (work.tinySort) {
	            tinysort.tinyPartitionBitSort(dst, work.startPos, work.size, sc);
	            return;
	        }

	        if (work.bucket >= 0) {
	            sortOneMsdBucketWithLsdRadix(scratch, dst, plan, cfg, sc, work.bucket);
	            return;
	        }

	        sortPartitionByVariableMask(
	                scratch,
	                dst,
	                work.startPos,
	                work.size,
	                sc,
	                cfg,
	                work.variableMask,
	                work.remainingBits
	        );
	    }

	    public static int localMsdShiftForBucket(MsdBucketPlan plan, Config cfg, int b) {
	        if (!Apex.LOCAL_MSD_REPARTITION) {
	            return -1;
	        }

	        int size = plan.sizes[b];
	        if (size < Apex.LOCAL_MSD_MIN_RECORDS || size < cfg.tinyPartitionThreshold) {
	            return -1;
	        }

	        boolean offHeapSized = size > Apex.MAX_HEAP_SCRATCH_RECORDS;
	        if (!offHeapSized &&
	                Apex.LOCAL_MSD_MIN_SHARE_DIVISOR > 0 &&
	                (long) size * Apex.LOCAL_MSD_MIN_SHARE_DIVISOR < plan.totalRecords) {
	            return -1;
	        }

	        int plannedPasses = plan.cycleCounts[b] + (plan.tupleTailMasks[b] != 0L ? 1 : 0);
	        if (plannedPasses < Apex.LOCAL_MSD_MIN_PASSES) {
	            return -1;
	        }

	        long variableMask = plan.variableMasks[b];
	        if (variableMask == 0L || tuples.tupleSpaceFitsDirectPass(variableMask, size)) {
	            return -1;
	        }

	        int highestVariableBit = 63 - Long.numberOfLeadingZeros(variableMask);
	        int shift = Math.max(0, highestVariableBit - cfg.msdBits + 1);
	        long windowMask = tools.lowBitsMask(cfg.msdBits) << shift;
	        int windowBits = Long.bitCount(variableMask & windowMask);

	        return windowBits >= Apex.LOCAL_MSD_MIN_WINDOW_BITS ? shift : -1;
	    }

	    static void sortPartitionByVariableMask(
	            MemorySegment scratch,
	            MemorySegment dst,
	            long startPos,
	            int size,
	            Scratch sc,
	            Config cfg,
	            long variableMask,
	            int remainingBits
	    ) {
	        if (size <= 1 || variableMask == 0L) {
	            return;
	        }

	        if (size < cfg.tinyPartitionThreshold) {
	            tinysort.tinyPartitionBitSort(dst, startPos, size, sc);
	            return;
	        }

	        long tupleTailMask;
	        long tupleTailPlan;
	        int plannedCycles;

	        if (tuples.tupleSpaceFitsDirectPass(variableMask, size)) {
	            tuples.tryDirectTupleSpaceSort(scratch, dst, startPos, size, sc,
	                    variableMask, tuples.buildSmallTuplePlan(variableMask));
	            return;
	        }

	        int cycles = buildLsdCyclePlan(
	                variableMask,
	                cfg,
	                remainingBits,
	                sc.cycleShifts,
	                sc.cycleMasks,
	                sc.cycleBitMasks,
	                sc.cycleTuplePlans
	        );

	        if (cycles == 0) {
	            return;
	        }

	        plannedCycles = tuples.plannedCyclePrefixBeforeTupleTail(
	                variableMask,
	                sc.cycleBitMasks,
	                cycles,
	                size
	        );
	        tupleTailMask = tuples.tupleTailMaskAfterPrefix(
	                variableMask,
	                sc.cycleBitMasks,
	                plannedCycles,
	                size
	        );
	        tupleTailPlan = tuples.buildSmallTuplePlan(tupleTailMask);

	        if (plannedCycles == 0 && tupleTailMask == 0L) {
	            return;
	        }

	        if (size <= Apex.MAX_HEAP_SCRATCH_RECORDS) {
	            lsdRadixSortPartition(
	                    dst,
	                    startPos,
	                    size,
	                    sc,
	                    cfg,
	                    plannedCycles,
	                    sc.cycleShifts,
	                    sc.cycleMasks,
	                    sc.cycleBitMasks,
	                    sc.cycleTuplePlans,
	                    tupleTailMask,
	                    tupleTailPlan
	            );
	        } else {
	            lsdRadixSortPartitionOffHeap(
	                    scratch,
	                    dst,
	                    startPos,
	                    size,
	                    sc,
	                    cfg,
	                    plannedCycles,
	                    sc.cycleShifts,
	                    sc.cycleMasks,
	                    sc.cycleBitMasks,
	                    sc.cycleTuplePlans,
	                    tupleTailMask,
	                    tupleTailPlan
	            );
	        }
	    }
 
	    
	    
}
