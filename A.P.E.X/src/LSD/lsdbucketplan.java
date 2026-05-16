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
	  public static int buildLsdCyclePlan(
	            long variableMask,
	            Config cfg,
	            int remainingBits,
	            int[] cycleShifts,
	            int[] cycleMasks,
	            long[] cycleBitMasks
	    ) {
	        variableMask &= tools.lowBitsMask(remainingBits);

	        if (!Apex.PACKED_TUPLE_CYCLES) {
	            return buildContiguousLsdCyclePlan(variableMask, cfg, remainingBits,
	                    cycleShifts, cycleMasks, cycleBitMasks);
	        }

	        return tuples.buildPackedTupleLsdCyclePlan(variableMask, cfg, cycleShifts, cycleMasks, cycleBitMasks);
	    }

	  public  static int buildContiguousLsdCyclePlan(
	            long variableMask,
	            Config cfg,
	            int remainingBits,
	            int[] cycleShifts,
	            int[] cycleMasks,
	            long[] cycleBitMasks
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

	                cycles++;
	            }
	        }

	        return cycles;
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

	        if (Apex.LSD_WORK_STEALING) {
	            int[] workBuckets = msdbucketplan.buildLsdWorkBucketsByDescendingSize(plan, cfg);
	            AtomicInteger nextBucket = new AtomicInteger();

	            for (int t = 0; t < Apex.THREADS; t++) {
	                futures.add(Apex.POOL.submit(() -> {
	                    try {
	                        Scratch sc = tls.get();

	                        for (int workIndex; (workIndex = nextBucket.getAndIncrement()) < workBuckets.length; ) {
	                            sortOneMsdBucketWithLsdRadix(scratch, dst, plan, cfg, sc, workBuckets[workIndex]);
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

	        for (int i = 0; i < size; i++) {
	            currentKeys[i] = dst.get(Apex.LONG, p);
	            currentValues[i] = dst.get(Apex.LONG, p + 8);
	            p += Apex.RECORD_BYTES;
	        }

	        for (int cycle = 0; cycle < cycles; cycle++) {
	            int shift = cycleShifts[cycle];
	            int mask = cycleMasks[cycle];
	            long bitMask = cycleBitMasks[cycle];

	            int radixThisPass = mask + 1;

	            sc.ensureCounts(radixThisPass);
	            Arrays.fill(sc.counts, 0, radixThisPass, 0);

	            for (int i = 0; i < size; i++) {
	                int bin = tools.lsdDigit(currentKeys[i], shift, mask, bitMask);
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
	                int bin = tools.lsdDigit(k, shift, mask, bitMask);

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
	                smallTuplePlan = 0L;

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
	                (plan.sizes[b] < cfg.tinyPartitionThreshold ||
	                        plan.cycleCounts[b] > 0 ||
	                        plan.tupleTailMasks[b] != 0L);
	    }
 
	    
	    
}
