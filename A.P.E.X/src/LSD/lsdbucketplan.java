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
	        final boolean ascending;
	        final boolean descending;

	        PartitionWork(int bucket, long startPos, int size, long variableMask, int remainingBits,
	                boolean tinySort, boolean ascending, boolean descending) {
	            this.bucket = bucket;
	            this.startPos = startPos;
	            this.size = size;
	            this.variableMask = variableMask;
	            this.remainingBits = remainingBits;
	            this.tinySort = tinySort;
	            this.ascending = ascending;
	            this.descending = descending;
	        }

	        static PartitionWork bucket(int bucket, MsdBucketPlan plan) {
	            return new PartitionWork(bucket, plan.starts[bucket], plan.sizes[bucket], 0L, 0, false,
	                    plan.bucketAscending[bucket], plan.bucketDescending[bucket]);
	        }

	        static PartitionWork tiny(long startPos, int size) {
	            return new PartitionWork(-1, startPos, size, 0L, 0, true, false, false);
	        }

	        static PartitionWork tiny(long startPos, int size, boolean ascending, boolean descending) {
	            return new PartitionWork(-1, startPos, size, 0L, 0, true, ascending, descending);
	        }

	        static PartitionWork partition(long startPos, int size, long variableMask, int remainingBits) {
	            return new PartitionWork(-1, startPos, size, variableMask, remainingBits, false, false, false);
	        }

	        static PartitionWork partition(long startPos, int size, long variableMask, int remainingBits,
	                boolean ascending, boolean descending) {
	            return new PartitionWork(-1, startPos, size, variableMask, remainingBits, false,
	                    ascending, descending);
	        }
	    }

	  public static int buildLsdCyclePlan(
	            long variableMask,
	            Config cfg,
	            int remainingBits,
	            int size,
	            int[] cycleShifts,
	            int[] cycleMasks,
	            long[] cycleBitMasks,
	            long[] cycleTuplePlans
	    ) {
	        variableMask &= tools.lowBitsMask(remainingBits);

	        int contiguousCycles = buildContiguousLsdCyclePlan(variableMask, cfg, remainingBits,
	                cycleShifts, cycleMasks, cycleBitMasks, cycleTuplePlans);
	        long bestScore = scoreExistingCyclePlan(variableMask, size, contiguousCycles,
	                cycleMasks, cycleBitMasks);
	        int bestTupleBits = 0;
	        int bestCycles = contiguousCycles;

	        int packedCycles = packedTupleCycleCount(variableMask, cfg);
	        if (packedCycles < contiguousCycles || Apex.PACKED_TUPLE_CYCLES) {
	            long packedScore = scorePackedTupleCyclePlan(variableMask, size, cfg.lsdBits);
	            if (Apex.PACKED_TUPLE_CYCLES || packedScore < bestScore) {
	                bestScore = packedScore;
	                bestTupleBits = cfg.lsdBits;
	                bestCycles = packedCycles;
	            }
	        }

	        int maxStaggerBits = maxStaggerTupleBits(cfg);
	        if (maxStaggerBits > cfg.lsdBits && size >= Apex.STAGGER_TUPLE_MIN_RECORDS &&
	                !Apex.STAGGER_TUPLE_COST_MODEL) {
	            int staggerCycles = packedTupleCycleCount(variableMask, maxStaggerBits);
	            if (staggerCycles > 0 && staggerCycles < bestCycles) {
	                bestTupleBits = maxStaggerBits;
	            }
	        } else if (maxStaggerBits > cfg.lsdBits && size >= Apex.STAGGER_TUPLE_MIN_RECORDS) {
	            for (int bits = cfg.lsdBits + 1; bits <= maxStaggerBits; bits++) {
	                long score = scorePackedTupleCyclePlan(variableMask, size, bits);
	                if (score < bestScore) {
	                    bestScore = score;
	                    bestTupleBits = bits;
	                    bestCycles = packedTupleCycleCount(variableMask, bits);
	                }
	            }

	            if (shouldPreferMaxWidthTupleTail(variableMask, size, maxStaggerBits)) {
	                bestTupleBits = maxStaggerBits;
	            }
	        }

	        if (bestTupleBits > 0) {
	            return tuples.buildPackedTupleCyclePlan(variableMask, bestTupleBits,
	                    cycleShifts, cycleMasks, cycleBitMasks, cycleTuplePlans);
	        }

	        return contiguousCycles;
	    }

	  public static int buildContiguousLsdCyclePlan(
		        long variableMask,
		        Config cfg,
		        int remainingBits,
		        int[] cycleShifts,
		        int[] cycleMasks,
		        long[] cycleBitMasks,
		        long[] cycleTuplePlans
		) {
		    // Zero out any high-order bits beyond our active range
		    variableMask &= tools.lowBitsMask(remainingBits);
		    int cycles = 0;

		    while (variableMask != 0) {
		        // 1. Instantly pinpoint the start of the next active contiguous run via TZCNT
		        int runStart = Long.numberOfTrailingZeros(variableMask);

		        // 2. Isolate the contiguous block by creating a mask that flips trailing zeros to ones
		        long invertedMask = ~variableMask;
		        // Strip out trailing zeros from the inverted space to locate where the active run stops
		        long trailingZerosCleared = invertedMask >>> runStart;
		        int runLength = Long.numberOfTrailingZeros(trailingZerosCleared);
		        int runEnd = runStart + runLength;

		        // 3. Construct your standard execution parameters for this discovered block range
		        for (int shift = runStart; shift < runEnd; shift += cfg.lsdBits) {
		            int bitsThisCycle = Math.min(cfg.lsdBits, runEnd - shift);
		            long bitMask = tools.lowBitsMask(bitsThisCycle) << shift;

		            cycleShifts[cycles] = shift;
		            cycleMasks[cycles] = tools.lowIntMask(bitsThisCycle);
		            cycleBitMasks[cycles] = bitMask;
		            cycleTuplePlans[cycles] = 0L;

		            cycles++;
		        }

		        // 4. Clear out the entire processed contiguous run in a single instruction pass
		        // (Creates a mask spanning from runEnd down to bit 0, then ANDs it away)
		        long clearMask = (runEnd >= 64) ? 0L : (~0L << runEnd);
		        variableMask &= clearMask;
		    }

		    return cycles;
		}


	  static int packedTupleCycleCount(long variableMask, Config cfg) {
	        return packedTupleCycleCount(variableMask, cfg.lsdBits);
	    }

	  static int packedTupleCycleCount(long variableMask, int bitsPerCycle) {
	        int variableBits = Long.bitCount(variableMask);
	        int cycleBits = Math.max(1, bitsPerCycle);
	        return variableBits == 0 ? 0 : (variableBits + cycleBits - 1) / cycleBits;
	    }

	  static int maxStaggerTupleBits(Config cfg) {
	        if (!Apex.STAGGER_TUPLE_CYCLES) {
	            return 0;
	        }

	        int configured = Math.min(Apex.STAGGER_TUPLE_BITS, Apex.DIRECT_TUPLE_BITS);
	        configured = Math.min(configured, Apex.MAX_DIRECT_TUPLE_BITS);
	        return configured > cfg.lsdBits ? configured : 0;
	    }

	  static long scoreExistingCyclePlan(
	            long variableMask,
	            int size,
	            int cycles,
	            int[] cycleMasks,
	            long[] cycleBitMasks
	    ) {
	        if (cycles == 0) {
	            return 0L;
	        }

	        long consumed = 0L;
	        long counterSlots = 0L;
	        int plannedCycles = 0;
	        int sparsePasses = 0;

	        for (int cycle = 0; cycle < cycles; cycle++) {
	            long bitMask = cycleBitMasks[cycle];
	            counterSlots += (long) cycleMasks[cycle] + 1L;
	            if (tuples.contiguousShift(bitMask) < 0) {
	                sparsePasses++;
	            }

	            consumed |= bitMask;
	            plannedCycles++;

	            if (cycle + 1 < cycles) {
	                long tailMask = variableMask & ~consumed;
	                if (tuples.tupleSpaceFitsDirectPass(tailMask, size)) {
	                    counterSlots += tuples.tupleRadix(tailMask);
	                    sparsePasses++;
	                    return cyclePlanScore(size, plannedCycles + 1, counterSlots, sparsePasses);
	                }
	            }
	        }

	        return cyclePlanScore(size, plannedCycles, counterSlots, sparsePasses);
	    }

	  static long scorePackedTupleCyclePlan(long variableMask, int size, int bitsPerCycle) {
	        int cycleBits = Math.max(1, Math.min(Apex.MAX_DIRECT_TUPLE_BITS, bitsPerCycle));
	        long remaining = variableMask;
	        long consumed = 0L;
	        long counterSlots = 0L;
	        int plannedPasses = 0;
	        int sparsePasses = 0;

	        while (remaining != 0L) {
	            long bitMask = 0L;
	            int bitsInCycle = 0;

	            while (remaining != 0L && bitsInCycle < cycleBits) {
	                long bit = remaining & -remaining;
	                remaining ^= bit;
	                bitMask |= bit;
	                bitsInCycle++;
	            }

	            counterSlots += 1L << bitsInCycle;
	            if (tuples.contiguousShift(bitMask) < 0) {
	                sparsePasses++;
	            }

	            consumed |= bitMask;
	            plannedPasses++;

	            if (remaining != 0L) {
	                long tailMask = variableMask & ~consumed;
	                if (tuples.tupleSpaceFitsDirectPass(tailMask, size)) {
	                    counterSlots += tuples.tupleRadix(tailMask);
	                    sparsePasses++;
	                    return cyclePlanScore(size, plannedPasses + 1, counterSlots, sparsePasses);
	                }
	            }
	        }

	        return cyclePlanScore(size, plannedPasses, counterSlots, sparsePasses);
	    }

	  static long cyclePlanScore(
	            int size,
	            int plannedPasses,
	            long counterSlots,
	            int sparsePasses
	    ) {
	        long recordTraffic = (long) plannedPasses * Math.max(1, size);
	        long sparsePenalty = (long) sparsePasses * Math.max(1, size >>> 4);
	        return recordTraffic + counterSlots + sparsePenalty;
	    }

	  static boolean shouldPreferMaxWidthTupleTail(
	            long variableMask,
	            int size,
	            int maxBits
	    ) {
	        int variableBits = Long.bitCount(variableMask);
	        return variableBits > maxBits &&
	                variableBits <= (maxBits << 1) &&
	                size >= (1 << maxBits);
	    }
	  	

	  static void setTupleCycle(
	            int cycle,
	            long bitMask,
	            int[] cycleShifts,
	            int[] cycleMasks,
	            long[] cycleBitMasks,
	            long[] cycleTuplePlans
	    ) {
	        int shift = tuples.contiguousShift(bitMask);
	        cycleShifts[cycle] = shift;
	        cycleMasks[cycle] = tools.lowIntMask(Long.bitCount(bitMask));
	        cycleBitMasks[cycle] = bitMask;
	        cycleTuplePlans[cycle] = shift < 0 ? tuples.buildSmallTuplePlan(bitMask) : 0L;
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

	        if (localWorkItems != null) {
	            if (localWorkItems.length == 0) {
	                return;
	            }

	            // Track global work progress via atomic indices
	            AtomicInteger nextWork = new AtomicInteger();

	            for (int t = 0; t < Apex.THREADS; t++) {
	               // final int threadId = t;
	                futures.add(Apex.POOL.submit(() -> {
	                    try {
	                        Scratch sc = tls.get();

	                        for (;;) {
	                            // Subsystem B Check: Dynamically scale how many work items are claimed
	                            // by looking at the remaining density to prevent queue lock contention.
	                            long remainingWorkCount = localWorkItems.length - nextWork.get();
	                            int adaptiveBatch = (remainingWorkCount <= 64) ? 1 : 
	                                                Math.max(4, Math.min(2048, (int)(remainingWorkCount / Apex.THREADS)));

	                            int startWork = nextWork.getAndAdd(adaptiveBatch);
	                            if (startWork >= localWorkItems.length) {
	                                break;
	                            }

	                            int endWork = Math.min(startWork + adaptiveBatch, localWorkItems.length);
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

	            // Atomic progress pointer
	            AtomicInteger nextBucket = new AtomicInteger();

	            for (int t = 0; t < Apex.THREADS; t++) {
	                futures.add(Apex.POOL.submit(() -> {
	                    try {
	                        Scratch sc = tls.get();

	                        for (;;) {
	                            int chosenWorkIndex = nextBucket.getAndIncrement();
	                            if (chosenWorkIndex >= workBuckets.length) {
	                                break;
	                            }

	                            // Subsystem B Check: Adaptive task sizing applied directly to bucket volumes
	                            int bucketId = workBuckets[chosenWorkIndex];
	                        //    long bucketDataSize = plan.sizes[bucketId];
	                            
	                            // If processing a massive skewed chunk (like your Zipfian 7.8M outlier),
	                            // this adaptive threshold tells us if it should run immediately.
	                            sortOneMsdBucketWithLsdRadix(scratch, dst, plan, cfg, sc, bucketId);
	                        }
	                    } finally {
	                        tls.remove();
	                    }
	                }));
	            }
	        } else {
	            // Classic static partitioning routing logic
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
	        if (plan.bucketAscending[b]) {
	            return;
	        }
	        if (plan.bucketDescending[b]) {
	            tools.reverseRecordsInPlace(dst, startPos << 4, size);
	            return;
	        }

	        if (size < cfg.tinyPartitionThreshold) {
	            tinysort.tinyPartitionBitSort(dst, startPos, size, sc);
	            return;
	        }

	        if (cycles == 0 && tuples.tryDirectTupleSpaceSort(
	                scratch, dst, startPos, size, sc, tupleTailMask, tupleTailPlan, false
	        )) {
	            return;
	        }

	        if (size > Apex.MAX_HEAP_SCRATCH_RECORDS &&
	                tryDominantPrefixCoreSort(scratch, dst, startPos, size, sc, cfg,
	                        plan.variableMasks[b], plan.msdShift)) {
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

	        loadHeapPartition(dst, base, size, currentKeys, currentValues);

	        for (int cycle = 0; cycle < cycles; cycle++) {
	            int shift = cycleShifts[cycle];
	            int mask = cycleMasks[cycle];
	            long bitMask = cycleBitMasks[cycle];
	            long smallTuplePlan = cycleTuplePlans[cycle];

	            int radixThisPass = mask + 1;

	            sc.ensureCounts(radixThisPass);
	            Arrays.fill(sc.counts, 0, radixThisPass, 0);

	            countHeapDigits(currentKeys, size, sc.counts, shift, mask, bitMask, smallTuplePlan);

	            int sum = 0;

	            for (int i = 0; i < radixThisPass; i++) {
	                int c = sc.counts[i];
	                sc.counts[i] = sum;
	                sum += c;
	            }

	            scatterHeapDigits(currentKeys, currentValues, nextKeys, nextValues, size,
	                    sc.counts, shift, mask, bitMask, smallTuplePlan);

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

	        storeHeapPartition(dst, base, size, currentKeys, currentValues);
	    }

	    private static void loadHeapPartition(
	            MemorySegment dst,
	            long base,
	            int size,
	            long[] keys,
	            long[] values
	    ) {
	        int i = 0;
	        long p = base;

	        if (useHeapUnroll8(size)) {
	            int vectorEnd = size - (size & 7);
	            for (; i < vectorEnd; i += 8) {
	                keys[i] = dst.get(Apex.LONG, p);
	                values[i] = dst.get(Apex.LONG, p + 8);
	                keys[i + 1] = dst.get(Apex.LONG, p + 16);
	                values[i + 1] = dst.get(Apex.LONG, p + 24);
	                keys[i + 2] = dst.get(Apex.LONG, p + 32);
	                values[i + 2] = dst.get(Apex.LONG, p + 40);
	                keys[i + 3] = dst.get(Apex.LONG, p + 48);
	                values[i + 3] = dst.get(Apex.LONG, p + 56);
	                keys[i + 4] = dst.get(Apex.LONG, p + 64);
	                values[i + 4] = dst.get(Apex.LONG, p + 72);
	                keys[i + 5] = dst.get(Apex.LONG, p + 80);
	                values[i + 5] = dst.get(Apex.LONG, p + 88);
	                keys[i + 6] = dst.get(Apex.LONG, p + 96);
	                values[i + 6] = dst.get(Apex.LONG, p + 104);
	                keys[i + 7] = dst.get(Apex.LONG, p + 112);
	                values[i + 7] = dst.get(Apex.LONG, p + 120);
	                p += 8L * Apex.RECORD_BYTES;
	            }
	        }

	        for (; i < size; i++) {
	            keys[i] = dst.get(Apex.LONG, p);
	            values[i] = dst.get(Apex.LONG, p + 8);
	            p += Apex.RECORD_BYTES;
	        }
	    }

	    private static void countHeapDigits(
	            long[] keys,
	            int size,
	            int[] counts,
	            int shift,
	            int mask,
	            long bitMask,
	            long smallTuplePlan
	    ) {
	        int i = 0;

	        if (useHeapUnroll8(size)) {
	            int vectorEnd = size - (size & 7);
	            for (; i < vectorEnd; i += 8) {
	                counts[tools.digit(keys[i], shift, mask, bitMask, smallTuplePlan)]++;
	                counts[tools.digit(keys[i + 1], shift, mask, bitMask, smallTuplePlan)]++;
	                counts[tools.digit(keys[i + 2], shift, mask, bitMask, smallTuplePlan)]++;
	                counts[tools.digit(keys[i + 3], shift, mask, bitMask, smallTuplePlan)]++;
	                counts[tools.digit(keys[i + 4], shift, mask, bitMask, smallTuplePlan)]++;
	                counts[tools.digit(keys[i + 5], shift, mask, bitMask, smallTuplePlan)]++;
	                counts[tools.digit(keys[i + 6], shift, mask, bitMask, smallTuplePlan)]++;
	                counts[tools.digit(keys[i + 7], shift, mask, bitMask, smallTuplePlan)]++;
	            }
	        }

	        for (; i < size; i++) {
	            counts[tools.digit(keys[i], shift, mask, bitMask, smallTuplePlan)]++;
	        }
	    }

	    private static void scatterHeapDigits(
	            long[] currentKeys,
	            long[] currentValues,
	            long[] nextKeys,
	            long[] nextValues,
	            int size,
	            int[] counts,
	            int shift,
	            int mask,
	            long bitMask,
	            long smallTuplePlan
	    ) {
	        int i = 0;

	        if (useHeapUnroll8(size)) {
	            int vectorEnd = size - (size & 7);
	            for (; i < vectorEnd; i += 8) {
	                long k0 = currentKeys[i];
	                int bin0 = tools.digit(k0, shift, mask, bitMask, smallTuplePlan);
	                int pos0 = counts[bin0]++;
	                nextKeys[pos0] = k0;
	                nextValues[pos0] = currentValues[i];

	                long k1 = currentKeys[i + 1];
	                int bin1 = tools.digit(k1, shift, mask, bitMask, smallTuplePlan);
	                int pos1 = counts[bin1]++;
	                nextKeys[pos1] = k1;
	                nextValues[pos1] = currentValues[i + 1];

	                long k2 = currentKeys[i + 2];
	                int bin2 = tools.digit(k2, shift, mask, bitMask, smallTuplePlan);
	                int pos2 = counts[bin2]++;
	                nextKeys[pos2] = k2;
	                nextValues[pos2] = currentValues[i + 2];

	                long k3 = currentKeys[i + 3];
	                int bin3 = tools.digit(k3, shift, mask, bitMask, smallTuplePlan);
	                int pos3 = counts[bin3]++;
	                nextKeys[pos3] = k3;
	                nextValues[pos3] = currentValues[i + 3];

	                long k4 = currentKeys[i + 4];
	                int bin4 = tools.digit(k4, shift, mask, bitMask, smallTuplePlan);
	                int pos4 = counts[bin4]++;
	                nextKeys[pos4] = k4;
	                nextValues[pos4] = currentValues[i + 4];

	                long k5 = currentKeys[i + 5];
	                int bin5 = tools.digit(k5, shift, mask, bitMask, smallTuplePlan);
	                int pos5 = counts[bin5]++;
	                nextKeys[pos5] = k5;
	                nextValues[pos5] = currentValues[i + 5];

	                long k6 = currentKeys[i + 6];
	                int bin6 = tools.digit(k6, shift, mask, bitMask, smallTuplePlan);
	                int pos6 = counts[bin6]++;
	                nextKeys[pos6] = k6;
	                nextValues[pos6] = currentValues[i + 6];

	                long k7 = currentKeys[i + 7];
	                int bin7 = tools.digit(k7, shift, mask, bitMask, smallTuplePlan);
	                int pos7 = counts[bin7]++;
	                nextKeys[pos7] = k7;
	                nextValues[pos7] = currentValues[i + 7];
	            }
	        }

	        for (; i < size; i++) {
	            long k = currentKeys[i];
	            int bin = tools.digit(k, shift, mask, bitMask, smallTuplePlan);
	            int pos = counts[bin]++;
	            nextKeys[pos] = k;
	            nextValues[pos] = currentValues[i];
	        }
	    }

	    private static void storeHeapPartition(
	            MemorySegment dst,
	            long base,
	            int size,
	            long[] keys,
	            long[] values
	    ) {
	        int i = 0;
	        long p = base;

	        if (useHeapUnroll8(size)) {
	            int vectorEnd = size - (size & 7);
	            for (; i < vectorEnd; i += 8) {
	                dst.set(Apex.LONG, p, keys[i]);
	                dst.set(Apex.LONG, p + 8, values[i]);
	                dst.set(Apex.LONG, p + 16, keys[i + 1]);
	                dst.set(Apex.LONG, p + 24, values[i + 1]);
	                dst.set(Apex.LONG, p + 32, keys[i + 2]);
	                dst.set(Apex.LONG, p + 40, values[i + 2]);
	                dst.set(Apex.LONG, p + 48, keys[i + 3]);
	                dst.set(Apex.LONG, p + 56, values[i + 3]);
	                dst.set(Apex.LONG, p + 64, keys[i + 4]);
	                dst.set(Apex.LONG, p + 72, values[i + 4]);
	                dst.set(Apex.LONG, p + 80, keys[i + 5]);
	                dst.set(Apex.LONG, p + 88, values[i + 5]);
	                dst.set(Apex.LONG, p + 96, keys[i + 6]);
	                dst.set(Apex.LONG, p + 104, values[i + 6]);
	                dst.set(Apex.LONG, p + 112, keys[i + 7]);
	                dst.set(Apex.LONG, p + 120, values[i + 7]);
	                p += 8L * Apex.RECORD_BYTES;
	            }
	        }

	        for (; i < size; i++) {
	            dst.set(Apex.LONG, p, keys[i]);
	            dst.set(Apex.LONG, p + 8, values[i]);
	            p += Apex.RECORD_BYTES;
	        }
	    }

	    private static boolean useHeapUnroll8(int size) {
	        if (Apex.LSD_HEAP_UNROLL >= 8) {
	            return true;
	        }

	        return Apex.LSD_HEAP_UNROLL == 0 && size >= Apex.LSD_HEAP_UNROLL_MIN_RECORDS;
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

	                try {
	                    tuples.parallelTupleCountingPassSegments(
	                            source,
	                            sourceBase,
	                            target,
	                            targetBase,
	                            size,
	                            shift,
	                            mask,
	                            bitMask,
	                            smallTuplePlan
	                    );
	                } catch (Exception e) {
	                    throw new RuntimeException("Parallel off-heap radix pass failed", e);
	                }

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

	                try {
	                    tuples.parallelTupleCountingPassSegments(
	                            source,
	                            sourceBase,
	                            target,
	                            targetBase,
	                            size,
	                            -1,
	                            mask,
	                            tupleTailMask,
	                            tupleTailPlan
	                    );
	                } catch (Exception e) {
	                    throw new RuntimeException("Parallel off-heap tuple-tail pass failed", e);
	                }

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
	        if (plan.sizes[b] <= 1) {
	            return false;
	        }

	        if (plan.bucketFlags[b] == Apex.BUCKET_DESCENDING) {
	            return true;
	        }

	        return plan.bucketFlags[b] == Apex.BUCKET_MIXED &&
	                plan.variableMasks[b] != 0L &&
	                (plan.sizes[b] < cfg.tinyPartitionThreshold ||
	                        plan.cycleCounts[b] > 0 ||
	                        plan.tupleTailMasks[b] != 0L);
	    }

	    public static boolean localChildHasLsdWork(MsdBucketPlan plan, int bucket, int child) {
	        int[] sizes = plan.localSizes[bucket];
	        long[] variableMasks = plan.localVariableMasks[bucket];
	        if (sizes == null || variableMasks == null ||
	                sizes[child] <= 1 || variableMasks[child] == 0L) {
	            return false;
	        }

	        return plan.localAscending[bucket] == null || !plan.localAscending[bucket][child];
	    }

	    public static boolean bucketHasScheduledLsdWork(MsdBucketPlan plan, Config cfg, int b) {
	        if (!bucketHasLsdWork(plan, cfg, b)) {
	            return false;
	        }

	        if (plan.localMsdShifts[b] < 0) {
	            return true;
	        }

	        int childCount = plan.localSizes[b] == null ? 0 : plan.localSizes[b].length;
	        for (int child = 0; child < childCount; child++) {
	            if (localChildHasLsdWork(plan, b, child)) {
	                return true;
	            }
	        }

	        return false;
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

	                for (int child = 0; child < sizes.length; child++) {
	                    if (!localChildHasLsdWork(plan, b, child)) {
	                        continue;
	                    }

	                    int size = sizes[child];
	                    long variableMask = variableMasks[child];
	                    boolean ascending = plan.localAscending[b] != null && plan.localAscending[b][child];
	                    boolean descending = plan.localDescending[b] != null && plan.localDescending[b][child];

	                    if (size < cfg.tinyPartitionThreshold) {
	                        workItems.add(PartitionWork.tiny(starts[child], size, ascending, descending));
	                        continue;
	                    }

	                    workItems.add(PartitionWork.partition(
	                            starts[child],
	                            size,
	                            variableMask,
	                            localMsdShift,
	                            ascending,
	                            descending
	                    ));
	                }
	                localMsdBuckets++;
	            } else {
	                if (plan.sizes[b] < cfg.tinyPartitionThreshold) {
	                    workItems.add(PartitionWork.tiny(plan.starts[b], plan.sizes[b],
	                            plan.bucketAscending[b], plan.bucketDescending[b]));
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
	            if (work.ascending) {
	                return;
	            }
	            if (work.descending) {
	                tools.reverseRecordsInPlace(dst, work.startPos << 4, work.size);
	                return;
	            }
	            tinysort.tinyPartitionBitSort(dst, work.startPos, work.size, sc);
	            return;
	        }

	        if (work.bucket >= 0) {
	            sortOneMsdBucketWithLsdRadix(scratch, dst, plan, cfg, sc, work.bucket);
	            return;
	        }

	        if (work.ascending) {
	            return;
	        }
	        if (work.descending) {
	            tools.reverseRecordsInPlace(dst, work.startPos << 4, work.size);
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
	        return localMsdShiftForBucket(plan, cfg, b, localMsdBits(cfg));
	    }

	    public static int localMsdShiftForBucket(MsdBucketPlan plan, Config cfg, int b, int localBits) {
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
	        int shift = Math.max(0, highestVariableBit - localBits + 1);
	        long windowMask = tools.lowBitsMask(localBits) << shift;
	        int windowBits = Long.bitCount(variableMask & windowMask);

	        return windowBits >= Apex.LOCAL_MSD_MIN_WINDOW_BITS ? shift : -1;
	    }

	    public static int localMsdBits(Config cfg) {
	        return Apex.LOCAL_MSD_BITS > 0 ? Apex.LOCAL_MSD_BITS : cfg.msdBits;
	    }

	    public static int localMsdBitsForCandidateCount(Config cfg, int candidateCount) {
	        int bits = localMsdBits(cfg);
	        if (candidateCount <= 0 || Apex.LOCAL_MSD_MAX_CHILDREN <= 0) {
	            return bits;
	        }

	        int childrenPerBucket = Math.max(1, Apex.LOCAL_MSD_MAX_CHILDREN / candidateCount);
	        int cappedBits = 31 - Integer.numberOfLeadingZeros(childrenPerBucket);
	        return Math.max(1, Math.min(bits, cappedBits));
	    }

	    public static int localMsdBucketCount(Config cfg) {
	        return 1 << localMsdBits(cfg);
	    }

	    public static int localMsdBucketCount(int localBits) {
	        return 1 << localBits;
	    }

	    static boolean tryDominantPrefixCoreSort(
	            MemorySegment scratch,
	            MemorySegment dst,
	            long startPos,
	            int size,
	            Scratch sc,
	            Config cfg,
	            long variableMask,
	            int remainingBits
	    ) {
	        if (!Apex.DOMINANT_CORE_FAST_PATH ||
	                size <= Apex.MAX_HEAP_SCRATCH_RECORDS ||
	                scratch == dst) {
	            return false;
	        }

	        int candidateCap = Math.max(2, Math.min(256, Apex.DOMINANT_CORE_CANDIDATES));
	        long[] candidates = new long[candidateCap];
	        int[] votes = new int[candidateCap];
	        boolean[] used = new boolean[candidateCap];
	        int sample = Math.min(size, Math.max(1, Apex.DOMINANT_CORE_SAMPLE_RECORDS));
	        long base = startPos << 4;
	        long p = base;

	        for (int i = 0; i < sample; i++) {
	            long key = dst.get(Apex.LONG, p);
	            int slot = findUsedKey(candidates, used, key);

	            if (slot >= 0) {
	                votes[slot]++;
	            } else {
	                int empty = findEmpty(used);
	                if (empty >= 0) {
	                    used[empty] = true;
	                    candidates[empty] = key;
	                    votes[empty] = 1;
	                } else {
	                    for (int c = 0; c < candidateCap; c++) {
	                        if (--votes[c] == 0) {
	                            used[c] = false;
	                        }
	                    }
	                }
	            }

	            p += Apex.RECORD_BYTES;
	        }

	        int candidateCount = compactCandidates(candidates, used);
	        if (candidateCount == 0) {
	            return false;
	        }

	        int tableSize = 1;
	        while (tableSize < candidateCount * 4) {
	            tableSize <<= 1;
	        }

	        long[] tableKeys = new long[tableSize];
	        int[] tableIndexes = new int[tableSize];
	        boolean[] tableUsed = new boolean[tableSize];
	        int[] counts = new int[candidateCount];

	        for (int i = 0; i < candidateCount; i++) {
	            insertCandidate(tableKeys, tableIndexes, tableUsed, candidates[i], i);
	        }

	        p = base;
	        long end = base + ((long) size << 4);
	        while (p < end) {
	            long key = dst.get(Apex.LONG, p);
	            int index = lookupCandidate(tableKeys, tableIndexes, tableUsed, key);
	            if (index >= 0) {
	                counts[index]++;
	            }
	            p += Apex.RECORD_BYTES;
	        }

	        int minPerKey = Math.max(2, size / Math.max(2, Apex.DOMINANT_KEY_MIN_SHARE_DIVISOR));
	        int heavyCount = 0;

	        for (int i = 0; i < candidateCount; i++) {
	            if (counts[i] >= minPerKey) {
	                candidates[heavyCount] = candidates[i];
	                counts[heavyCount] = counts[i];
	                heavyCount++;
	            }
	        }

	        if (heavyCount == 0) {
	            return false;
	        }

	        sortCandidateCounts(candidates, counts, heavyCount);

	        tableSize = 1;
	        while (tableSize < heavyCount * 4) {
	            tableSize <<= 1;
	        }

	        tableKeys = new long[tableSize];
	        tableIndexes = new int[tableSize];
	        tableUsed = new boolean[tableSize];

	        long coreRecords = 0L;
	        for (int i = 0; i < heavyCount; i++) {
	            insertCandidate(tableKeys, tableIndexes, tableUsed, candidates[i], i);
	            coreRecords += counts[i];
	        }

	        long minRequired = (long) size * Math.max(1, Apex.DOMINANT_CORE_MIN_SHARE_PERCENT);
	        if (coreRecords * 100L < minRequired || coreRecords >= size) {
	            return false;
	        }

	        boolean sawNonCore = false;
	        long nonCoreMin = 0L;
	        long coreMax = candidates[heavyCount - 1];
	        p = base;

	        while (p < end) {
	            long key = dst.get(Apex.LONG, p);
	            if (lookupCandidate(tableKeys, tableIndexes, tableUsed, key) < 0) {
	                if (!sawNonCore || Long.compareUnsigned(key, nonCoreMin) < 0) {
	                    nonCoreMin = key;
	                }
	                sawNonCore = true;
	            }
	            p += Apex.RECORD_BYTES;
	        }

	        if (!sawNonCore || Long.compareUnsigned(coreMax, nonCoreMin) >= 0) {
	            return false;
	        }

	        int[] offsets = new int[heavyCount];
	        int running = 0;
	        for (int i = 0; i < heavyCount; i++) {
	            offsets[i] = running;
	            running += counts[i];
	        }

	        int nonCoreOffset = (int) coreRecords;
	        p = base;
	        long scratchBase = base;

	        while (p < end) {
	            long key = dst.get(Apex.LONG, p);
	            long value = dst.get(Apex.LONG, p + 8);
	            int index = lookupCandidate(tableKeys, tableIndexes, tableUsed, key);
	            int out = index >= 0 ? offsets[index]++ : nonCoreOffset++;
	            long target = scratchBase + ((long) out << 4);

	            scratch.set(Apex.LONG, target, key);
	            scratch.set(Apex.LONG, target + 8, value);
	            p += Apex.RECORD_BYTES;
	        }

	        try {
	            tools.parallelBulkCopy(scratch, scratchBase, dst, base, size);
	        } catch (Exception e) {
	            throw new RuntimeException("Dominant core copy failed", e);
	        }

	        int tailSize = size - (int) coreRecords;
	        if (tailSize > 1) {
	            sortPartitionByVariableMask(
	                    scratch,
	                    dst,
	                    startPos + coreRecords,
	                    tailSize,
	                    sc,
	                    cfg,
	                    variableMask,
	                    remainingBits
	            );
	        }

	        return true;
	    }

	    static int findUsedKey(long[] keys, boolean[] used, long key) {
	        for (int i = 0; i < keys.length; i++) {
	            if (used[i] && keys[i] == key) {
	                return i;
	            }
	        }
	        return -1;
	    }

	    static int findEmpty(boolean[] used) {
	        for (int i = 0; i < used.length; i++) {
	            if (!used[i]) {
	                return i;
	            }
	        }
	        return -1;
	    }

	    static int compactCandidates(long[] candidates, boolean[] used) {
	        int count = 0;
	        for (int i = 0; i < candidates.length; i++) {
	            if (used[i]) {
	                candidates[count++] = candidates[i];
	            }
	        }
	        return count;
	    }

	    static void insertCandidate(long[] keys, int[] indexes, boolean[] used, long key, int index) {
	        int mask = keys.length - 1;
	        int slot = Long.hashCode(key) & mask;

	        while (used[slot]) {
	            if (keys[slot] == key) {
	                indexes[slot] = index;
	                return;
	            }
	            slot = (slot + 1) & mask;
	        }

	        used[slot] = true;
	        keys[slot] = key;
	        indexes[slot] = index;
	    }

	    static int lookupCandidate(long[] keys, int[] indexes, boolean[] used, long key) {
	        int mask = keys.length - 1;
	        int slot = Long.hashCode(key) & mask;

	        while (used[slot]) {
	            if (keys[slot] == key) {
	                return indexes[slot];
	            }
	            slot = (slot + 1) & mask;
	        }

	        return -1;
	    }

	    static void sortCandidateCounts(long[] keys, int[] counts, int size) {
	        for (int i = 1; i < size; i++) {
	            long key = keys[i];
	            int count = counts[i];
	            int j = i - 1;

	            while (j >= 0 && Long.compareUnsigned(keys[j], key) > 0) {
	                keys[j + 1] = keys[j];
	                counts[j + 1] = counts[j];
	                j--;
	            }

	            keys[j + 1] = key;
	            counts[j + 1] = count;
	        }
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
	                    variableMask, tuples.buildSmallTuplePlan(variableMask), false);
	            return;
	        }

	        if (size > Apex.MAX_HEAP_SCRATCH_RECORDS &&
	                tryDominantPrefixCoreSort(scratch, dst, startPos, size, sc, cfg,
	                        variableMask, remainingBits)) {
	            return;
	        }

	        int cycles = buildLsdCyclePlan(
	                variableMask,
	                cfg,
	                remainingBits,
	                size,
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
