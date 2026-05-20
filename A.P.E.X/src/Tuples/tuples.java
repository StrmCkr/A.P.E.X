package Tuples;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;

import Tools.tools;
import config.configurations.Config;
import main.Apex;
import main.Apex.Scratch;

public class tuples {
    
    public static int directTupleRadixCap() {
        return Apex.DIRECT_TUPLE_BITS <= 0 ? 1 : 1 << Apex.DIRECT_TUPLE_BITS;
    }

    public static boolean tupleSpaceFitsDirectPass(long entropyMask) {
        int tupleBits = Long.bitCount(entropyMask);
        return tupleBits > 1 && tupleBits <= Apex.DIRECT_TUPLE_BITS;
    }

    public static boolean tupleSpaceFitsDirectPass(long entropyMask, int size) {
        return tupleSpaceFitsDirectPass(entropyMask) && tupleRadix(entropyMask) <= size;
    }

    public static int tupleRadix(long entropyMask) {
        return 1 << Long.bitCount(entropyMask);
    }

    public static long buildSmallTuplePlan(long entropyMask) {
        int tupleBits = Long.bitCount(entropyMask);

        if (tupleBits <= 1 || tupleBits > Apex.SMALL_TUPLE_LOOKUP_BITS) {
            return 0L;
        }

        long plan = tupleBits;
        int outShift = 4;

        while (entropyMask != 0L) {
            long bit = entropyMask & -entropyMask;
            plan |= (long) Long.numberOfTrailingZeros(bit) << outShift;
            entropyMask ^= bit;
            outShift += 6;
        }

        return plan;
    }

    /**
     * 🚀 Hardware-Accelerated Tuple Index Extraction.
     * Capitalizes on JDK 25 BMI2 intrinsics to route straight to the single-cycle native PEXT instruction.
     */
    public static int tupleIndex(long key, long entropyMask, long smallTuplePlan) {
        // Under JDK 25, Long.compress maps directly to raw CPU hardware silicon pipelines
        return (int) Long.compress(key, entropyMask);
    }

    public static long tupleTailMaskAfterPrefix(long variableMask, long[] cycleBitMasks, int prefix) {
        return tupleTailMaskAfterPrefix(variableMask, cycleBitMasks, prefix, Integer.MAX_VALUE);
    }

    public static long tupleTailMaskAfterPrefix(long variableMask, long[] cycleBitMasks, int prefix, int size) {
        long consumed = 0L;

        for (int i = 0; i < prefix; i++) {
            consumed |= cycleBitMasks[i];
        }

        long tailMask = variableMask & ~consumed;
        return tupleSpaceFitsDirectPass(tailMask, size) ? tailMask : 0L;
    }
    
    public static boolean tryDirectTupleSpaceSort(
            MemorySegment scratch,
            MemorySegment dst,
            long startPos,
            int size,
            Apex.Scratch sc,
            long entropyMask,
            long smallTuplePlan
    ) {
        if (!tupleSpaceFitsDirectPass(entropyMask, size)) {
            return false;
        }

        if (size <= Apex.MAX_HEAP_SCRATCH_RECORDS) {
            directTupleSpaceSortHeap(dst, startPos, size, sc, entropyMask, smallTuplePlan);
        } else {
            directTupleSpaceSortOffHeap(scratch, dst, startPos, size, sc, entropyMask, smallTuplePlan);
        }

        return true;
    }

    public static void directTupleSpaceSortHeap(
            MemorySegment dst,
            long startPos,
            int size,
            Scratch sc,
            long entropyMask,
            long smallTuplePlan
    ) {
        sc.ensure(size);

        long base = startPos << 4;
        long p = base;
        boolean ascending = true;
        boolean descending = true;
        long previous = 0L;

        for (int i = 0; i < size; i++) {
            long key = dst.get(Apex.LONG, p);
            sc.k1[i] = key;
            sc.v1[i] = dst.get(Apex.LONG, p + 8);
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

        tupleCountingPass(sc.k1, sc.v1, sc.k2, sc.v2, size, sc, entropyMask, smallTuplePlan);

        p = base;
        for (int i = 0; i < size; i++) {
            dst.set(Apex.LONG, p, sc.k2[i]);
            dst.set(Apex.LONG, p + 8, sc.v2[i]);
            p += Apex.RECORD_BYTES;
        }
    }

    public static void directTupleSpaceSortOffHeap(
            MemorySegment scratch,
            MemorySegment dst,
            long startPos,
            int size,
            Scratch sc,
            long entropyMask,
            long smallTuplePlan
    ) {
        try {
            Apex.LARGE_PARTITION_PERMITS.acquire();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        }

        try {
            int radix = tupleRadix(entropyMask);
            sc.ensureCounts(radix);
            int[] counts = sc.counts;
            long dstBase = startPos << 4;

            tupleCountingPassSegments(dst, dstBase, scratch, dstBase, size, counts, entropyMask, smallTuplePlan);

            try {
                tools.parallelBulkCopy(scratch, dstBase, dst, dstBase, size);
            } catch (Exception e) {
                throw new RuntimeException("Parallel off-heap blit failed", e);
            }
        } finally {
            Apex.LARGE_PARTITION_PERMITS.release();
        }
    }    

    public static int buildPackedTupleLsdCyclePlan(
            long variableMask,
            Config cfg,
            int[] cycleShifts,
            int[] cycleMasks,
            long[] cycleBitMasks,
            long[] cycleTuplePlans
    ) {
        int cycles = 0;
        int bitsInCycle = 0;
        long bitMask = 0L;

        while (variableMask != 0L) {
            long bit = variableMask & -variableMask;
            variableMask ^= bit;
            bitMask |= bit;
            bitsInCycle++;

            if (bitsInCycle == cfg.lsdBits || variableMask == 0L) {
                cycleBitMasks[cycles] = bitMask;
                cycleMasks[cycles] = tools.lowIntMask(bitsInCycle);
                cycleShifts[cycles] = contiguousShift(bitMask);
                cycleTuplePlans[cycles] = cycleShifts[cycles] < 0 ? buildSmallTuplePlan(bitMask) : 0L;
                cycles++;

                bitMask = 0L;
                bitsInCycle = 0;
            }
        }

        return cycles;
    }

    public static int contiguousShift(long bitMask) {
        int shift = Long.numberOfTrailingZeros(bitMask);
        int bits = Long.bitCount(bitMask);
        return ((bitMask >>> shift) == tools.lowBitsMask(bits)) ? shift : -1;
    }

    public static int plannedCyclePrefixBeforeTupleTail(long variableMask, long[] cycleBitMasks, int cycles) {
        return plannedCyclePrefixBeforeTupleTail(variableMask, cycleBitMasks, cycles, Integer.MAX_VALUE);
    }

    public static int plannedCyclePrefixBeforeTupleTail(long variableMask, long[] cycleBitMasks, int cycles, int size) {
        if (tupleSpaceFitsDirectPass(variableMask, size)) {
            return 0;
        }

        long consumed = 0L;
        for (int prefix = 1; prefix < cycles; prefix++) {
            consumed |= cycleBitMasks[prefix - 1];
            if (tupleSpaceFitsDirectPass(variableMask & ~consumed, size)) {
                return prefix;
            }
        }
        return cycles;
    }
       
    public static void tupleCountingPass(
            long[] currentKeys,
            long[] currentValues,
            long[] nextKeys,
            long[] nextValues,
            int size,
            Scratch sc,
            long entropyMask,
            long smallTuplePlan
    ) {
        int radix = tuples.tupleRadix(entropyMask);
        sc.ensureCounts(radix);
        Arrays.fill(sc.counts, 0, radix, 0);

        for (int i = 0; i < size; i++) {
            int bin = (int) Long.compress(currentKeys[i], entropyMask);
            sc.counts[bin]++;
        }

        int sum = 0;
        for (int i = 0; i < radix; i++) {
            int c = sc.counts[i];
            sc.counts[i] = sum;
            sum += c;
        }

        for (int i = 0; i < size; i++) {
            long k = currentKeys[i];
            int bin = (int) Long.compress(k, entropyMask);
            int pos = sc.counts[bin]++;
            nextKeys[pos] = k;
            nextValues[pos] = currentValues[i];
        }
    }

    public static void tupleCountingPassSegments(
            MemorySegment source,
            long sourceBase,
            MemorySegment target,
            long targetBase,
            int size,
            int[] counts,
            long entropyMask,
            long smallTuplePlan
    ) {
        tupleCountingPassSegments(source, sourceBase, target, targetBase, size, counts,
                -1, tuples.tupleRadix(entropyMask) - 1, entropyMask, smallTuplePlan);
    }

    public static void tupleCountingPassSegments(
            MemorySegment source,
            long sourceBase,
            MemorySegment target,
            long targetBase,
            int size,
            int[] counts,
            int shift,
            int mask,
            long bitMask,
            long smallTuplePlan
    ) {
        int radixThisPass = mask + 1;
        Arrays.fill(counts, 0, radixThisPass, 0);

        long p = sourceBase;
        long end = sourceBase + ((long) size << 4);
        long unrolledEnd = end - (4L * Apex.RECORD_BYTES);

        // --- 🚀 Stride-Unrolled Counting Staged to Saturate L1 Caches ---
        while (p <= unrolledEnd) {
            long k0 = source.get(Apex.LONG, p);
            long k1 = source.get(Apex.LONG, p + 16);
            long k2 = source.get(Apex.LONG, p + 32);
            long k3 = source.get(Apex.LONG, p + 48);

            counts[(int) Long.compress(k0, bitMask)]++;
            counts[(int) Long.compress(k1, bitMask)]++;
            counts[(int) Long.compress(k2, bitMask)]++;
            counts[(int) Long.compress(k3, bitMask)]++;

            p += 4L * Apex.RECORD_BYTES;
        }

        while (p < end) {
            long k = source.get(Apex.LONG, p);
            counts[(int) Long.compress(k, bitMask)]++;
            p += Apex.RECORD_BYTES;
        }

        int sum = 0;
        for (int i = 0; i < radixThisPass; i++) {
            int c = counts[i];
            counts[i] = sum;
            sum += c;
        }

        p = sourceBase;

        // --- 🚀 Stride-Unrolled Writeback Shuffling Symmetrically ---
        while (p <= unrolledEnd) {
            long k0 = source.get(Apex.LONG, p);
            long v0 = source.get(Apex.LONG, p + 8);
            long k1 = source.get(Apex.LONG, p + 16);
            long v1 = source.get(Apex.LONG, p + 24);
            long k2 = source.get(Apex.LONG, p + 32);
            long v2 = source.get(Apex.LONG, p + 40);
            long k3 = source.get(Apex.LONG, p + 48);
            long v3 = source.get(Apex.LONG, p + 56);

            int bin0 = (int) Long.compress(k0, bitMask);
            long targetPos0 = targetBase + ((long) counts[bin0]++ << 4);
            target.set(Apex.LONG, targetPos0, k0);
            target.set(Apex.LONG, targetPos0 + 8, v0);

            int bin1 = (int) Long.compress(k1, bitMask);
            long targetPos1 = targetBase + ((long) counts[bin1]++ << 4);
            target.set(Apex.LONG, targetPos1, k1);
            target.set(Apex.LONG, targetPos1 + 8, v1);

            int bin2 = (int) Long.compress(k2, bitMask);
            long targetPos2 = targetBase + ((long) counts[bin2]++ << 4);
            target.set(Apex.LONG, targetPos2, k2);
            target.set(Apex.LONG, targetPos2 + 8, v2);

            int bin3 = (int) Long.compress(k3, bitMask);
            long targetPos3 = targetBase + ((long) counts[bin3]++ << 4);
            target.set(Apex.LONG, targetPos3, k3);
            target.set(Apex.LONG, targetPos3 + 8, v3);

            p += 4L * Apex.RECORD_BYTES;
        }

        while (p < end) {
            long k = source.get(Apex.LONG, p);
            long v = source.get(Apex.LONG, p + 8);

            int bin = (int) Long.compress(k, bitMask);
            long targetPos = targetBase + ((long) counts[bin]++ << 4);

            target.set(Apex.LONG, targetPos, k);
            target.set(Apex.LONG, targetPos + 8, v);
            p += Apex.RECORD_BYTES;
        }
    }
}
