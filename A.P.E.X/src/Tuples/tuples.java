package Tuples;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Future;

import Tools.tools;
import main.Apex;
import main.Apex.Scratch;

public class tuples {
    
    public static int directTupleRadixCap() {
        int tupleBits = Math.max(Apex.DIRECT_TUPLE_BITS, Apex.DIRECT_TUPLE_CONTIGUOUS_BITS);
        return tupleBits <= 0 ? 1 : 1 << tupleBits;
    }

    public static boolean tupleSpaceFitsDirectPass(long entropyMask) {
        int tupleBits = Long.bitCount(entropyMask);
        return tupleBits > 1 && tupleBits <= Apex.DIRECT_TUPLE_BITS;
    }

    public static boolean tupleSpaceFitsDirectPass(long entropyMask, int size) {
        int tupleBits = Long.bitCount(entropyMask);
        int maxTupleBits = Math.max(Apex.DIRECT_TUPLE_BITS, Apex.DIRECT_TUPLE_CONTIGUOUS_BITS);
        if (tupleBits <= 1 || tupleBits > maxTupleBits) {
            return false;
        }

        if ((1 << tupleBits) > size) {
            return false;
        }

        if (tupleBits <= Apex.DIRECT_TUPLE_BITS) {
            return true;
        }

        return tupleBits <= Apex.DIRECT_TUPLE_CONTIGUOUS_BITS &&
                size <= Apex.MAX_HEAP_SCRATCH_RECORDS &&
                tupleShift(entropyMask) >= 0;
    }

    public static boolean directTupleUsesInPlace(int size) {
        return directTupleUsesInPlace(size, false);
    }

    public static boolean directTupleUsesInPlace(int size, boolean preferInPlace) {
        return preferInPlace || size <= Apex.DIRECT_TUPLE_IN_PLACE_MAX_RECORDS;
    }

    public static boolean directTupleUsesInPlace(
            MemorySegment scratch,
            MemorySegment dst,
            int size,
            boolean checkOrder,
            boolean preferInPlace
    ) {
        return checkOrder || scratch == dst || directTupleUsesInPlace(size, preferInPlace);
    }

    public static int tupleRadix(long entropyMask) {
        return 1 << Long.bitCount(entropyMask);
    }

    /**
     * 🚀 Hardware-Accelerated Tuple Index Extraction.
     * Capitalizes on JDK 25 BMI2 intrinsics to route straight to the single-cycle native PEXT instruction.
     */
    public static int tupleIndex(long key, long entropyMask) {
        // Under JDK 25, Long.compress maps directly to raw CPU hardware silicon pipelines
        return (int) Long.compress(key ^ Apex.KEY_ORDER_XOR, entropyMask);
    }

    public static int tupleShift(long entropyMask) {
        return contiguousShift(entropyMask);
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
            long entropyMask
    ) {
        return tryDirectTupleSpaceSort(scratch, dst, startPos, size, sc, entropyMask, true);
    }

    public static boolean tryDirectTupleSpaceSort(
            MemorySegment scratch,
            MemorySegment dst,
            long startPos,
            int size,
            Apex.Scratch sc,
            long entropyMask,
            boolean checkOrder
    ) {
        return tryDirectTupleSpaceSort(scratch, dst, startPos, size, sc, entropyMask, checkOrder, false);
    }

    public static boolean tryDirectTupleSpaceSort(
            MemorySegment scratch,
            MemorySegment dst,
            long startPos,
            int size,
            Apex.Scratch sc,
            long entropyMask,
            boolean checkOrder,
            boolean preferInPlace
    ) {
        if (!tupleSpaceFitsDirectPass(entropyMask, size)) {
            return false;
        }

        if (directTupleUsesInPlace(scratch, dst, size, checkOrder, preferInPlace)) {
            directTupleSpaceSortHeap(dst, startPos, size, sc, entropyMask, checkOrder);
        } else {
            directTupleSpaceSortOffHeap(scratch, dst, startPos, size, sc, entropyMask);
        }

        return true;
    }

    public static void directTupleSpaceSortHeap(
            MemorySegment dst,
            long startPos,
            int size,
            Scratch sc,
            long entropyMask
    ) {
        directTupleSpaceSortHeap(dst, startPos, size, sc, entropyMask, true);
    }

    public static void directTupleSpaceSortHeap(
            MemorySegment dst,
            long startPos,
            int size,
            Scratch sc,
            long entropyMask,
            boolean checkOrder
    ) {
        int radix = tupleRadix(entropyMask);
        sc.ensureCounts(radix);
        sc.ensureBucketScratch(radix);

        long base = startPos << 4;
        long p = base;
        int[] counts = sc.counts;
        int[] starts = sc.bucketStarts;
        int[] offsets = sc.bucketOffsets;
        int[] ends = sc.bucketEnds;
        boolean ascending = true;
        boolean descending = true;
        long previous = 0L;
        int tupleShift = tupleShift(entropyMask);
        int tupleMask = radix - 1;
        long keyOrderXor = Apex.KEY_ORDER_XOR;
        Arrays.fill(counts, 0, radix, 0);

        if (tupleShift >= 0) {
            for (int i = 0; i < size; i++) {
                long key = dst.get(Apex.LONG, p);
                counts[(int) (((key ^ keyOrderXor) >>> tupleShift) & tupleMask)]++;
                if (checkOrder && (ascending || descending) && i > 0) {
                    int cmp = tools.compareKeys(previous, key);
                    ascending &= cmp <= 0;
                    descending &= cmp >= 0;
                }
                previous = key;
                p += Apex.RECORD_BYTES;
            }
        } else {
            for (int i = 0; i < size; i++) {
                long key = dst.get(Apex.LONG, p);
                counts[(int) Long.compress(key ^ keyOrderXor, entropyMask)]++;
                if (checkOrder && (ascending || descending) && i > 0) {
                    int cmp = tools.compareKeys(previous, key);
                    ascending &= cmp <= 0;
                    descending &= cmp >= 0;
                }
                previous = key;
                p += Apex.RECORD_BYTES;
            }
        }

        if (checkOrder) {
            if (ascending) {
                return;
            }

            if (descending) {
                tools.reverseRecordsInPlace(dst, base, size);
                return;
            }
        }

        int sum = 0;
        for (int bin = 0; bin < radix; bin++) {
            int count = counts[bin];
            starts[bin] = sum;
            offsets[bin] = sum;
            sum += count;
            ends[bin] = sum;
        }

        if (tupleShift >= 0) {
            for (int bin = 0; bin < radix; bin++) {
                int i = offsets[bin];
                int end = ends[bin];

                while (i < end) {
                    long recordOffset = base + ((long) i << 4);
                    long key = dst.get(Apex.LONG, recordOffset);
                    int targetBin = (int) (((key ^ keyOrderXor) >>> tupleShift) & tupleMask);

                    if (targetBin == bin) {
                        i++;
                        offsets[bin] = i;
                        continue;
                    }

                    int targetIndex = offsets[targetBin]++;
                    long targetOffset = base + ((long) targetIndex << 4);

                    long value = dst.get(Apex.LONG, recordOffset + 8);
                    long targetKey = dst.get(Apex.LONG, targetOffset);
                    long targetValue = dst.get(Apex.LONG, targetOffset + 8);

                    dst.set(Apex.LONG, targetOffset, key);
                    dst.set(Apex.LONG, targetOffset + 8, value);
                    dst.set(Apex.LONG, recordOffset, targetKey);
                    dst.set(Apex.LONG, recordOffset + 8, targetValue);
                }
            }
        } else {
            for (int bin = 0; bin < radix; bin++) {
                int i = offsets[bin];
                int end = ends[bin];

                while (i < end) {
                    long recordOffset = base + ((long) i << 4);
                    long key = dst.get(Apex.LONG, recordOffset);
                    int targetBin = (int) Long.compress(key ^ keyOrderXor, entropyMask);

                    if (targetBin == bin) {
                        i++;
                        offsets[bin] = i;
                        continue;
                    }

                    int targetIndex = offsets[targetBin]++;
                    long targetOffset = base + ((long) targetIndex << 4);

                    long value = dst.get(Apex.LONG, recordOffset + 8);
                    long targetKey = dst.get(Apex.LONG, targetOffset);
                    long targetValue = dst.get(Apex.LONG, targetOffset + 8);

                    dst.set(Apex.LONG, targetOffset, key);
                    dst.set(Apex.LONG, targetOffset + 8, value);
                    dst.set(Apex.LONG, recordOffset, targetKey);
                    dst.set(Apex.LONG, recordOffset + 8, targetValue);
                }
            }
        }
    }

    public static void directTupleSpaceSortOffHeap(
            MemorySegment scratch,
            MemorySegment dst,
            long startPos,
            int size,
            Scratch sc,
            long entropyMask
    ) {
        try {
            Apex.LARGE_PARTITION_PERMITS.acquire();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        }

        try {
            int radix = tupleRadix(entropyMask);
            long dstBase = startPos << 4;
            int tupleShift = tupleShift(entropyMask);

            try {
                parallelTupleCountingPassSegments(
                        dst,
                        dstBase,
                        scratch,
                        dstBase,
                        size,
                        tupleShift,
                        radix - 1,
                        entropyMask
                );
            } catch (Exception e) {
                throw new RuntimeException("Parallel direct tuple off-heap pass failed", e);
            }

            try {
                tools.parallelBulkCopy(scratch, dstBase, dst, dstBase, size);
            } catch (Exception e) {
                throw new RuntimeException("Parallel off-heap blit failed", e);
            }
        } finally {
            Apex.LARGE_PARTITION_PERMITS.release();
        }
    }    

    public static int buildPackedTupleCyclePlan(
            long variableMask,
            int bitsPerCycle,
            int[] cycleShifts,
            int[] cycleMasks,
            long[] cycleBitMasks
    ) {
        int cycles = 0;
        int bitsInCycle = 0;
        long bitMask = 0L;
        int cycleBits = Math.max(1, Math.min(Apex.MAX_DIRECT_TUPLE_BITS, bitsPerCycle));

        while (variableMask != 0L) {
            long bit = variableMask & -variableMask;
            variableMask ^= bit;
            bitMask |= bit;
            bitsInCycle++;

            if (bitsInCycle == cycleBits || variableMask == 0L) {
                cycleBitMasks[cycles] = bitMask;
                cycleMasks[cycles] = tools.lowIntMask(bitsInCycle);
                cycleShifts[cycles] = contiguousShift(bitMask);
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
            long entropyMask
    ) {
        int radix = tuples.tupleRadix(entropyMask);
        sc.ensureCounts(radix);
        Arrays.fill(sc.counts, 0, radix, 0);
        int tupleShift = tupleShift(entropyMask);
        int tupleMask = radix - 1;
        long keyOrderXor = Apex.KEY_ORDER_XOR;

        if (tupleShift >= 0) {
            for (int i = 0; i < size; i++) {
                int bin = (int) (((currentKeys[i] ^ keyOrderXor) >>> tupleShift) & tupleMask);
                sc.counts[bin]++;
            }
        } else {
            for (int i = 0; i < size; i++) {
                int bin = (int) Long.compress(currentKeys[i] ^ keyOrderXor, entropyMask);
                sc.counts[bin]++;
            }
        }

        int sum = 0;
        for (int i = 0; i < radix; i++) {
            int c = sc.counts[i];
            sc.counts[i] = sum;
            sum += c;
        }

        if (tupleShift >= 0) {
            for (int i = 0; i < size; i++) {
                long k = currentKeys[i];
                int bin = (int) (((k ^ keyOrderXor) >>> tupleShift) & tupleMask);
                int pos = sc.counts[bin]++;
                nextKeys[pos] = k;
                nextValues[pos] = currentValues[i];
            }
        } else {
            for (int i = 0; i < size; i++) {
                long k = currentKeys[i];
                int bin = (int) Long.compress(k ^ keyOrderXor, entropyMask);
                int pos = sc.counts[bin]++;
                nextKeys[pos] = k;
                nextValues[pos] = currentValues[i];
            }
        }
    }

    public static void tupleCountingPassSegments(
            MemorySegment source,
            long sourceBase,
            MemorySegment target,
            long targetBase,
            int size,
            int[] counts,
            long entropyMask
    ) {
        tupleCountingPassSegments(source, sourceBase, target, targetBase, size, counts,
                tupleShift(entropyMask), tuples.tupleRadix(entropyMask) - 1, entropyMask);
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
            long bitMask
    ) {
        int radixThisPass = mask + 1;
        Arrays.fill(counts, 0, radixThisPass, 0);

        long p = sourceBase;
        long end = sourceBase + ((long) size << 4);
        long unrolledEnd = end - (4L * Apex.RECORD_BYTES);
        long keyOrderXor = Apex.KEY_ORDER_XOR;

        // --- 🚀 Stride-Unrolled Counting Staged to Saturate L1 Caches ---
        if (shift >= 0) {
            while (p <= unrolledEnd) {
                long k0 = source.get(Apex.LONG, p);
                long k1 = source.get(Apex.LONG, p + 16);
                long k2 = source.get(Apex.LONG, p + 32);
                long k3 = source.get(Apex.LONG, p + 48);

                counts[(int) (((k0 ^ keyOrderXor) >>> shift) & mask)]++;
                counts[(int) (((k1 ^ keyOrderXor) >>> shift) & mask)]++;
                counts[(int) (((k2 ^ keyOrderXor) >>> shift) & mask)]++;
                counts[(int) (((k3 ^ keyOrderXor) >>> shift) & mask)]++;

                p += 4L * Apex.RECORD_BYTES;
            }

            while (p < end) {
                long k = source.get(Apex.LONG, p);
                counts[(int) (((k ^ keyOrderXor) >>> shift) & mask)]++;
                p += Apex.RECORD_BYTES;
            }
        } else {
            while (p <= unrolledEnd) {
                long k0 = source.get(Apex.LONG, p);
                long k1 = source.get(Apex.LONG, p + 16);
                long k2 = source.get(Apex.LONG, p + 32);
                long k3 = source.get(Apex.LONG, p + 48);

                counts[(int) Long.compress(k0 ^ keyOrderXor, bitMask)]++;
                counts[(int) Long.compress(k1 ^ keyOrderXor, bitMask)]++;
                counts[(int) Long.compress(k2 ^ keyOrderXor, bitMask)]++;
                counts[(int) Long.compress(k3 ^ keyOrderXor, bitMask)]++;

                p += 4L * Apex.RECORD_BYTES;
            }

            while (p < end) {
                long k = source.get(Apex.LONG, p);
                counts[(int) Long.compress(k ^ keyOrderXor, bitMask)]++;
                p += Apex.RECORD_BYTES;
            }
        }

        int sum = 0;
        for (int i = 0; i < radixThisPass; i++) {
            int c = counts[i];
            counts[i] = sum;
            sum += c;
        }

        p = sourceBase;

        // --- 🚀 Stride-Unrolled Writeback Shuffling Symmetrically ---
        if (shift >= 0) {
            while (p <= unrolledEnd) {
                long k0 = source.get(Apex.LONG, p);
                long v0 = source.get(Apex.LONG, p + 8);
                long k1 = source.get(Apex.LONG, p + 16);
                long v1 = source.get(Apex.LONG, p + 24);
                long k2 = source.get(Apex.LONG, p + 32);
                long v2 = source.get(Apex.LONG, p + 40);
                long k3 = source.get(Apex.LONG, p + 48);
                long v3 = source.get(Apex.LONG, p + 56);

                int bin0 = (int) (((k0 ^ keyOrderXor) >>> shift) & mask);
                long targetPos0 = targetBase + ((long) counts[bin0]++ << 4);
                target.set(Apex.LONG, targetPos0, k0);
                target.set(Apex.LONG, targetPos0 + 8, v0);

                int bin1 = (int) (((k1 ^ keyOrderXor) >>> shift) & mask);
                long targetPos1 = targetBase + ((long) counts[bin1]++ << 4);
                target.set(Apex.LONG, targetPos1, k1);
                target.set(Apex.LONG, targetPos1 + 8, v1);

                int bin2 = (int) (((k2 ^ keyOrderXor) >>> shift) & mask);
                long targetPos2 = targetBase + ((long) counts[bin2]++ << 4);
                target.set(Apex.LONG, targetPos2, k2);
                target.set(Apex.LONG, targetPos2 + 8, v2);

                int bin3 = (int) (((k3 ^ keyOrderXor) >>> shift) & mask);
                long targetPos3 = targetBase + ((long) counts[bin3]++ << 4);
                target.set(Apex.LONG, targetPos3, k3);
                target.set(Apex.LONG, targetPos3 + 8, v3);

                p += 4L * Apex.RECORD_BYTES;
            }

            while (p < end) {
                long k = source.get(Apex.LONG, p);
                long v = source.get(Apex.LONG, p + 8);

                int bin = (int) (((k ^ keyOrderXor) >>> shift) & mask);
                long targetPos = targetBase + ((long) counts[bin]++ << 4);

                target.set(Apex.LONG, targetPos, k);
                target.set(Apex.LONG, targetPos + 8, v);
                p += Apex.RECORD_BYTES;
            }
        } else {
            while (p <= unrolledEnd) {
                long k0 = source.get(Apex.LONG, p);
                long v0 = source.get(Apex.LONG, p + 8);
                long k1 = source.get(Apex.LONG, p + 16);
                long v1 = source.get(Apex.LONG, p + 24);
                long k2 = source.get(Apex.LONG, p + 32);
                long v2 = source.get(Apex.LONG, p + 40);
                long k3 = source.get(Apex.LONG, p + 48);
                long v3 = source.get(Apex.LONG, p + 56);

                int bin0 = (int) Long.compress(k0 ^ keyOrderXor, bitMask);
                long targetPos0 = targetBase + ((long) counts[bin0]++ << 4);
                target.set(Apex.LONG, targetPos0, k0);
                target.set(Apex.LONG, targetPos0 + 8, v0);

                int bin1 = (int) Long.compress(k1 ^ keyOrderXor, bitMask);
                long targetPos1 = targetBase + ((long) counts[bin1]++ << 4);
                target.set(Apex.LONG, targetPos1, k1);
                target.set(Apex.LONG, targetPos1 + 8, v1);

                int bin2 = (int) Long.compress(k2 ^ keyOrderXor, bitMask);
                long targetPos2 = targetBase + ((long) counts[bin2]++ << 4);
                target.set(Apex.LONG, targetPos2, k2);
                target.set(Apex.LONG, targetPos2 + 8, v2);

                int bin3 = (int) Long.compress(k3 ^ keyOrderXor, bitMask);
                long targetPos3 = targetBase + ((long) counts[bin3]++ << 4);
                target.set(Apex.LONG, targetPos3, k3);
                target.set(Apex.LONG, targetPos3 + 8, v3);

                p += 4L * Apex.RECORD_BYTES;
            }

            while (p < end) {
                long k = source.get(Apex.LONG, p);
                long v = source.get(Apex.LONG, p + 8);

                int bin = (int) Long.compress(k ^ keyOrderXor, bitMask);
                long targetPos = targetBase + ((long) counts[bin]++ << 4);

                target.set(Apex.LONG, targetPos, k);
                target.set(Apex.LONG, targetPos + 8, v);
                p += Apex.RECORD_BYTES;
            }
        }
    }

    public static void parallelTupleCountingPassSegments(
            MemorySegment source,
            long sourceBase,
            MemorySegment target,
            long targetBase,
            int size,
            int shift,
            int mask,
            long bitMask
    ) throws Exception {
        int radixThisPass = mask + 1;
        int[][] threadOffsets = new int[Apex.THREADS][radixThisPass];
        ArrayList<Future<?>> futures = new ArrayList<>(Apex.THREADS);
        int chunk = size / Apex.THREADS;

        for (int t = 0; t < Apex.THREADS; t++) {
            final int tid = t;
            futures.add(Apex.POOL.submit(() -> {
                int[] counts = threadOffsets[tid];
                int start = tid * chunk;
                int end = (tid == Apex.THREADS - 1) ? size : start + chunk;
                long p = sourceBase + ((long) start << 4);
                long limit = sourceBase + ((long) end << 4);
                long unrolledLimit = limit - (4L * Apex.RECORD_BYTES);
                long keyOrderXor = Apex.KEY_ORDER_XOR;

                if (shift >= 0) {
                    while (p <= unrolledLimit) {
                        long k0 = source.get(Apex.LONG, p);
                        long k1 = source.get(Apex.LONG, p + 16);
                        long k2 = source.get(Apex.LONG, p + 32);
                        long k3 = source.get(Apex.LONG, p + 48);

                        counts[(int) (((k0 ^ keyOrderXor) >>> shift) & mask)]++;
                        counts[(int) (((k1 ^ keyOrderXor) >>> shift) & mask)]++;
                        counts[(int) (((k2 ^ keyOrderXor) >>> shift) & mask)]++;
                        counts[(int) (((k3 ^ keyOrderXor) >>> shift) & mask)]++;
                        p += 4L * Apex.RECORD_BYTES;
                    }

                    while (p < limit) {
                        long k = source.get(Apex.LONG, p);
                        counts[(int) (((k ^ keyOrderXor) >>> shift) & mask)]++;
                        p += Apex.RECORD_BYTES;
                    }
                } else {
                    while (p <= unrolledLimit) {
                        long k0 = source.get(Apex.LONG, p);
                        long k1 = source.get(Apex.LONG, p + 16);
                        long k2 = source.get(Apex.LONG, p + 32);
                        long k3 = source.get(Apex.LONG, p + 48);

                        counts[(int) Long.compress(k0 ^ keyOrderXor, bitMask)]++;
                        counts[(int) Long.compress(k1 ^ keyOrderXor, bitMask)]++;
                        counts[(int) Long.compress(k2 ^ keyOrderXor, bitMask)]++;
                        counts[(int) Long.compress(k3 ^ keyOrderXor, bitMask)]++;
                        p += 4L * Apex.RECORD_BYTES;
                    }

                    while (p < limit) {
                        long k = source.get(Apex.LONG, p);
                        counts[(int) Long.compress(k ^ keyOrderXor, bitMask)]++;
                        p += Apex.RECORD_BYTES;
                    }
                }
            }));
        }

        tools.waitForFutures(futures);

        int sum = 0;
        for (int bin = 0; bin < radixThisPass; bin++) {
            int offset = sum;

            for (int t = 0; t < Apex.THREADS; t++) {
                int count = threadOffsets[t][bin];
                threadOffsets[t][bin] = offset;
                offset += count;
            }

            sum = offset;
        }

        futures.clear();

        for (int t = 0; t < Apex.THREADS; t++) {
            final int tid = t;
            futures.add(Apex.POOL.submit(() -> {
                int[] offsets = threadOffsets[tid];
                int start = tid * chunk;
                int end = (tid == Apex.THREADS - 1) ? size : start + chunk;
                long p = sourceBase + ((long) start << 4);
                long limit = sourceBase + ((long) end << 4);
                long unrolledLimit = limit - (4L * Apex.RECORD_BYTES);
                long keyOrderXor = Apex.KEY_ORDER_XOR;

                if (shift >= 0) {
                    while (p <= unrolledLimit) {
                        long k0 = source.get(Apex.LONG, p);
                        long v0 = source.get(Apex.LONG, p + 8);
                        long k1 = source.get(Apex.LONG, p + 16);
                        long v1 = source.get(Apex.LONG, p + 24);
                        long k2 = source.get(Apex.LONG, p + 32);
                        long v2 = source.get(Apex.LONG, p + 40);
                        long k3 = source.get(Apex.LONG, p + 48);
                        long v3 = source.get(Apex.LONG, p + 56);

                        int bin0 = (int) (((k0 ^ keyOrderXor) >>> shift) & mask);
                        long targetPos0 = targetBase + ((long) offsets[bin0]++ << 4);
                        target.set(Apex.LONG, targetPos0, k0);
                        target.set(Apex.LONG, targetPos0 + 8, v0);

                        int bin1 = (int) (((k1 ^ keyOrderXor) >>> shift) & mask);
                        long targetPos1 = targetBase + ((long) offsets[bin1]++ << 4);
                        target.set(Apex.LONG, targetPos1, k1);
                        target.set(Apex.LONG, targetPos1 + 8, v1);

                        int bin2 = (int) (((k2 ^ keyOrderXor) >>> shift) & mask);
                        long targetPos2 = targetBase + ((long) offsets[bin2]++ << 4);
                        target.set(Apex.LONG, targetPos2, k2);
                        target.set(Apex.LONG, targetPos2 + 8, v2);

                        int bin3 = (int) (((k3 ^ keyOrderXor) >>> shift) & mask);
                        long targetPos3 = targetBase + ((long) offsets[bin3]++ << 4);
                        target.set(Apex.LONG, targetPos3, k3);
                        target.set(Apex.LONG, targetPos3 + 8, v3);
                        p += 4L * Apex.RECORD_BYTES;
                    }

                    while (p < limit) {
                        long k = source.get(Apex.LONG, p);
                        long v = source.get(Apex.LONG, p + 8);
                        int bin = (int) (((k ^ keyOrderXor) >>> shift) & mask);
                        long targetPos = targetBase + ((long) offsets[bin]++ << 4);
                        target.set(Apex.LONG, targetPos, k);
                        target.set(Apex.LONG, targetPos + 8, v);
                        p += Apex.RECORD_BYTES;
                    }
                } else {
                    while (p <= unrolledLimit) {
                        long k0 = source.get(Apex.LONG, p);
                        long v0 = source.get(Apex.LONG, p + 8);
                        long k1 = source.get(Apex.LONG, p + 16);
                        long v1 = source.get(Apex.LONG, p + 24);
                        long k2 = source.get(Apex.LONG, p + 32);
                        long v2 = source.get(Apex.LONG, p + 40);
                        long k3 = source.get(Apex.LONG, p + 48);
                        long v3 = source.get(Apex.LONG, p + 56);

                        int bin0 = (int) Long.compress(k0 ^ keyOrderXor, bitMask);
                        long targetPos0 = targetBase + ((long) offsets[bin0]++ << 4);
                        target.set(Apex.LONG, targetPos0, k0);
                        target.set(Apex.LONG, targetPos0 + 8, v0);

                        int bin1 = (int) Long.compress(k1 ^ keyOrderXor, bitMask);
                        long targetPos1 = targetBase + ((long) offsets[bin1]++ << 4);
                        target.set(Apex.LONG, targetPos1, k1);
                        target.set(Apex.LONG, targetPos1 + 8, v1);

                        int bin2 = (int) Long.compress(k2 ^ keyOrderXor, bitMask);
                        long targetPos2 = targetBase + ((long) offsets[bin2]++ << 4);
                        target.set(Apex.LONG, targetPos2, k2);
                        target.set(Apex.LONG, targetPos2 + 8, v2);

                        int bin3 = (int) Long.compress(k3 ^ keyOrderXor, bitMask);
                        long targetPos3 = targetBase + ((long) offsets[bin3]++ << 4);
                        target.set(Apex.LONG, targetPos3, k3);
                        target.set(Apex.LONG, targetPos3 + 8, v3);
                        p += 4L * Apex.RECORD_BYTES;
                    }

                    while (p < limit) {
                        long k = source.get(Apex.LONG, p);
                        long v = source.get(Apex.LONG, p + 8);
                        int bin = (int) Long.compress(k ^ keyOrderXor, bitMask);
                        long targetPos = targetBase + ((long) offsets[bin]++ << 4);
                        target.set(Apex.LONG, targetPos, k);
                        target.set(Apex.LONG, targetPos + 8, v);
                        p += Apex.RECORD_BYTES;
                    }
                }
            }));
        }

        tools.waitForFutures(futures);
    }
}
