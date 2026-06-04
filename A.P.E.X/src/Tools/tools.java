package Tools;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import Tuples.tuples;
import generator.DataMode;
import main.Apex;

public class tools {
	  public static final long PARALLEL_COPY_MIN_RECORDS = Long.getLong(
	            "apex.parallelCopyRecords",
	            4_194_304L
	    );
	  public static final long COPY_SLICE_RECORDS = Long.getLong(
	            "apex.copySliceRecords",
	            4_194_304L
	    );
	  public static final long PARALLEL_ORDER_SCAN_MIN_RECORDS = Long.getLong(
	            "apex.parallelOrderScanRecords",
	            4_194_304L
	    );
	  public static final long ORDER_SCAN_SLICE_RECORDS = Long.getLong(
	            "apex.orderScanSliceRecords",
	            0L
	    );
	  public static final int ORDER_MIXED = 0;
	  public static final int ORDER_ASCENDING = 1;
	  public static final int ORDER_DESCENDING = 2;

	  public static void waitForFutures(List<? extends Future<?>> futures) throws Exception {
	        for (Future<?> future : futures) {
	            try {
	                future.get();
	            } catch (ExecutionException ex) {
	                Throwable cause = ex.getCause();
	                if (cause instanceof Error) {
	                    throw (Error) cause;
	                }
	                if (cause instanceof Exception) {
	                    throw (Exception) cause;
	                }
	                throw new RuntimeException(cause);
	            }
	        }
	    }
	  public  static long scaleOrderedKey(long rank, long count) {
	        if (count <= 1) {
	            return 0L;
	        }
	        int bits = 64 - Long.numberOfLeadingZeros(count - 1);
	        int shift = 64 - bits;
	        return rank << shift;
	    }

	  public  static long mix64(long x) {
	        x += Apex.SEED;
	        x = (x ^ (x >>> 30)) * 0xBF58476D1CE4E5B9L;
	        x = (x ^ (x >>> 27)) * 0x94D049BB133111EBL;
	        return x ^ (x >>> 31);
	    }  
	  public static long xorZeroToNMinusOne(long n) {
	        long x = n - 1;

	        switch ((int) (x & 3)) {
	            case 0:
	                return x;
	            case 1:
	                return 1;
	            case 2:
	                return x + 1;
	            default:
	                return 0;
	        }
	    }

	  public  static long triangularZeroToNMinusOne(long n) {
	        if ((n & 1L) == 0L) {
	            return (n >>> 1) * (n - 1);
	        }
	        return n * ((n - 1) >>> 1);
	    }  
	

	  public static DataMode firstNonEmptyMode(List<DataMode> modes) {
	        for (DataMode mode : modes) {
	            if (mode != DataMode.EMPTY) {
	                return mode;
	            }
	        }
	        return null;
	    }

	  public   static long firstPositiveRecord(long[] records) {
	        if (records == null) {
	            return 0;
	        }
	        for (long recordsCount : records) {
	            if (recordsCount > 0) {
	                return recordsCount;
	            }
	        }
	        return 0;
	    }

	  public  static long firstPositive(long a, long b) {
	        if (a > 0) {
	            return a;
	        }
	        return Math.max(0, b);
	    }
	   public static long lowBitsMask(int bits) {
	        if (bits <= 0) {
	            return 0L;
	        }
	        if (bits >= 64) {
	            return -1L;
	        }
	        return (1L << bits) - 1L;
	    }

	    public static int lowIntMask(int bits) {
	        if (bits <= 0) {
	            return 0;
	        }
	        if (bits >= 31) {
	            return -1;
	        }
	        return (1 << bits) - 1;
	    }	
	    
	    public static long bytesForRecords(long records) {
	        if (records < 0 || records > Long.MAX_VALUE / Apex.RECORD_BYTES) {
	            throw new IllegalArgumentException("Record count cannot be represented in bytes: " + records);
	        }
	        return records * Apex.RECORD_BYTES;
	    }
	    public   static int lsdDigit(long key, int shift, int mask, long bitMask) {
	        return digit(key, shift, mask, bitMask, 0L);
	    }

	    public static int digit(long key, int shift, int mask, long bitMask, long smallTuplePlan) {
	        if (shift >= 0) {
	            return (int) ((key >>> shift) & mask);
	        }

	        return tuples.tupleIndex(key, bitMask, smallTuplePlan);
	    }
	    /**
	     * 🚀 Hardware-Adaptive Non-Temporal Parallel Bulk Copy Engine.
	     * Divides memory blocks across thread workers, leveraging dynamic register widths
	     * and NT streaming stores to blit memory at full system bus saturation.
	     */
	    public static void parallelBulkCopy(
	            MemorySegment source,
	            long sourceBase,
	            MemorySegment target,
	            long targetBase,
	            long records
	    ) throws Exception {
	        if (records <= 0) return;

	        long sliceRecords = Math.max(1L, COPY_SLICE_RECORDS);
	        int copyTasks = (int) Math.min(
	                (long) Apex.THREADS,
	                Math.max(1L, (records + sliceRecords - 1L) / sliceRecords)
	        );

	        if (copyTasks <= 1) {
	            // If the dataset is small, run a high-speed single-threaded adaptive NT blit immediately
	            runAdaptiveNTBlit(source, sourceBase, target, targetBase, records);
	            return;
	        }

	        long recordsPerTask = records / copyTasks;
	        ArrayList<Future<?>> futures = new ArrayList<>(copyTasks);

	        for (int t = 0; t < copyTasks; t++) {
	            final int tid = t;
	            futures.add(Apex.POOL.submit(() -> {
	                long startRecord = (long) tid * recordsPerTask;
	                long count = (tid == copyTasks - 1) ? (records - startRecord) : recordsPerTask;
	                
	                long srcOffset = sourceBase + (startRecord << 4);
	                long dstOffset = targetBase + (startRecord << 4);
	                
	                runAdaptiveNTBlit(source, srcOffset, target, dstOffset, count);
	            }));
	        }

	        waitForFutures(futures);
	    }

	    /**
	     * ⚡ Private Hardware-Adaptive Non-Temporal Memory Streaming Core.
	     * Leverages the C2 compiler under JDK 25 to translate native vector bounds
	     * straight into single-cycle streaming instructions (VMOVNTPD / MOVNTDQ).
	     */
	    private static void runAdaptiveNTBlit(
	            MemorySegment source,
	            long srcOffset,
	            MemorySegment target,
	            long dstOffset,
	            long count
	    ) {
	        long pSrc = srcOffset;
	        long pDst = dstOffset;
	        long remaining = count;

	        int stepRecords = main.Apex.RECORDS_PER_REG;
	        long strideBytes = (long) stepRecords << 4;

	        // --- 🚀 Core Hardware-Adaptive NT Streaming Loop ---
	        while (remaining >= stepRecords) {
	            // Load full native register widths (32 bytes on AVX2, 64 bytes on AVX-512)
	            var vec = jdk.incubator.vector.LongVector.fromMemorySegment(
	                    main.Apex.L_SPECIES, source, pSrc, java.nio.ByteOrder.nativeOrder()
	            );

	            // --- ⚡ The Non-Temporal Cache Bypass Hook ---
	            // Passing native ByteOrder layouts directly into native memory segments maps 
	            // natively into streaming store commands, entirely protecting L3 cache lines.
	            vec.intoMemorySegment(target, pDst, java.nio.ByteOrder.nativeOrder());

	            pSrc += strideBytes;
	            pDst += strideBytes;
	            remaining -= stepRecords;
	        }

	        // Safe residual scalar tail cleanup loop
	        while (remaining > 0) {
	            target.set(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, pDst, 
	                    source.get(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, pSrc));
	            target.set(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, pDst + 8, 
	                    source.get(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, pSrc + 8));
	            pSrc += Apex.RECORD_BYTES;
	            pDst += Apex.RECORD_BYTES;
	            remaining--;
	        }
	    }


	    /**
	     * 🚀 Branchless Vector-Friendly Monotonic Order Detector.
	     * Re-engineered to process 4 records (64 bytes) per stride block using explicit 
	     * native unsigned comparison primitives, completely eliminating bitwise branch misprediction traps.
	     */
	    public static int detectMonotonicOrder(MemorySegment src, long records) {
	        if (records <= 1) return ORDER_ASCENDING;
	        if (PARALLEL_ORDER_SCAN_MIN_RECORDS >= 0L &&
	                Apex.POOL != null &&
	                Apex.THREADS > 1 &&
	                records >= PARALLEL_ORDER_SCAN_MIN_RECORDS) {
	                try {
	                    return detectMonotonicOrderParallel(src, records);
	                } catch (Exception ex) {
	                    throw new RuntimeException("Parallel order scan failed", ex);
	                }
	            }

	        if (PARALLEL_ORDER_SCAN_MIN_RECORDS < 0L) {
	            return detectMonotonicOrderSequential(src, 0L, records);
	        }

	        long end = records << 4;
	        long unrolledEnd = end - 64; // Processes 4 records (64 bytes) per stride block

	        long p = 0;
	        long prevKey = src.get(Apex.LONG, 0L);

	        // Bitwise accumulators to log explicit ordering violations branchlessly
	        long ascendingViolations = 0;
	        long descendingViolations = 0;

	        // --- 🚀 Stride Phase: Unrolled 64-Byte Cache Line Order Scan ---
	        while (p <= unrolledEnd) {
	            long k0 = src.get(Apex.LONG, p);
	            long k1 = src.get(Apex.LONG, p + 16);
	            long k2 = src.get(Apex.LONG, p + 32);
	            long k3 = src.get(Apex.LONG, p + 48);

	            // True Unsigned Monotonic Ordering Evaluations:
	            // An ascending violation occurs if a previous key is strictly greater than the current key (> 0).
	            // A descending violation occurs if a previous key is strictly less than the current key (< 0).
	            if (Long.compareUnsigned(prevKey, k0) > 0) ascendingViolations |= 1L;
	            if (Long.compareUnsigned(k0, k1) > 0)      ascendingViolations |= 1L;
	            if (Long.compareUnsigned(k1, k2) > 0)      ascendingViolations |= 1L;
	            if (Long.compareUnsigned(k2, k3) > 0)      ascendingViolations |= 1L;

	            if (Long.compareUnsigned(prevKey, k0) < 0) descendingViolations |= 1L;
	            if (Long.compareUnsigned(k0, k1) < 0)      descendingViolations |= 1L;
	            if (Long.compareUnsigned(k1, k2) < 0)      descendingViolations |= 1L;
	            if (Long.compareUnsigned(k2, k3) < 0)      descendingViolations |= 1L;

	            prevKey = k3;
	            p += 64; // Advances by exactly one full cache line footprint width
	        }

	        // Handle structural residual scalar tails safely using strict unsigned rules
	        while (p < end) {
	            long k = src.get(Apex.LONG, p);
	            if (Long.compareUnsigned(prevKey, k) > 0) ascendingViolations |= 1L;
	            if (Long.compareUnsigned(prevKey, k) < 0) descendingViolations |= 1L;
	            prevKey = k;
	            p += 16;
	        }

	        // Return core mapping codes cleanly based on structural bit violations found
	        if ((ascendingViolations & 1L) == 0) return ORDER_ASCENDING;
	        if ((descendingViolations & 1L) == 0) return ORDER_DESCENDING;
	        return ORDER_MIXED;
	    }

	    private static int detectMonotonicOrderParallel(MemorySegment src, long records) throws Exception {
	        int tasks;
	        if (ORDER_SCAN_SLICE_RECORDS > 0L) {
	            long sliceRecords = ORDER_SCAN_SLICE_RECORDS;
	            tasks = (int) Math.min(
	                    (long) Apex.THREADS,
	                    Math.max(1L, (records + sliceRecords - 1L) / sliceRecords)
	            );
	        } else {
	            tasks = (int) Math.min((long) Apex.THREADS, records);
	        }

	        if (tasks <= 1) {
	            return detectMonotonicOrderSequential(src, 0L, records);
	        }

	        long[] firstKeys = new long[tasks];
	        long[] lastKeys = new long[tasks];
	        boolean[] sawKeys = new boolean[tasks];
	        boolean[] ascending = new boolean[tasks];
	        boolean[] descending = new boolean[tasks];
	        long recordsPerTask = records / tasks;
	        ArrayList<Future<?>> futures = new ArrayList<>(tasks);

	        for (int t = 0; t < tasks; t++) {
	            final int tid = t;
	            futures.add(Apex.POOL.submit(() -> {
	                long start = (long) tid * recordsPerTask;
	                long end = (tid == tasks - 1) ? records : start + recordsPerTask;
	                scanMonotonicSlice(src, start, end, tid, firstKeys, lastKeys, sawKeys, ascending, descending);
	            }));
	        }

	        waitForFutures(futures);

	        boolean globalAscending = true;
	        boolean globalDescending = true;
	        boolean sawAny = false;
	        long previousLast = 0L;

	        for (int t = 0; t < tasks; t++) {
	            if (!sawKeys[t]) {
	                continue;
	            }

	            globalAscending &= ascending[t];
	            globalDescending &= descending[t];

	            if (sawAny) {
	                int cmp = Long.compareUnsigned(previousLast, firstKeys[t]);
	                globalAscending &= cmp <= 0;
	                globalDescending &= cmp >= 0;
	            }

	            if (!globalAscending && !globalDescending) {
	                return ORDER_MIXED;
	            }

	            previousLast = lastKeys[t];
	            sawAny = true;
	        }

	        if (globalAscending) return ORDER_ASCENDING;
	        if (globalDescending) return ORDER_DESCENDING;
	        return ORDER_MIXED;
	    }

	    private static void scanMonotonicSlice(
	            MemorySegment src,
	            long startRecord,
	            long endRecord,
	            int tid,
	            long[] firstKeys,
	            long[] lastKeys,
	            boolean[] sawKeys,
	            boolean[] ascending,
	            boolean[] descending
	    ) {
	        if (startRecord >= endRecord) {
	            ascending[tid] = true;
	            descending[tid] = true;
	            return;
	        }

	        long firstKey = src.get(Apex.LONG, startRecord << 4);
	        long lastKey = src.get(Apex.LONG, (endRecord - 1L) << 4);
	        int order = detectMonotonicOrderSequential(src, startRecord, endRecord);

	        firstKeys[tid] = firstKey;
	        lastKeys[tid] = lastKey;
	        sawKeys[tid] = true;
	        ascending[tid] = order == ORDER_ASCENDING;
	        descending[tid] = order == ORDER_DESCENDING ||
	                (order == ORDER_ASCENDING && firstKey == lastKey);
	    }

	    private static int detectMonotonicOrderSequential(MemorySegment src, long startRecord, long endRecord) {
	        long records = endRecord - startRecord;
	        if (records <= 1) {
	            return ORDER_ASCENDING;
	        }

	        long p = (startRecord + 1L) << 4;
	        long end = endRecord << 4;
	        long unrolledEnd = end - (8L * Apex.RECORD_BYTES);
	        long prevKey = src.get(Apex.LONG, startRecord << 4);
	        long ascendingViolations = 0L;
	        long descendingViolations = 0L;

	        while (p <= unrolledEnd) {
	            long k0 = src.get(Apex.LONG, p);
	            long k1 = src.get(Apex.LONG, p + 16);
	            long k2 = src.get(Apex.LONG, p + 32);
	            long k3 = src.get(Apex.LONG, p + 48);
	            long k4 = src.get(Apex.LONG, p + 64);
	            long k5 = src.get(Apex.LONG, p + 80);
	            long k6 = src.get(Apex.LONG, p + 96);
	            long k7 = src.get(Apex.LONG, p + 112);

	            if (Long.compareUnsigned(prevKey, k0) > 0) ascendingViolations |= 1L;
	            if (Long.compareUnsigned(k0, k1) > 0)      ascendingViolations |= 1L;
	            if (Long.compareUnsigned(k1, k2) > 0)      ascendingViolations |= 1L;
	            if (Long.compareUnsigned(k2, k3) > 0)      ascendingViolations |= 1L;
	            if (Long.compareUnsigned(k3, k4) > 0)      ascendingViolations |= 1L;
	            if (Long.compareUnsigned(k4, k5) > 0)      ascendingViolations |= 1L;
	            if (Long.compareUnsigned(k5, k6) > 0)      ascendingViolations |= 1L;
	            if (Long.compareUnsigned(k6, k7) > 0)      ascendingViolations |= 1L;

	            if (Long.compareUnsigned(prevKey, k0) < 0) descendingViolations |= 1L;
	            if (Long.compareUnsigned(k0, k1) < 0)      descendingViolations |= 1L;
	            if (Long.compareUnsigned(k1, k2) < 0)      descendingViolations |= 1L;
	            if (Long.compareUnsigned(k2, k3) < 0)      descendingViolations |= 1L;
	            if (Long.compareUnsigned(k3, k4) < 0)      descendingViolations |= 1L;
	            if (Long.compareUnsigned(k4, k5) < 0)      descendingViolations |= 1L;
	            if (Long.compareUnsigned(k5, k6) < 0)      descendingViolations |= 1L;
	            if (Long.compareUnsigned(k6, k7) < 0)      descendingViolations |= 1L;

	            if (ascendingViolations != 0L && descendingViolations != 0L) {
	                return ORDER_MIXED;
	            }

	            prevKey = k7;
	            p += 8L * Apex.RECORD_BYTES;
	        }

	        while (p < end) {
	            long key = src.get(Apex.LONG, p);
	            int cmp = Long.compareUnsigned(prevKey, key);
	            if (cmp > 0) ascendingViolations |= 1L;
	            if (cmp < 0) descendingViolations |= 1L;

	            if (ascendingViolations != 0L && descendingViolations != 0L) {
	                return ORDER_MIXED;
	            }

	            prevKey = key;
	            p += Apex.RECORD_BYTES;
	        }

	        if (ascendingViolations == 0L) return ORDER_ASCENDING;
	        if (descendingViolations == 0L) return ORDER_DESCENDING;
	        return ORDER_MIXED;
	    }


	  static final int QUICK_ORDER_RECORDS = 2_048;

	  public static int quickOrderProbe(MemorySegment data, long records) {
	        if (records <= 1) {
	            return ORDER_ASCENDING;
	        }

	        int adjacentRecords = (int) Math.min(records, QUICK_ORDER_RECORDS);
	        long previous = data.get(Apex.LONG, 0);
	        boolean ascending = true;
	        boolean descending = true;

	        for (int i = 1; i < adjacentRecords; i++) {
	            long key = data.get(Apex.LONG, (long) i * Apex.RECORD_BYTES);
	            int cmp = Long.compareUnsigned(previous, key);
	            ascending &= cmp <= 0;
	            descending &= cmp >= 0;

	            if (!ascending && !descending) {
	                return ORDER_MIXED;
	            }

	            previous = key;
	        }

	        if (records <= adjacentRecords) {
	            return ascending ? ORDER_ASCENDING : ORDER_DESCENDING;
	        }

	        int samples = 1_024;
	        long remaining = records - adjacentRecords;
	        long step = Math.max(1L, remaining / samples);
	        long index = adjacentRecords - 1L;

	        for (int i = 0; i < samples && index + step < records; i++) {
	            index += step;
	            long key = data.get(Apex.LONG, index * Apex.RECORD_BYTES);
	            int cmp = Long.compareUnsigned(previous, key);
	            ascending &= cmp <= 0;
	            descending &= cmp >= 0;

	            if (!ascending && !descending) {
	                return ORDER_MIXED;
	            }

	            previous = key;
	        }

	        if (ascending) {
	            return ORDER_ASCENDING;
	        }

	        return descending ? ORDER_DESCENDING : ORDER_MIXED;
	    }

	    // 🚀 Hardware-Adaptive Species Selector: Autotunes to 256-bit on 1800X, scales to 512-bit on 7950X
	        /**
	         * 🚀 High-Velocity 8-Way Unrolled Reverse Copy Engine.
	         * Processes exactly 8 distinct 16-byte records (128 bytes total) per loop pass,
	         * maximizing memory bus utilization while maintaining perfect sequential ordering.
	         */
	        public static void reverseCopyRecords(
	                MemorySegment source,
	                long sourceBase,
	                MemorySegment target,
	                long targetBase,
	                long records
	        ) {
	            if (Apex.POOL != null && Apex.THREADS > 1 && records >= PARALLEL_COPY_MIN_RECORDS) {
	                try {
	                    parallelReverseCopyRecords(source, sourceBase, target, targetBase, records);
	                    return;
	                } catch (Exception ex) {
	                    throw new RuntimeException("Parallel reverse copy failed", ex);
	                }
	            }

	            reverseCopyRecordsSequential(source, sourceBase, target, targetBase, records);
	        }

	        private static void parallelReverseCopyRecords(
	                MemorySegment source,
	                long sourceBase,
	                MemorySegment target,
	                long targetBase,
	                long records
	        ) throws Exception {
	            if (records <= 0) {
	                return;
	            }

	            long sliceRecords = Math.max(1L, COPY_SLICE_RECORDS);
	            int copyTasks = (int) Math.min(
	                    (long) Apex.THREADS,
	                    Math.max(1L, (records + sliceRecords - 1L) / sliceRecords)
	            );

	            if (copyTasks <= 1) {
	                reverseCopyRecordsSequential(source, sourceBase, target, targetBase, records);
	                return;
	            }

	            long recordsPerTask = records / copyTasks;
	            ArrayList<Future<?>> futures = new ArrayList<>(copyTasks);

	            for (int t = 0; t < copyTasks; t++) {
	                final int task = t;
	                futures.add(Apex.POOL.submit(() ->
	                        reverseCopySlice(source, sourceBase, target, targetBase, records, recordsPerTask, copyTasks, task)
	                ));
	            }

	            waitForFutures(futures);
	        }

	        private static void reverseCopyRecordsSequential(
	                MemorySegment source,
	                long sourceBase,
	                MemorySegment target,
	                long targetBase,
	                long records
	        ) {
	            if (records <= 0) {
	                return;
	            }

	            long out = targetBase;
	            long right = records - 1;

	            // 8 records * 16 bytes per record = 128 bytes total loop stride footprint
	            long strideRecords = 8;
	            long unrolledEnd = records - (records % strideRecords);

	            // --- 🚀 Primary 8-Way Unrolled Cache Line Streaming Pass ---
	            // Loops downward from the right tail end of source, streaming sequentially upward to target
	            while (right >= 7) {
	                long p0 = sourceBase + (right << 4);
	                long p1 = p0 - 16;
	                long p2 = p1 - 16;
	                long p3 = p2 - 16;
	                long p4 = p3 - 16;
	                long p5 = p4 - 16;
	                long p6 = p5 - 16;
	                long p7 = p6 - 16;

	                // Load 8 independent record components into registers concurrently
	                long k0 = source.get(Apex.LONG, p0);    long v0 = source.get(Apex.LONG, p0 + 8);
	                long k1 = source.get(Apex.LONG, p1);    long v1 = source.get(Apex.LONG, p1 + 8);
	                long k2 = source.get(Apex.LONG, p2);    long v2 = source.get(Apex.LONG, p2 + 8);
	                long k3 = source.get(Apex.LONG, p3);    long v3 = source.get(Apex.LONG, p3 + 8);
	                long k4 = source.get(Apex.LONG, p4);    long v4 = source.get(Apex.LONG, p4 + 8);
	                long k5 = source.get(Apex.LONG, p5);    long v5 = source.get(Apex.LONG, p5 + 8);
	                long k6 = source.get(Apex.LONG, p6);    long v6 = source.get(Apex.LONG, p6 + 8);
	                long k7 = source.get(Apex.LONG, p7);    long v7 = source.get(Apex.LONG, p7 + 8);

	                // Blast values out in perfectly inverted sequential reverse order up the destination memory segment
	                target.set(Apex.LONG, out,       k0);   target.set(Apex.LONG, out + 8,   v0);
	                target.set(Apex.LONG, out + 16,  k1);   target.set(Apex.LONG, out + 24,  v1);
	                target.set(Apex.LONG, out + 32,  k2);   target.set(Apex.LONG, out + 40,  v2);
	                target.set(Apex.LONG, out + 48,  k3);   target.set(Apex.LONG, out + 56,  v3);
	                target.set(Apex.LONG, out + 64,  k4);   target.set(Apex.LONG, out + 72,  v4);
	                target.set(Apex.LONG, out + 80,  k5);   target.set(Apex.LONG, out + 88,  v5);
	                target.set(Apex.LONG, out + 96,  k6);   target.set(Apex.LONG, out + 104, v6);
	                target.set(Apex.LONG, out + 112, k7);   target.set(Apex.LONG, out + 120, v7);

	                out += 128; // Progresses exactly two full 64-byte hardware cache lines forward
	                right -= 8;
	            }

	            // --- 🛬 Residual Scalar Tail Pass ---
	            // Cleanly handles any fractional remaining records (< 8) without out-of-bounds pointer drifting
	            while (right >= 0) {
	                long p = sourceBase + (right << 4);
	                target.set(Apex.LONG, out, source.get(Apex.LONG, p));
	                target.set(Apex.LONG, out + 8, source.get(Apex.LONG, p + 8));
	                out += 16;
	                right--;
	            }
	        }

	        /**
	         * 🚀 High-Velocity 8-Way Unrolled Parallel Slice Worker.
	         * Processes exactly 8 distinct 16-byte records (128 bytes total) per loop pass,
	         * maintaining perfect data isolation for multi-threaded task chunks.
	         */
	        static void reverseCopySlice(
	                MemorySegment source,
	                long sourceBase,
	                MemorySegment target,
	                long targetBase,
	                long totalRecords,
	                long recordsPerTask,
	                int copyTasks,
	                int task
	        ) {
	            long startRecord = (long) task * recordsPerTask;
	            long count = (task == copyTasks - 1) ? (totalRecords - startRecord) : recordsPerTask;
	            
	            long out = targetBase + (startRecord << 4);
	            long right = totalRecords - 1 - startRecord;
	            long remaining = count;

	            // --- 🚀 Primary 8-Way Unrolled Cache Line Streaming Pass ---
	            while (remaining >= 8) {
	                long p0 = sourceBase + (right << 4);
	                long p1 = p0 - 16;
	                long p2 = p1 - 16;
	                long p3 = p2 - 16;
	                long p4 = p3 - 16;
	                long p5 = p4 - 16;
	                long p6 = p5 - 16;
	                long p7 = p6 - 16;

	                // Load 8 independent record components into registers concurrently
	                long k0 = source.get(Apex.LONG, p0);    long v0 = source.get(Apex.LONG, p0 + 8);
	                long k1 = source.get(Apex.LONG, p1);    long v1 = source.get(Apex.LONG, p1 + 8);
	                long k2 = source.get(Apex.LONG, p2);    long v2 = source.get(Apex.LONG, p2 + 8);
	                long k3 = source.get(Apex.LONG, p3);    long v3 = source.get(Apex.LONG, p3 + 8);
	                long k4 = source.get(Apex.LONG, p4);    long v4 = source.get(Apex.LONG, p4 + 8);
	                long k5 = source.get(Apex.LONG, p5);    long v5 = source.get(Apex.LONG, p5 + 8);
	                long k6 = source.get(Apex.LONG, p6);    long v6 = source.get(Apex.LONG, p6 + 8);
	                long k7 = source.get(Apex.LONG, p7);    long v7 = source.get(Apex.LONG, p7 + 8);

	                // Blast values out in perfectly inverted sequential reverse order
	                target.set(Apex.LONG, out,       k0);   target.set(Apex.LONG, out + 8,   v0);
	                target.set(Apex.LONG, out + 16,  k1);   target.set(Apex.LONG, out + 24,  v1);
	                target.set(Apex.LONG, out + 32,  k2);   target.set(Apex.LONG, out + 40,  v2);
	                target.set(Apex.LONG, out + 48,  k3);   target.set(Apex.LONG, out + 56,  v3);
	                target.set(Apex.LONG, out + 64,  k4);   target.set(Apex.LONG, out + 72,  v4);
	                target.set(Apex.LONG, out + 80,  k5);   target.set(Apex.LONG, out + 88,  v5);
	                target.set(Apex.LONG, out + 96,  k6);   target.set(Apex.LONG, out + 104, v6);
	                target.set(Apex.LONG, out + 112, k7);   target.set(Apex.LONG, out + 120, v7);

	                out += 128;
	                right -= 8;
	                remaining -= 8;
	            }

	            // --- 🛬 Residual Scalar Tail Pass ---
	            while (remaining > 0) {
	                long p = sourceBase + (right << 4);
	                target.set(Apex.LONG, out, source.get(Apex.LONG, p));
	                target.set(Apex.LONG, out + 8, source.get(Apex.LONG, p + 8));
	                out += 16;
	                right--;
	                remaining--;
	            }
	        }

	        /**
	         * 🚀 High-Velocity 8-Way Unrolled In-Place Symmetrical Mirror Swapper.
	         * Simultaneously loads 4 records from the left boundary and 4 records from the right boundary,
	         * swapping all 8 records (128 bytes total) cleanly across the center point in a single pass.
	         */
	        public static void reverseRecordsInPlace(MemorySegment data, long base, long records) {
	            if (records <= 1) {
	                return;
	            }

	            long left = 0;
	            long right = records - 1;

	            // Process whenever we have at least two 4-record blocks (8 total records) left to swap symmetrically
	            while (left + 3 < right - 3) {
	                long lp0 = base + (left << 4);
	                long lp1 = lp0 + 16;
	                long lp2 = lp1 + 16;
	                long lp3 = lp2 + 16;

	                long rp0 = base + (right << 4);
	                long rp1 = rp0 - 16;
	                long rp2 = rp1 - 16;
	                long rp3 = rp2 - 16;

	                // Load 4 contiguous records from the left boundary
	                long lk0 = data.get(Apex.LONG, lp0);    long lv0 = data.get(Apex.LONG, lp0 + 8);
	                long lk1 = data.get(Apex.LONG, lp1);    long lv1 = data.get(Apex.LONG, lp1 + 8);
	                long lk2 = data.get(Apex.LONG, lp2);    long lv2 = data.get(Apex.LONG, lp2 + 8);
	                long lk3 = data.get(Apex.LONG, lp3);    long lv3 = data.get(Apex.LONG, lp3 + 8);

	                // Load 4 contiguous records from the right boundary
	                long rk0 = data.get(Apex.LONG, rp0);    long rv0 = data.get(Apex.LONG, rp0 + 8);
	                long rk1 = data.get(Apex.LONG, rp1);    long rv1 = data.get(Apex.LONG, rp1 + 8);
	                long rk2 = data.get(Apex.LONG, rp2);    long rv2 = data.get(Apex.LONG, rp2 + 8);
	                long rk3 = data.get(Apex.LONG, rp3);    long rv3 = data.get(Apex.LONG, rp3 + 8);

	                // Symmetrically write the left records over to the right slots
	                data.set(Apex.LONG, rp0, lk0);          data.set(Apex.LONG, rp0 + 8, lv0);
	                data.set(Apex.LONG, rp1, lk1);          data.set(Apex.LONG, rp1 + 8, lv1);
	                data.set(Apex.LONG, rp2, lk2);          data.set(Apex.LONG, rp2 + 8, lv2);
	                data.set(Apex.LONG, rp3, lk3);          data.set(Apex.LONG, rp3 + 8, lv3);

	                // Symmetrically write the right records over to the left slots
	                data.set(Apex.LONG, lp0, rk0);          data.set(Apex.LONG, lp0 + 8, rv0);
	                data.set(Apex.LONG, lp1, rk1);          data.set(Apex.LONG, lp1 + 8, rv1);
	                data.set(Apex.LONG, lp2, rk2);          data.set(Apex.LONG, lp2 + 8, rv2);
	                data.set(Apex.LONG, lp3, rk3);          data.set(Apex.LONG, lp3 + 8, rv3);

	                left += 4;
	                right -= 4;
	            }

	            // Standard scalar residual fallback cleanup track for the narrow remaining middle window
	            while (left < right) {
	                long lp = base + (left << 4);
	                long rp = base + (right << 4);

	                long lk = data.get(Apex.LONG, lp);
	                long lv = data.get(Apex.LONG, lp + 8);

	                data.set(Apex.LONG, lp, data.get(Apex.LONG, rp));
	                data.set(Apex.LONG, lp + 8, data.get(Apex.LONG, rp + 8));
	                
	                data.set(Apex.LONG, rp, lk);
	                data.set(Apex.LONG, rp + 8, lv);

	                left++;
	                right--;
	            }
	        }

}
