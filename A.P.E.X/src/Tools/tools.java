package Tools;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import Tuples.tuples;
import config.runoptions.Options;
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
	
	/*   public static void bulkCopyRecords(
	            MemorySegment source,
	            long sourceBase,
	            MemorySegment target,
	            long targetBase,
	            int records
	    ) {
	        MemorySegment.copy(source, sourceBase, target, targetBase, tools.bytesForRecords(records));
	    }*/
	   
	    public static void parallelBulkCopy(
	            MemorySegment source,
	            long sourceBase,
	            MemorySegment target,
	            long targetBase,
	            long totalRecords
	    ) throws Exception { // <--- Passes the exception up to the Apex orchestrator safely
	        if (totalRecords <= 0 || (source == target && sourceBase == targetBase)) {
	            return;
	        }

	        long totalBytes = tools.bytesForRecords(totalRecords);
	        if (Apex.POOL == null || Apex.THREADS <= 1 || totalRecords < PARALLEL_COPY_MIN_RECORDS) {
	            MemorySegment.copy(source, sourceBase, target, targetBase, totalBytes);
	            return;
	        }

	        long sliceRecords = Math.max(1L, COPY_SLICE_RECORDS);
	        int copyTasks = (int) Math.min(
	                (long) Apex.THREADS,
	                Math.max(1L, (totalRecords + sliceRecords - 1L) / sliceRecords)
	        );

	        if (copyTasks <= 1) {
	            MemorySegment.copy(source, sourceBase, target, targetBase, totalBytes);
	            return;
	        }

	        long recordsPerTask = totalRecords / copyTasks;
	        java.util.ArrayList<java.util.concurrent.Future<?>> loops =
	                new java.util.ArrayList<>(copyTasks - 1);

	        for (int t = 1; t < copyTasks; t++) {
	            final int tid = t;
	            loops.add(Apex.POOL.submit(() -> {
	                copyRecordSlice(source, sourceBase, target, targetBase, totalRecords, recordsPerTask, copyTasks, tid);
	            }));
	        }

	        copyRecordSlice(source, sourceBase, target, targetBase, totalRecords, recordsPerTask, copyTasks, 0);
	        
	        Tools.tools.waitForFutures(loops); 
	    }

	  public static int detectMonotonicOrder(MemorySegment data, long records) throws Exception {
	        if (records <= 1) {
	            return ORDER_ASCENDING;
	        }

	        int quickOrder = quickOrderProbe(data, records);
	        if (quickOrder == ORDER_MIXED || records <= QUICK_ORDER_RECORDS) {
	            return quickOrder;
	        }

	        long[] firstKeys = new long[Apex.THREADS];
	        long[] lastKeys = new long[Apex.THREADS];
	        boolean[] sawKeys = new boolean[Apex.THREADS];
	        boolean[] ascending = new boolean[Apex.THREADS];
	        boolean[] descending = new boolean[Apex.THREADS];
	        AtomicBoolean mixed = new AtomicBoolean(false);
	        ArrayList<Future<?>> futures = new ArrayList<>(Apex.THREADS);
	        long chunk = records / Apex.THREADS;

	        for (int t = 0; t < Apex.THREADS; t++) {
	            final int tid = t;
	            ascending[tid] = true;
	            descending[tid] = true;

	            futures.add(Apex.POOL.submit(() -> {
	                long s = tid * chunk;
	                long e = (tid == Apex.THREADS - 1) ? records : s + chunk;
	                long p = s << 4;
	                long end = e << 4;

	                if (p >= end || mixed.get()) {
	                    return;
	                }

	                boolean asc = true;
	                boolean desc = true;
	                long first = data.get(Apex.LONG, p);
	                long previous = first;
	                p += Apex.RECORD_BYTES;

	                long unrolledEnd = end - (4L * Apex.RECORD_BYTES);
	                while (p <= unrolledEnd && (asc || desc) && !mixed.get()) {
	                    long k0 = data.get(Apex.LONG, p);
	                    long k1 = data.get(Apex.LONG, p + 16);
	                    long k2 = data.get(Apex.LONG, p + 32);
	                    long k3 = data.get(Apex.LONG, p + 48);

	                    int cmpPrev = Long.compareUnsigned(previous, k0);
	                    int cmp01 = Long.compareUnsigned(k0, k1);
	                    int cmp12 = Long.compareUnsigned(k1, k2);
	                    int cmp23 = Long.compareUnsigned(k2, k3);

	                    asc &= cmpPrev <= 0 && cmp01 <= 0 && cmp12 <= 0 && cmp23 <= 0;
	                    desc &= cmpPrev >= 0 && cmp01 >= 0 && cmp12 >= 0 && cmp23 >= 0;
	                    previous = k3;

	                    if (!asc && !desc) {
	                        mixed.set(true);
	                        break;
	                    }

	                    p += 4L * Apex.RECORD_BYTES;
	                }

	                while (p < end && (asc || desc) && !mixed.get()) {
	                    long key = data.get(Apex.LONG, p);
	                    int cmp = Long.compareUnsigned(previous, key);
	                    asc &= cmp <= 0;
	                    desc &= cmp >= 0;
	                    previous = key;

	                    if (!asc && !desc) {
	                        mixed.set(true);
	                        break;
	                    }

	                    p += Apex.RECORD_BYTES;
	                }

	                sawKeys[tid] = true;
	                firstKeys[tid] = first;
	                lastKeys[tid] = previous;
	                ascending[tid] = asc;
	                descending[tid] = desc;
	            }));
	        }

	        waitForFutures(futures);

	        if (mixed.get()) {
	            return ORDER_MIXED;
	        }

	        boolean sawAny = false;
	        boolean globalAscending = true;
	        boolean globalDescending = true;
	        long previousLast = 0L;

	        for (int t = 0; t < Apex.THREADS; t++) {
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

	            previousLast = lastKeys[t];
	            sawAny = true;
	        }

	        if (globalAscending) {
	            return ORDER_ASCENDING;
	        }

	        return globalDescending ? ORDER_DESCENDING : ORDER_MIXED;
	    }

	  static final int QUICK_ORDER_RECORDS = 2_048;

	  static int quickOrderProbe(MemorySegment data, long records) {
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

	    static void copyRecordSlice(
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

	        if (count <= 0) {
	            return;
	        }

	        long offset = startRecord * Apex.RECORD_BYTES;
	        MemorySegment.copy(source, sourceBase + offset, target, targetBase + offset, count * Apex.RECORD_BYTES);
	    }

	    // Unlocks 512-bit vector hardware registers under JDK 25
	    private static final jdk.incubator.vector.VectorSpecies<Long> L_SPECIES = jdk.incubator.vector.LongVector.SPECIES_512;

	    /**
	     * 🚀 Vector-Accelerated Parallel Reverse Copy Router.
	     * Capitalizes on 512-bit registers to swap and mirror records at memory bus saturation.
	     */
	    public static void reverseCopyRecords(
	            MemorySegment source,
	            long sourceBase,
	            MemorySegment target,
	            long targetBase,
	            long records
	    ) {
	        if (records <= 1) {
	            if (records == 1 && (source != target || sourceBase != targetBase)) {
	                MemorySegment.copy(source, sourceBase, target, targetBase, Apex.RECORD_BYTES);
	            }
	            return;
	        }

	        if (source == target && sourceBase == targetBase) {
	            reverseRecordsInPlace(target, targetBase, records);
	            return;
	        }

	        if (Apex.POOL != null && Apex.THREADS > 1 && records >= PARALLEL_COPY_MIN_RECORDS) {
	            long sliceRecords = Math.max(1L, COPY_SLICE_RECORDS);
	            int copyTasks = (int) Math.min(
	                    (long) Apex.THREADS,
	                    Math.max(1L, (records + sliceRecords - 1L) / sliceRecords)
	            );

	            if (copyTasks > 1) {
	                long recordsPerTask = records / copyTasks;
	                java.util.ArrayList<java.util.concurrent.Future<?>> loops =
	                        new java.util.ArrayList<>(copyTasks - 1);

	                for (int t = 1; t < copyTasks; t++) {
	                    final int tid = t;
	                    loops.add(Apex.POOL.submit(() -> {
	                        reverseCopySlice(source, sourceBase, target, targetBase, records, recordsPerTask, copyTasks, tid);
	                    }));
	                }

	                reverseCopySlice(source, sourceBase, target, targetBase, records, recordsPerTask, copyTasks, 0);

	                try {
	                    Tools.tools.waitForFutures(loops);
	                } catch (Exception ex) {
	                    throw new RuntimeException("Parallel vector reverse copy failed", ex);
	                }
	                return;
	            }
	        }

	        // --- ⚡ Fast 4-Record (64-byte) SIMD Vector Pipeline ---
	        long out = targetBase;
	        long right = records - 1;

	        while (right >= 3) {
	            long pStart = sourceBase + ((right - 3) << 4);

	            // Load 4 contiguous records (8 longs) directly into a 512-bit vector register
	            var vec = jdk.incubator.vector.LongVector.fromMemorySegment(L_SPECIES, source, pStart, java.nio.ByteOrder.nativeOrder());

	            // Rearrange the 64-bit lanes inside the register to invert the record positions cleanly in silicon:
	            // Original:  [K3, V3, K2, V2, K1, V1, K0, V0]
	            // Target:    [K0, V0, K1, V1, K2, V2, K3, V3]
	            var reordered = vec.rearrange(jdk.incubator.vector.VectorShuffle.fromValues(L_SPECIES, 6, 7, 4, 5, 2, 3, 0, 1));

	            // Stream the inverted records directly out to the destination target memory segments
	            reordered.intoMemorySegment(target, out, java.nio.ByteOrder.nativeOrder());

	            out += 64; 
	            right -= 4;
	        }

	        // Handle structural residual scalar tails safely
	        while (right >= 0) {
	            long p = sourceBase + (right << 4);
	            target.set(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, out, source.get(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, p));
	            target.set(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, out + 8, source.get(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, p + 8));
	            out += Apex.RECORD_BYTES;
	            right--;
	        }
	    }

	    /**
	     * 🚀 Vector-Accelerated Sub-Slice Parallel Worker.
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

	        while (remaining >= 4) {
	            long pStart = sourceBase + ((right - 3) << 4);

	            var vec = jdk.incubator.vector.LongVector.fromMemorySegment(L_SPECIES, source, pStart, java.nio.ByteOrder.nativeOrder());
	            var reordered = vec.rearrange(jdk.incubator.vector.VectorShuffle.fromValues(L_SPECIES, 6, 7, 4, 5, 2, 3, 0, 1));
	            reordered.intoMemorySegment(target, out, java.nio.ByteOrder.nativeOrder());

	            out += 64;
	            right -= 4;
	            remaining -= 4;
	        }

	        while (remaining > 0) {
	            long p = sourceBase + (right << 4);
	            target.set(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, out, source.get(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, p));
	            target.set(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, out + 8, source.get(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, p + 8));
	            out += Apex.RECORD_BYTES;
	            right--;
	            remaining--;
	        }
	    }

	    /**
	     * 🚀 In-Place Vector-Accelerated Dual-Pointer Mirror Swapper.
	     */
	    public static void reverseRecordsInPlace(MemorySegment data, long base, long records) {
	        long left = 0;
	        long right = records - 1;

	        // Process whenever we have at least two 4-record blocks (8 total records) left to swap symmetrically
	        while (left + 3 < right - 3) {
	            long leftPos = base + (left << 4);
	            long rightPosStart = base + ((right - 3) << 4);

	            // Load left block and right block into matching hardware vector lanes simultaneously
	            var vecL = jdk.incubator.vector.LongVector.fromMemorySegment(L_SPECIES, data, leftPos, java.nio.ByteOrder.nativeOrder());
	            var vecR = jdk.incubator.vector.LongVector.fromMemorySegment(L_SPECIES, data, rightPosStart, java.nio.ByteOrder.nativeOrder());

	            // Symmetrically invert the internal records during cross-over registration shuffles
	            var invertedL = vecR.rearrange(jdk.incubator.vector.VectorShuffle.fromValues(L_SPECIES, 6, 7, 4, 5, 2, 3, 0, 1));
	            var invertedR = vecL.rearrange(jdk.incubator.vector.VectorShuffle.fromValues(L_SPECIES, 6, 7, 4, 5, 2, 3, 0, 1));

	            // Flush register states back directly over old cross-allocated spatial vectors
	            invertedL.intoMemorySegment(data, leftPos, java.nio.ByteOrder.nativeOrder());
	            invertedR.intoMemorySegment(data, rightPosStart, java.nio.ByteOrder.nativeOrder());

	            left += 4;
	            right -= 4;
	        }

	        // Standard scalar residual fallback cleanup track for the narrow middle window
	        while (left < right) {
	            long lp = base + (left << 4);
	            long rp = base + (right << 4);

	            long lk = data.get(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, lp);
	            long lv = data.get(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, lp + 8);

	            data.set(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, lp, data.get(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, rp));
	            data.set(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, lp + 8, data.get(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, rp + 8));
	            data.set(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, rp, lk);
	            data.set(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, rp + 8, lv);

	            left++;
	            right--;
	        }
	    }






	 public static void configureLargePartitionPermits(Options options) {
	        Apex.LARGE_PARTITION_PERMIT_COUNT = options.largePartitionPermits > 0
	                ? options.largePartitionPermits
	                : Math.max(1, Apex.THREADS / 8);
	        Apex.LARGE_PARTITION_PERMITS = new Semaphore(Apex.LARGE_PARTITION_PERMIT_COUNT);
	    }

	   
	   
}
