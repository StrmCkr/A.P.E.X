package Tools;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

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

	 public static void configureLargePartitionPermits(Options options) {
	        Apex.LARGE_PARTITION_PERMIT_COUNT = options.largePartitionPermits > 0
	                ? options.largePartitionPermits
	                : Math.max(1, Apex.THREADS / 8);
	        Apex.LARGE_PARTITION_PERMITS = new Semaphore(Apex.LARGE_PARTITION_PERMIT_COUNT);
	    }
	 
		   
}
