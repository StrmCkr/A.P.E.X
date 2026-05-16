package MSD;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.TreeSet;
import java.util.concurrent.Future;

import LSD.lsdbucketplan;
import Tools.tools;
import Tuples.tuples;
import config.configurations.Config;
import histogram.buildhistogram;
import histogram.histogram.HistogramResult;
import main.Apex;

public class msdbucketplan {

	public static class MsdBucketPlan {
		public      final long[] starts;
		public      final int[] sizes;
		public       final int[][] threadScatterOffsets;
	        public      final byte[] bucketFlags;
	        public     final int msdShift;

	        public     final long[] variableMasks;

	        public       final int[] cycleCounts;
	        public        final int[][] cycleShifts;
	        public     final int[][] cycleMasks;
	        public    final long[][] cycleBitMasks;
	        public    final long[] tupleTailMasks;
	        public   final long[] tupleTailPlans;

	        public MsdBucketPlan(Config cfg, int msdShift) {
	            starts = new long[cfg.msdBucketCount];
	            sizes = new int[cfg.msdBucketCount];
	            threadScatterOffsets = new int[Apex.THREADS][cfg.msdBucketCount];
	            bucketFlags = new byte[cfg.msdBucketCount];

	            variableMasks = new long[cfg.msdBucketCount];

	            cycleCounts = new int[cfg.msdBucketCount];
	            cycleShifts = new int[cfg.msdBucketCount][];
	            cycleMasks = new int[cfg.msdBucketCount][];
	            cycleBitMasks = new long[cfg.msdBucketCount][];
	            tupleTailMasks = new long[cfg.msdBucketCount];
	            tupleTailPlans = new long[cfg.msdBucketCount];

	            this.msdShift = msdShift;
	        }
	    }
	   public static MsdBucketPlan buildMsdBucketPlan(
	            HistogramResult result,
	            long n,
	            Config cfg,
	            int msdShift
	    ) {
	        MsdBucketPlan plan = new MsdBucketPlan(cfg, msdShift);

	        long pos = 0;
	        int[] tempCycleShifts = new int[64];
	        int[] tempCycleMasks = new int[64];
	        long[] tempCycleBitMasks = new long[64];

	        for (int b = 0; b < cfg.msdBucketCount; b++) {
	            plan.starts[b] = pos;
	            long bucketOr = 0L;
	            long bucketAnd = ~0L;
	            long bucketSize = 0;
	            boolean seenAny = false;
	            int bucketThreadOffset = 0;

	            for (int t = 0; t < Apex.THREADS; t++) {
	                plan.threadScatterOffsets[t][b] = bucketThreadOffset;

	                int c = result.histograms[t][b];
	                if (c < 0 || bucketSize + c > Integer.MAX_VALUE) {
	                    throw new IllegalArgumentException("Bucket " + b + " exceeds int-sized partition support");
	                }
	                bucketSize += c;
	                bucketThreadOffset += c;

	                if (c == 0) {
	                    continue;
	                }

	                seenAny = true;
	                bucketOr |= result.orMasks[t][b];
	                bucketAnd &= result.andMasks[t][b];
	            }

	            int size = (int) bucketSize;
	            pos += bucketSize;

	            long variableMask = (bucketOr ^ bucketAnd) & tools.lowBitsMask(msdShift);

	            plan.variableMasks[b] = variableMask;
	            plan.sizes[b] = size;

	            if (!seenAny || size == 0) {
	                plan.bucketFlags[b] = Apex.BUCKET_EMPTY;
	            } else if (variableMask == 0L) {
	                plan.bucketFlags[b] = Apex.BUCKET_ALL_EQUAL;
	            } else {
	                plan.bucketFlags[b] = Apex.BUCKET_MIXED;
	            }

	            if (plan.bucketFlags[b] == Apex.BUCKET_MIXED) {
	                if (size < cfg.tinyPartitionThreshold) {
	                    continue;
	                }

	                if (tuples.tupleSpaceFitsDirectPass(variableMask)) {
	                    plan.tupleTailMasks[b] = variableMask;
	                    plan.tupleTailPlans[b] = tuples.buildSmallTuplePlan(variableMask);
	                } else {
	                    int cycles = lsdbucketplan.buildLsdCyclePlan(
	                            variableMask,
	                            cfg,
	                            msdShift,
	                            tempCycleShifts,
	                            tempCycleMasks,
	                            tempCycleBitMasks
	                    );

	                    if (cycles == 0) {
	                        plan.bucketFlags[b] = Apex.BUCKET_ALL_EQUAL;
	                    } else {
	                        int plannedCycles = tuples.plannedCyclePrefixBeforeTupleTail(
	                                variableMask,
	                                tempCycleBitMasks,
	                                cycles
	                        );
	                        long tupleTailMask = tuples.tupleTailMaskAfterPrefix(
	                                variableMask,
	                                tempCycleBitMasks,
	                                plannedCycles
	                        );

	                        plan.cycleCounts[b] = plannedCycles;
	                        plan.tupleTailMasks[b] = tupleTailMask;
	                        plan.tupleTailPlans[b] = tuples.buildSmallTuplePlan(tupleTailMask);

	                        if (plannedCycles > 0) {
	                            plan.cycleShifts[b] = Arrays.copyOf(tempCycleShifts, plannedCycles);
	                            plan.cycleMasks[b] = Arrays.copyOf(tempCycleMasks, plannedCycles);
	                            plan.cycleBitMasks[b] = Arrays.copyOf(tempCycleBitMasks, plannedCycles);
	                        }
	                    }
	                }
	            }
	        }

	        if (pos != n) {
	            throw new RuntimeException("Histogram mismatch: " + pos + " != " + n);
	        }

	        return plan;
	    }
	
	 public static MsdBucketPlan buildAdaptiveMsdBucketPlan(MemorySegment src, long n, Config cfg) throws Exception {
	        int topShift = 64 - cfg.msdBits;
	        HistogramResult topHist = buildhistogram.buildMsdHistograms(src, n, cfg, topShift);
	        MsdBucketPlan topPlan = buildMsdBucketPlan(topHist, n, cfg, topShift);

	        if (largestBucketSize(topPlan, cfg) != n) {
	            return topPlan;
	        }

	        for (int shift : msdShiftCandidates(cfg)) {
	            if (shift == topShift) {
	                continue;
	            }

	            if (!prefixAboveWindowIsConstant(src, n, shift + cfg.msdBits)) {
	                continue;
	            }

	            HistogramResult hist = buildhistogram.buildMsdHistograms(src, n, cfg, shift);
	            MsdBucketPlan plan = buildMsdBucketPlan(hist, n, cfg, shift);

	            if (largestBucketSize(plan, cfg) != n || shift == 0) {
	                return plan;
	            }
	        }

	        return topPlan;
	    }
	  
	  public static int largestBucketSize(MsdBucketPlan plan, Config cfg) {
	        int max = 0;
	        for (int i = 0; i < cfg.msdBucketCount; i++) {
	            if (plan.sizes[i] > max) {
	                max = plan.sizes[i];
	            }
	        }
	        return max;    }
	  
	  public static boolean prefixAboveWindowIsConstant(MemorySegment src, long n, int firstVariableBit) throws Exception {
	        if (firstVariableBit >= 64 || n <= 1) {
	            return true;
	        }
	        ArrayList<Future<Boolean>> futures = new ArrayList<>(Apex.THREADS);
	        long chunk = n / Apex.THREADS;
	        long expected = src.get(Apex.LONG, 0) >>> firstVariableBit;

	        for (int t = 0; t < Apex.THREADS; t++) {
	            final int tid = t;

	            futures.add(Apex.POOL.submit(() -> {
	                long s = tid * chunk;
	                long e = (tid == Apex.THREADS - 1) ? n : s + chunk;
	                long p = s << 4;
	                long end = e << 4;

	                while (p < end) {
	                    if ((src.get(Apex.LONG, p) >>> firstVariableBit) != expected) {
	                        return false;
	                    }
	                    p += Apex.RECORD_BYTES;
	                }

	                return true;
	            }));
	        }

	        for (Future<Boolean> future : futures) {
	            if (!future.get()) {
	                return false;
	            }
	        }

	        return true;
	    }
	        

	        static int[] msdShiftCandidates(Config cfg) {
	            TreeSet<Integer> shifts = new TreeSet<>(Comparator.reverseOrder());
	            int topShift = 64 - cfg.msdBits;

	            for (int shift = topShift; shift > 0; shift = Math.max(0, shift - cfg.msdBits)) {
	                shifts.add(shift);
	                if (shift <= cfg.msdBits) {
	                    shifts.add(0);
	                    break;
	                }
	            }

	            int[] anchorShifts = {32, 16, 8, 0};
	            for (int shift : anchorShifts) {
	                if (shift >= 0 && shift <= topShift) {
	                    shifts.add(shift);
	                }
	            }

	            return shifts.stream().mapToInt(Integer::intValue).toArray();
	        }
	        
	        public static int[] buildLsdWorkBucketsByDescendingSize(MsdBucketPlan plan, Config cfg) {
	            long[] packed = new long[cfg.msdBucketCount];
	            int workCount = 0;

	            for (int b = 0; b < cfg.msdBucketCount; b++) {
	                int size = plan.sizes[b];

	                if (lsdbucketplan.bucketHasLsdWork(plan, cfg, b)) {
	                    packed[workCount++] = (((long) -size) << 32) | (b & 0xFFFFFFFFL);
	                }
	            }

	            Arrays.sort(packed, 0, workCount);

	            int[] workBuckets = new int[workCount];

	            for (int i = 0; i < workCount; i++) {
	                workBuckets[i] = (int) packed[i];
	            }

	            return workBuckets;
	        }

	  
}
