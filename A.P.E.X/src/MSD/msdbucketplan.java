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
	        public     final long totalRecords;
	        public     boolean inputAscending;
	        public     boolean inputDescending;
	        public     boolean hasLocalMsd;
	        public     double localMsdAttachSeconds;
	        public     final int[] localMsdShifts;
	        public     final int[] localBucketCounts;
	        public     final long[][] localStarts;
	        public     final int[][] localSizes;
	        public     final long[][] localVariableMasks;
	        public     final int[][][] localThreadScatterOffsets;

	        public     final long[] variableMasks;
	        public     final boolean[] bucketAscending;
	        public     final boolean[] bucketDescending;

	        public       final int[] cycleCounts;
	        public        final int[][] cycleShifts;
	        public     final int[][] cycleMasks;
	        public    final long[][] cycleBitMasks;
	        public    final long[][] cycleTuplePlans;
	        public    final long[] tupleTailMasks;
	        public   final long[] tupleTailPlans;
	        public     final boolean[][] localAscending;
	        public     final boolean[][] localDescending;

	        public MsdBucketPlan(Config cfg, int msdShift, long totalRecords) {
	            starts = new long[cfg.msdBucketCount];
	            sizes = new int[cfg.msdBucketCount];
	            threadScatterOffsets = new int[Apex.THREADS][cfg.msdBucketCount];
	            bucketFlags = new byte[cfg.msdBucketCount];
	            localMsdShifts = new int[cfg.msdBucketCount];
	            localBucketCounts = new int[cfg.msdBucketCount];
	            localStarts = new long[cfg.msdBucketCount][];
	            localSizes = new int[cfg.msdBucketCount][];
	            localVariableMasks = new long[cfg.msdBucketCount][];
	            localThreadScatterOffsets = new int[cfg.msdBucketCount][][];
	            Arrays.fill(localMsdShifts, -1);

	            variableMasks = new long[cfg.msdBucketCount];
	            bucketAscending = new boolean[cfg.msdBucketCount];
	            bucketDescending = new boolean[cfg.msdBucketCount];

	            cycleCounts = new int[cfg.msdBucketCount];
	            cycleShifts = new int[cfg.msdBucketCount][];
	            cycleMasks = new int[cfg.msdBucketCount][];
	            cycleBitMasks = new long[cfg.msdBucketCount][];
	            cycleTuplePlans = new long[cfg.msdBucketCount][];
	            tupleTailMasks = new long[cfg.msdBucketCount];
	            tupleTailPlans = new long[cfg.msdBucketCount];
	            localAscending = new boolean[cfg.msdBucketCount][];
	            localDescending = new boolean[cfg.msdBucketCount][];

	            this.msdShift = msdShift;
	            this.totalRecords = totalRecords;
	        }
	    }
	   public static MsdBucketPlan buildMsdBucketPlan(
	            HistogramResult result,
	            long n,
	            Config cfg,
	            int msdShift
	    ) {
	        MsdBucketPlan plan = new MsdBucketPlan(cfg, msdShift, n);
	        plan.inputAscending = globallyMonotonic(result, true);
	        plan.inputDescending = globallyMonotonic(result, false);

	        long pos = 0;
	        long lowerKeyMask = tools.lowBitsMask(msdShift);
	        int[] tempCycleShifts = new int[64];
	        int[] tempCycleMasks = new int[64];
	        long[] tempCycleBitMasks = new long[64];
	        long[] tempCycleTuplePlans = new long[64];

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

	            long variableMask = size > 1 ? ((bucketOr ^ bucketAnd) & lowerKeyMask) : 0L;

	            plan.variableMasks[b] = variableMask;
	            plan.sizes[b] = size;
	            plan.bucketAscending[b] = size <= 1 || bucketMonotonic(result, b, true);
	            plan.bucketDescending[b] = size <= 1 || bucketMonotonic(result, b, false);

	            if (!seenAny || size == 0) {
	                plan.bucketFlags[b] = Apex.BUCKET_EMPTY;
	            } else if (size == 1 || variableMask == 0L) {
	                plan.bucketFlags[b] = Apex.BUCKET_ALL_EQUAL;
	            } else { if (plan.bucketAscending[b]) {
	                    plan.bucketFlags[b] = Apex.BUCKET_ASCENDING;
	                } else if (plan.bucketDescending[b]) {
	                    plan.bucketFlags[b] = Apex.BUCKET_DESCENDING;
	                } else
	                plan.bucketFlags[b] = Apex.BUCKET_MIXED;
	            }

	            if (plan.bucketFlags[b] == Apex.BUCKET_MIXED) {
	                if (size < cfg.tinyPartitionThreshold) {
	                    continue;
	                }

	                if (tuples.tupleSpaceFitsDirectPass(variableMask, size)) {
	                    plan.tupleTailMasks[b] = variableMask;
	                    plan.tupleTailPlans[b] = tuples.buildSmallTuplePlan(variableMask);
	                } else {
	                    int cycles = lsdbucketplan.buildLsdCyclePlan(
	                            variableMask,
	                            cfg,
	                            msdShift,
	                            size,
	                            tempCycleShifts,
	                            tempCycleMasks,
	                            tempCycleBitMasks,
	                            tempCycleTuplePlans
	                    );

	                    if (cycles == 0) {
	                        plan.bucketFlags[b] = Apex.BUCKET_ALL_EQUAL;
	                    } else {
	                        int plannedCycles = tuples.plannedCyclePrefixBeforeTupleTail(
	                                variableMask,
	                                tempCycleBitMasks,
	                                cycles,
	                                size
	                        );
	                        long tupleTailMask = tuples.tupleTailMaskAfterPrefix(
	                                variableMask,
	                                tempCycleBitMasks,
	                                plannedCycles,
	                                size
	                        );

	                        plan.cycleCounts[b] = plannedCycles;
	                        plan.tupleTailMasks[b] = tupleTailMask;
	                        plan.tupleTailPlans[b] = tuples.buildSmallTuplePlan(tupleTailMask);

	                        if (plannedCycles > 0) {
	                            plan.cycleShifts[b] = Arrays.copyOf(tempCycleShifts, plannedCycles);
	                            plan.cycleMasks[b] = Arrays.copyOf(tempCycleMasks, plannedCycles);
	                            plan.cycleBitMasks[b] = Arrays.copyOf(tempCycleBitMasks, plannedCycles);
	                            plan.cycleTuplePlans[b] = Arrays.copyOf(tempCycleTuplePlans, plannedCycles);
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
	            attachLocalMsdPlans(src, n, cfg, topPlan);
	            return topPlan;
	        }

	        long variableMask = collapsedPlanVariableMask(topPlan, cfg);
	        if (variableMask == 0L) {
	            return topPlan;
	        }

	        for (int shift : msdShiftCandidates(cfg)) {
	            if (shift == topShift) {
	                continue;
	            }

	            if (!prefixAboveWindowIsConstant(variableMask, shift + cfg.msdBits)) {
	                continue;
	            }

	            if (shift != 0 && !windowContainsVariableBits(variableMask, shift, cfg.msdBits)) {
	                continue;
	            }

	            HistogramResult hist = buildhistogram.buildMsdHistograms(src, n, cfg, shift);
	            MsdBucketPlan plan = buildMsdBucketPlan(hist, n, cfg, shift);

	            if (largestBucketSize(plan, cfg) != n || shift == 0) {
	                attachLocalMsdPlans(src, n, cfg, plan);
	                return plan;
	            }
	        }

	        attachLocalMsdPlans(src, n, cfg, topPlan);
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

	  static boolean globallyMonotonic(HistogramResult result, boolean ascending) {
	        boolean sawAny = false;
	        long previousLast = 0L;

	        for (int t = 0; t < Apex.THREADS; t++) {
	            if (!result.sawKeys[t]) {
	                continue;
	            }

	            if (ascending) {
	                if (!result.ascending[t]) {
	                    return false;
	                }
	            } else if (!result.descending[t]) {
	                return false;
	            }

	            if (sawAny) {
	                int cmp = Long.compareUnsigned(previousLast, result.firstKeys[t]);
	                if ((ascending && cmp > 0) || (!ascending && cmp < 0)) {
	                    return false;
	                }
	            }

	            previousLast = result.lastKeys[t];
	            sawAny = true;
	        }

	        return true;
	    }

	  static boolean bucketMonotonic(HistogramResult result, int bucket, boolean ascending) {
	        boolean sawAny = false;
	        long previousLast = 0L;

	        for (int t = 0; t < Apex.THREADS; t++) {
	            if (!result.bucketSawKeys[t][bucket]) {
	                continue;
	            }

	            if (ascending) {
	                if (!result.bucketAscending[t][bucket]) {
	                    return false;
	                }
	            } else if (!result.bucketDescending[t][bucket]) {
	                return false;
	            }

	            if (sawAny) {
	                int cmp = Long.compareUnsigned(previousLast, result.bucketFirstKeys[t][bucket]);
	                if ((ascending && cmp > 0) || (!ascending && cmp < 0)) {
	                    return false;
	                }
	            }

	            previousLast = result.bucketLastKeys[t][bucket];
	            sawAny = true;
	        }

	        return true;
	    }

	  static void attachLocalMsdPlans(MemorySegment src, long n, Config cfg, MsdBucketPlan plan) throws Exception {
	        if (!Apex.LOCAL_MSD_REPARTITION) {
	            return;
	        }

	        long attachStart = System.nanoTime();
	        int[] candidateIndexByBucket = new int[cfg.msdBucketCount];
	        Arrays.fill(candidateIndexByBucket, -1);

	        int[] candidateBucketsTemp = new int[cfg.msdBucketCount];
	        int candidateCount = 0;

	        for (int b = 0; b < cfg.msdBucketCount; b++) {
	            int localShift = lsdbucketplan.localMsdShiftForBucket(plan, cfg, b);
	            if (localShift >= 0) {
	                candidateBucketsTemp[candidateCount++] = b;
	            }
	        }

	        if (candidateCount == 0) {
	            return;
	        }

	        Arrays.fill(candidateIndexByBucket, -1);
	        int localBits = lsdbucketplan.localMsdBitsForCandidateCount(cfg, candidateCount);
	        int filteredCandidateCount = 0;

	        for (int i = 0; i < candidateCount; i++) {
	            int b = candidateBucketsTemp[i];
	            int localShift = lsdbucketplan.localMsdShiftForBucket(plan, cfg, b, localBits);
	            if (localShift >= 0) {
	                candidateIndexByBucket[b] = filteredCandidateCount;
	                candidateBucketsTemp[filteredCandidateCount++] = b;
	                plan.localMsdShifts[b] = localShift;
	            }
	        }

	        candidateCount = filteredCandidateCount;

	        if (candidateCount == 0) {
	            plan.localMsdAttachSeconds += (System.nanoTime() - attachStart) / 1e9;
	            return;
	        }

	        final int localCandidateCount = candidateCount;
	        int[] candidateBuckets = Arrays.copyOf(candidateBucketsTemp, candidateCount);
	        int localBucketCount = lsdbucketplan.localMsdBucketCount(localBits);
	        int localBucketMask = localBucketCount - 1;
	        int rows = Apex.THREADS * candidateCount;
	        int[][] childHistograms = new int[rows][localBucketCount];
	        long[][] childOrMasks = new long[rows][localBucketCount];
	        long[][] childAndMasks = new long[rows][localBucketCount];
	        long[][] childFirstKeys = new long[rows][localBucketCount];
	        long[][] childLastKeys = new long[rows][localBucketCount];
	        boolean[][] childSawKeys = new boolean[rows][localBucketCount];
	        boolean[][] childAscending = new boolean[rows][localBucketCount];
	        boolean[][] childDescending = new boolean[rows][localBucketCount];

	        for (int row = 0; row < rows; row++) {
	            Arrays.fill(childAndMasks[row], ~0L);
	            Arrays.fill(childAscending[row], true);
	            Arrays.fill(childDescending[row], true);
	        }

	        ArrayList<Future<?>> futures = new ArrayList<>(Apex.THREADS);
	        long chunk = n / Apex.THREADS;
	        int bucketMask = cfg.msdBucketCount - 1;

	        for (int t = 0; t < Apex.THREADS; t++) {
	            final int tid = t;

	            futures.add(Apex.POOL.submit(() -> {
	                long s = tid * chunk;
	                long e = (tid == Apex.THREADS - 1) ? n : s + chunk;
	                long p = s << 4;
	                long end = e << 4;
	                long unrolledEnd = end - (4L * Apex.RECORD_BYTES);

	                while (p <= unrolledEnd) {
	                    long k0 = src.get(Apex.LONG, p);
	                    long k1 = src.get(Apex.LONG, p + 16);
	                    long k2 = src.get(Apex.LONG, p + 32);
	                    long k3 = src.get(Apex.LONG, p + 48);

	                    int parent0 = (int) ((k0 >>> plan.msdShift) & bucketMask);
	                    int parent1 = (int) ((k1 >>> plan.msdShift) & bucketMask);
	                    int parent2 = (int) ((k2 >>> plan.msdShift) & bucketMask);
	                    int parent3 = (int) ((k3 >>> plan.msdShift) & bucketMask);

	                    int candidateIndex0 = candidateIndexByBucket[parent0];
	                    if (candidateIndex0 >= 0) {
	                        int child = (int) ((k0 >>> plan.localMsdShifts[parent0]) & localBucketMask);
	                        int row = (tid * localCandidateCount) + candidateIndex0;
	                        recordLocalOrder(k0, child, childFirstKeys[row], childLastKeys[row],
	                                childSawKeys[row], childAscending[row], childDescending[row]);
	                        childHistograms[row][child]++;
	                        childOrMasks[row][child] |= k0;
	                        childAndMasks[row][child] &= k0;
	                    }

	                    int candidateIndex1 = candidateIndexByBucket[parent1];
	                    if (candidateIndex1 >= 0) {
	                        int child = (int) ((k1 >>> plan.localMsdShifts[parent1]) & localBucketMask);
	                        int row = (tid * localCandidateCount) + candidateIndex1;
	                        recordLocalOrder(k1, child, childFirstKeys[row], childLastKeys[row],
	                                childSawKeys[row], childAscending[row], childDescending[row]);
	                        childHistograms[row][child]++;
	                        childOrMasks[row][child] |= k1;
	                        childAndMasks[row][child] &= k1;
	                    }

	                    int candidateIndex2 = candidateIndexByBucket[parent2];
	                    if (candidateIndex2 >= 0) {
	                        int child = (int) ((k2 >>> plan.localMsdShifts[parent2]) & localBucketMask);
	                        int row = (tid * localCandidateCount) + candidateIndex2;
	                        recordLocalOrder(k2, child, childFirstKeys[row], childLastKeys[row],
	                                childSawKeys[row], childAscending[row], childDescending[row]);
	                        childHistograms[row][child]++;
	                        childOrMasks[row][child] |= k2;
	                        childAndMasks[row][child] &= k2;
	                    }

	                    int candidateIndex3 = candidateIndexByBucket[parent3];
	                    if (candidateIndex3 >= 0) {
	                        int child = (int) ((k3 >>> plan.localMsdShifts[parent3]) & localBucketMask);
	                        int row = (tid * localCandidateCount) + candidateIndex3;
	                        recordLocalOrder(k3, child, childFirstKeys[row], childLastKeys[row],
	                                childSawKeys[row], childAscending[row], childDescending[row]);
	                        childHistograms[row][child]++;
	                        childOrMasks[row][child] |= k3;
	                        childAndMasks[row][child] &= k3;
	                    }

	                    p += 4L * Apex.RECORD_BYTES;
	                }

	                while (p < end) {
	                    long k = src.get(Apex.LONG, p);
	                    int parent = (int) ((k >>> plan.msdShift) & bucketMask);
	                    int candidateIndex = candidateIndexByBucket[parent];

	                    if (candidateIndex >= 0) {
	                        int child = (int) ((k >>> plan.localMsdShifts[parent]) & localBucketMask);
	                        int row = (tid * localCandidateCount) + candidateIndex;

	                        recordLocalOrder(k, child, childFirstKeys[row], childLastKeys[row],
	                                childSawKeys[row], childAscending[row], childDescending[row]);
	                        childHistograms[row][child]++;
	                        childOrMasks[row][child] |= k;
	                        childAndMasks[row][child] &= k;
	                    }

	                    p += Apex.RECORD_BYTES;
	                }
	            }));
	        }

	        tools.waitForFutures(futures);

	        for (int candidateIndex = 0; candidateIndex < candidateCount; candidateIndex++) {
	            int parent = candidateBuckets[candidateIndex];
	            long[] localStarts = new long[localBucketCount];
	            int[] localSizes = new int[localBucketCount];
	            long[] localVariableMasks = new long[localBucketCount];
	            boolean[] localAscending = new boolean[localBucketCount];
	            boolean[] localDescending = new boolean[localBucketCount];
	            int[][] localThreadOffsets = new int[Apex.THREADS][localBucketCount];
	            long pos = plan.starts[parent];
	            long lowerMask = tools.lowBitsMask(plan.localMsdShifts[parent]);

	            for (int child = 0; child < localBucketCount; child++) {
	                localStarts[child] = pos;

	                int size = 0;
	                long childOr = 0L;
	                long childAnd = ~0L;
	                int offset = 0;

	                for (int t = 0; t < Apex.THREADS; t++) {
	                    int row = (t * candidateCount) + candidateIndex;
	                    int count = childHistograms[row][child];

	                    localThreadOffsets[t][child] = offset;
	                    offset += count;
	                    size += count;

	                    if (count != 0) {
	                        childOr |= childOrMasks[row][child];
	                        childAnd &= childAndMasks[row][child];
	                    }
	                }

	                localSizes[child] = size;
	                localVariableMasks[child] = size > 1 ? ((childOr ^ childAnd) & lowerMask) : 0L;
	                localAscending[child] = size <= 1 || localChildMonotonic(
	                        child, candidateIndex, candidateCount, childSawKeys, childAscending,
	                        childFirstKeys, childLastKeys, true
	                );
	                localDescending[child] = size <= 1 || localChildMonotonic(
	                        child, candidateIndex, candidateCount, childSawKeys, childDescending,
	                        childFirstKeys, childLastKeys, false
	                );
	                pos += size;
	            }

	            if (pos != plan.starts[parent] + plan.sizes[parent]) {
	                throw new RuntimeException("Local MSD histogram mismatch for bucket " + parent);
	            }

	            plan.localStarts[parent] = localStarts;
	            plan.localSizes[parent] = localSizes;
	            plan.localVariableMasks[parent] = localVariableMasks;
	            plan.localThreadScatterOffsets[parent] = localThreadOffsets;
	            plan.localAscending[parent] = localAscending;
	            plan.localDescending[parent] = localDescending;
	            plan.localBucketCounts[parent] = localBucketCount;
	            plan.hasLocalMsd = true;
	        }

	        plan.localMsdAttachSeconds += (System.nanoTime() - attachStart) / 1e9;
	    }

	  static void recordLocalOrder(
	            long key,
	            int bucket,
	            long[] firstKeys,
	            long[] lastKeys,
	            boolean[] sawKeys,
	            boolean[] ascending,
	            boolean[] descending
	    ) {
	        if (sawKeys[bucket]) {
	            int cmp = Long.compareUnsigned(lastKeys[bucket], key);
	            ascending[bucket] &= cmp <= 0;
	            descending[bucket] &= cmp >= 0;
	        } else {
	            firstKeys[bucket] = key;
	            sawKeys[bucket] = true;
	        }
	        lastKeys[bucket] = key;
	    }

	  static boolean localChildMonotonic(
	            int child,
	            int candidateIndex,
	            int candidateCount,
	            boolean[][] childSawKeys,
	            boolean[][] childOrder,
	            long[][] childFirstKeys,
	            long[][] childLastKeys,
	            boolean ascending
	    ) {
	        boolean sawAny = false;
	        long previousLast = 0L;

	        for (int t = 0; t < Apex.THREADS; t++) {
	            int row = (t * candidateCount) + candidateIndex;
	            if (!childSawKeys[row][child]) {
	                continue;
	            }

	            if (!childOrder[row][child]) {
	                return false;
	            }

	            if (sawAny) {
	                int cmp = Long.compareUnsigned(previousLast, childFirstKeys[row][child]);
	                if ((ascending && cmp > 0) || (!ascending && cmp < 0)) {
	                    return false;
	                }
	            }

	            previousLast = childLastKeys[row][child];
	            sawAny = true;
	        }

	        return true;
	    }

	  static long collapsedPlanVariableMask(MsdBucketPlan plan, Config cfg) {
	        long variableMask = 0L;

	        for (int b = 0; b < cfg.msdBucketCount; b++) {
	            if (plan.sizes[b] != 0) {
	                variableMask |= plan.variableMasks[b];
	            }
	        }

	        return variableMask;
	    }

	  static boolean prefixAboveWindowIsConstant(long variableMask, int firstVariableBit) {
	        if (firstVariableBit >= 64) {
	            return true;
	        }

	        return (variableMask & ~tools.lowBitsMask(firstVariableBit)) == 0L;
	    }

	  static boolean windowContainsVariableBits(long variableMask, int shift, int bits) {
	        long windowMask = tools.lowBitsMask(bits) << shift;
	        return (variableMask & windowMask) != 0L;
	    }
	  
	  public static boolean prefixAboveWindowIsConstant(MemorySegment src, long n, int firstVariableBit) throws Exception {
	        if (firstVariableBit >= 64 || n <= 1) {
	            return true;
	        }
	        ArrayList<Future<Boolean>> futures = new ArrayList<>(Apex.THREADS);
	        long chunk = n / Apex.THREADS;
	        long prefixMask = ~tools.lowBitsMask(firstVariableBit);
	        long expected = src.get(Apex.LONG, 0) & prefixMask;

	        for (int t = 0; t < Apex.THREADS; t++) {
	            final int tid = t;

	            futures.add(Apex.POOL.submit(() -> {
	                long s = tid * chunk;
	                long e = (tid == Apex.THREADS - 1) ? n : s + chunk;
	                long p = s << 4;
	                long end = e << 4;
	                long unrolledEnd = end - (4L * Apex.RECORD_BYTES);

	                while (p <= unrolledEnd) {
	                    if (((src.get(Apex.LONG, p) & prefixMask) != expected) ||
	                            ((src.get(Apex.LONG, p + 16) & prefixMask) != expected) ||
	                            ((src.get(Apex.LONG, p + 32) & prefixMask) != expected) ||
	                            ((src.get(Apex.LONG, p + 48) & prefixMask) != expected)) {
	                        return false;
	                    }
	                    p += 4L * Apex.RECORD_BYTES;
	                }

	                while (p < end) {
	                    if ((src.get(Apex.LONG, p) & prefixMask) != expected) {
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
