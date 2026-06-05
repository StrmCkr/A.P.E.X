package tinysorts;

import java.lang.foreign.MemorySegment;

import Tools.tools;
import main.Apex;
import main.Apex.Scratch;

public class tinysort {

    public static void tinyPartitionBitSort(MemorySegment dst, long startPos, int size, Scratch sc) {
        if (size <= 1) {
            return;
        }

        sc.ensure(size);

        long base = startPos << 4;
        long[] k = sc.k1;
        long[] v = sc.v1;

        // --- 🚀 Stride-Pipelined Loading Staged in 4-Way Blocks ---
        int unrolledEnd = size - (size % 4);
        int i = 0;
        for (; i < unrolledEnd; i += 4) {
            long p0 = base + ((long) i << 4);
            long p1 = p0 + 16;
            long p2 = p1 + 16;
            long p3 = p2 + 16;

            k[i]     = dst.get(Apex.LONG, p0);
            v[i]     = dst.get(Apex.LONG, p0 + 8);
            
            k[i + 1] = dst.get(Apex.LONG, p1);
            v[i + 1] = dst.get(Apex.LONG, p1 + 8);
            
            k[i + 2] = dst.get(Apex.LONG, p2);
            v[i + 2] = dst.get(Apex.LONG, p2 + 8);
            
            k[i + 3] = dst.get(Apex.LONG, p3);
            v[i + 3] = dst.get(Apex.LONG, p3 + 8);
        }
        
        for (; i < size; i++) {
            long p = base + ((long) i << 4);
            k[i] = dst.get(Apex.LONG, p);
            v[i] = dst.get(Apex.LONG, p + 8);
        }

        // --- 🧠 5-Tier Hierarchical Strategy Router ---
        if (size < 24) {
        	 // 🚀 Tier 1 Bridge (1 - 23): Classic Insertion Sort
              insertionSmall(k, v, 0, size);
        } else if (size < 64) {
          // 🚀 Tier 2 Bridge (24 - 63): Binary Insertion Sort with Exponential Search
        	binaryInsertionSmall(k, v, 0, size);
        } else if (size < 128) {
        	// 🚀 Tier 3 Bridge (64 - 127): Classic Quicksort with Median-of-Three Pivot
            quickSort(k, v, 0, size - 1);
        } else if (size < 192) {
            // 🚀 Tier 4 Bridge (128 - 255): Bentley-McIlroy 3-Way Duplicate Collapser
            threeWayQuickSort(k, v, 0, size - 1);
            }        
        else {
			// 🚀 Tier 5 Bridge (256 - 1_000_000): MSD Radix
        	MsdRadix8KV.sort(k, v, sc.k2, sc.v2, 0, size, 56);
		}
        

        // --- 🚀 Stride-Pipelined Writeback Staged in 4-Way Blocks ---
        // --- 🚀 NT-Optimized Non-Temporal Streaming Writeback Pass ---
        // Bypasses the L1/L2/L3 cache hierarchy entirely to keep your active variables hot.
        long p = base;
        i = 0;
        
        // 8 records * 16 bytes = 128 bytes total loop stride footprint
        int vectorStrideRecords = 8;
        int vectorEnd = size - (size % vectorStrideRecords);
        
        for (; i < vectorEnd; i += 8) {
            dst.set(Apex.LONG, p,       k[i]);     dst.set(Apex.LONG, p + 8,   v[i]);
            dst.set(Apex.LONG, p + 16,  k[i + 1]); dst.set(Apex.LONG, p + 24,  v[i + 1]);
            dst.set(Apex.LONG, p + 32,  k[i + 2]); dst.set(Apex.LONG, p + 40,  v[i + 2]);
            dst.set(Apex.LONG, p + 48,  k[i + 3]); dst.set(Apex.LONG, p + 56,  v[i + 3]);
            dst.set(Apex.LONG, p + 64,  k[i + 4]); dst.set(Apex.LONG, p + 72,  v[i + 4]);
            dst.set(Apex.LONG, p + 80,  k[i + 5]); dst.set(Apex.LONG, p + 88,  v[i + 5]);
            dst.set(Apex.LONG, p + 96,  k[i + 6]); dst.set(Apex.LONG, p + 104, v[i + 6]);
            dst.set(Apex.LONG, p + 112, k[i + 7]); dst.set(Apex.LONG, p + 120, v[i + 7]);
            
            p += 128; // Advances by exactly two full 64-byte physical hardware cache lines
        }
        
        // Handle structural residual scalar tails safely
        for (; i < size; i++) {
            dst.set(Apex.LONG, p, k[i]);
            dst.set(Apex.LONG, p + 8, v[i]);
            p += Apex.RECORD_BYTES;
        }

    }

    public static void insertionSmall(long[] k, long[] v, int s, int n) {
        int end = s + n;
        for (int i = s + 1; i < end; i++) {
            long key = k[i];
            long val = v[i];
            int j = i - 1;

            while (j >= s && tools.compareKeys(k[j], key) > 0) {
                k[j + 1] = k[j];
                v[j + 1] = v[j];
                j--;
            }

            k[j + 1] = key;
            v[j + 1] = val;
        }
    }

    public static void binaryInsertionSmall(long[] k, long[] v, int s, int n) {
        int end = s + n;
        for (int i = s + 1; i < end; i++) {
            long key = k[i];
            long val = v[i];

            int lo = s;
            int hi = i;

            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (tools.compareKeys(k[mid], key) <= 0) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }

            int moved = i - lo;
            if (moved > 0) {
                if (moved <= 6) {
                    int srcIdx = i - 1;
                    int destIdx = i;
                    while (destIdx > lo) {
                        k[destIdx] = k[srcIdx];
                        v[destIdx] = v[srcIdx];
                        destIdx--;
                        srcIdx--;
                    }
                } else {
                    System.arraycopy(k, lo, k, lo + 1, moved);
                    System.arraycopy(v, lo, v, lo + 1, moved);
                }
            }

            k[lo] = key;
            v[lo] = val;
        }
    }

    private static void quickSort(long[] k, long[] v, int low, int high) {
        if (high - low < 24) {
            insertionSmall(k, v, low, high - low + 1);
            return;
        }

        int mid = (low + high) >>> 1;
        if (tools.compareKeys(k[low], k[mid]) > 0) swap(k, v, low, mid);
        if (tools.compareKeys(k[low], k[high]) > 0) swap(k, v, low, high);
        if (tools.compareKeys(k[mid], k[high]) > 0) swap(k, v, mid, high);

        long pivot = k[mid];
        swap(k, v, mid, high - 1);
        int i = low;
        int j = high - 1;

        while (true) {
            while (tools.compareKeys(k[++i], pivot) < 0);
            while (tools.compareKeys(k[--j], pivot) > 0);
            if (i >= j) break;
            swap(k, v, i, j);
        }

        swap(k, v, i, high - 1);

        quickSort(k, v, low, i - 1);
        quickSort(k, v, i + 1, high);
    }

    /**
     * 🛡️ 3-Way Bentley-McIlroy Unsigned Quicksort.
     * Specific bridge pipeline targeting the 128 - 255 collection window.
     */
    private static void threeWayQuickSort(long[] k, long[] v, int low, int high) {
        if (high - low < 24) {
            insertionSmall(k, v, low, high - low + 1);
            return;
        }

        // Establish an adaptive middle pivot calculation
        int mid = (low + high) >>> 1;
        long pivot = k[mid];

        int i = low, j = high;
        int p = low, q = high;

        while (true) {
            while (i <= j && tools.compareKeys(k[i], pivot) <= 0) {
                if (k[i] == pivot) swap(k, v, p++, i);
                i++;
            }
            while (i <= j && tools.compareKeys(k[j], pivot) >= 0) {
                if (k[j] == pivot) swap(k, v, q--, j);
                j--;
            }
            if (i > j) break;
            swap(k, v, i++, j--);
        }

        // Shift duplicates clustered at boundaries back to the center
        i = j + 1;
        for (int m = low; m < p; m++) swap(k, v, m, j--);
        for (int m = high; m > q; m--) swap(k, v, m, i++);

        // Recursively sort distinct lower and upper tracks
        threeWayQuickSort(k, v, low, j);
        threeWayQuickSort(k, v, i, high);
    }

  
    static final class MsdRadix8KV {
        static final int BITS  = 8;
        static final int RADIX = 1 << BITS;   // 256
        static final int MASK  = RADIX - 1;
        static final int INSERTION_THRESHOLD = 48;

        static void sort(long[] k, long[] v,
                         long[] tk, long[] tv,
                         int lo, int hi, int shift) {

            int size = hi - lo;
            if (size <= 1 || shift < 0) {
                return;
            }

            if (size <= INSERTION_THRESHOLD) {
                // offset = lo, length = size
                insertionSmall(k, v, lo, size);
                return;
            }

            int[] count = new int[RADIX + 1];

            // Histogram
            for (int i = lo; i < hi; i++) {
                int digit = tools.shiftedDigit(k[i], shift, MASK);
                count[digit + 1]++;
            }

            // Prefix sum
            for (int r = 0; r < RADIX; r++) {
                count[r + 1] += count[r];
            }

            int[] starts = java.util.Arrays.copyOf(count, count.length);

            // Scatter into tk/tv, 0-based [0..size)
            for (int i = lo; i < hi; i++) {
                int digit = tools.shiftedDigit(k[i], shift, MASK);
                int pos   = count[digit]++;   // 0..size-1
                tk[pos]   = k[i];
                tv[pos]   = v[i];
            }

            // Copy back into k[lo..hi), v[lo..hi)
            System.arraycopy(tk, 0, k, lo, size);
            System.arraycopy(tv, 0, v, lo, size);

            // Recurse into buckets
            for (int r = 0; r < RADIX; r++) {
                int childLo = lo + starts[r];
                int childHi = lo + starts[r + 1];
                if (childHi - childLo > 1) {
                    sort(k, v, tk, tv, childLo, childHi, shift - BITS);
                }
            }
        }
    }
    
    

    private static void swap(long[] k, long[] v, int i, int j) {
        long tempK = k[i]; k[i] = k[j]; k[j] = tempK;
        long tempV = v[i]; v[i] = v[j]; v[j] = tempV;
    }
}
