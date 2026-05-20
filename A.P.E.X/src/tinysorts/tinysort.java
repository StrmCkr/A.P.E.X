package tinysorts;

import java.lang.foreign.MemorySegment;
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
            insertionSmall(k, v, 0, size);
        } else if (size < 64) {
            binaryInsertionSmall(k, v, 0, size);
        } else if (size < 128) {
            quickSort(k, v, 0, size - 1);
        } else if (size < 256) {
            // 🚀 Tier 4 Bridge (128 - 255): Bentley-McIlroy 3-Way Duplicate Collapser
            threeWayQuickSort(k, v, 0, size - 1);
        } else {
            iterativeMergeSort(k, v, sc.k2, sc.v2, size);
        }

        // --- 🚀 Stride-Pipelined Writeback Staged in 4-Way Blocks ---
        long p = base;
        i = 0;
        for (; i < unrolledEnd; i += 4) {
            dst.set(Apex.LONG, p,      k[i]);
            dst.set(Apex.LONG, p + 8,  v[i]);
            
            dst.set(Apex.LONG, p + 16, k[i + 1]);
            dst.set(Apex.LONG, p + 24, v[i + 1]);
            
            dst.set(Apex.LONG, p + 32, k[i + 2]);
            dst.set(Apex.LONG, p + 40, v[i + 2]);
            
            dst.set(Apex.LONG, p + 48, k[i + 3]);
            dst.set(Apex.LONG, p + 56, v[i + 3]);
            
            p += 64;
        }
        
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

            while (j >= s && Long.compareUnsigned(k[j], key) > 0) {
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
                if (Long.compareUnsigned(k[mid], key) <= 0) {
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
        if (Long.compareUnsigned(k[low], k[mid]) > 0) swap(k, v, low, mid);
        if (Long.compareUnsigned(k[low], k[high]) > 0) swap(k, v, low, high);
        if (Long.compareUnsigned(k[mid], k[high]) > 0) swap(k, v, mid, high);

        long pivot = k[mid];
        swap(k, v, mid, high - 1);
        int i = low;
        int j = high - 1;

        while (true) {
            while (Long.compareUnsigned(k[++i], pivot) < 0);
            while (Long.compareUnsigned(k[--j], pivot) > 0);
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
            while (i <= j && Long.compareUnsigned(k[i], pivot) <= 0) {
                if (k[i] == pivot) swap(k, v, p++, i);
                i++;
            }
            while (i <= j && Long.compareUnsigned(k[j], pivot) >= 0) {
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

    private static void iterativeMergeSort(long[] srcK, long[] srcV, long[] workK, long[] workV, int n) {
        long[] currK = srcK; long[] currV = srcV;
        long[] destK = workK; long[] destV = workV;

        for (int i = 0; i < n; i += 16) {
            insertionSmall(currK, currV, i, Math.min(16, n - i));
        }

        for (int width = 16; width < n; width <<= 1) {
            for (int i = 0; i < n; i += (width << 1)) {
                int left = i;
                int mid = Math.min(i + width, n);
                int right = Math.min(i + (width << 1), n);

                int pL = left, pR = mid, out = left;

                while (pL < mid && pR < right) {
                    if (Long.compareUnsigned(currK[pL], currK[pR]) <= 0) {
                        destK[out] = currK[pL];
                        destV[out] = currV[pL++];
                    } else {
                        destK[out] = currK[pR];
                        destV[out] = currV[pR++];
                    }
                    out++;
                }

                while (pL < mid) { destK[out] = currK[pL]; destV[out] = currV[pL++]; out++; }
                while (pR < right) { destK[out] = currK[pR]; destV[out] = currV[pR++]; out++; }
            }

            long[] tempK = currK; currK = destK; destK = tempK;
            long[] tempV = currV; currV = destV; destV = tempV;
        }

        if (currK != srcK) {
            System.arraycopy(currK, 0, srcK, 0, n);
            System.arraycopy(currV, 0, srcV, 0, n);
        }
    }

    private static void swap(long[] k, long[] v, int i, int j) {
        long tempK = k[i]; k[i] = k[j]; k[j] = tempK;
        long tempV = v[i]; v[i] = v[j]; v[j] = tempV;
    }
}
