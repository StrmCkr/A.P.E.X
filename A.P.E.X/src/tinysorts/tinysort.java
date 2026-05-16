package tinysorts;

import java.lang.foreign.MemorySegment;

import main.Apex;
import main.Apex.Scratch;

public class tinysort {

	public static void tinyPartitionBitSort(MemorySegment dst, long startPos, int size, Scratch sc) {
	        sc.ensure(size);

	        long base = startPos << 4;

	        long[] k = sc.k1;
	        long[] v = sc.v1;

	        for (int i = 0; i < size; i++) {
	            long p = base + ((long) i << 4);
	            k[i] = dst.get(Apex.LONG, p);
	            v[i] = dst.get(Apex.LONG, p + 8);
	        }

	        if (size < 24) {
	            insertionSmall(k, v, 0, size);
	        } else {
	            binaryInsertionSmall(k, v, 0, size);
	        }

	        long p = base;

	        for (int i = 0; i < size; i++) {
	            dst.set(Apex.LONG, p, k[i]);
	            dst.set(Apex.LONG, p + 8, v[i]);
	            p += Apex.RECORD_BYTES;
	        }
	    }

	public    static void insertionSmall(long[] k, long[] v, int s, int n) {
	        for (int i = s + 1; i < s + n; i++) {
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

	public   static void binaryInsertionSmall(long[] k, long[] v, int s, int n) {
	        for (int i = s + 1; i < s + n; i++) {
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

	            for (int j = i; j > lo; j--) {
	                k[j] = k[j - 1];
	                v[j] = v[j - 1];
	            }

	            k[lo] = key;
	            v[lo] = val;
	        }
	    }
	
	
}
