package generator;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.concurrent.Future;

import Tools.tools;
import main.Apex;

public class initiatedata {
	
	public static void initData(MemorySegment seg, long n, DataMode mode) throws Exception {
	        if (n == 0 || mode == DataMode.EMPTY) {
	            return;
	        }

	        ArrayList<Future<?>> futures = new ArrayList<>(Apex.THREADS);
	        long chunk = n / Apex.THREADS;

	        for (int t = 0; t < Apex.THREADS; t++) {
	            final int tid = t;

	            futures.add(Apex.POOL.submit(() -> {
	                long s = tid * chunk;
	                long e = (tid == Apex.THREADS - 1) ? n : s + chunk;

	                long p = s << 4;

	                for (long i = s; i < e; i++) {
	                    seg.set(Apex.LONG, p, DataGenerator.keyForMode(i, n, mode));
	                    seg.set(Apex.LONG, p + 8, i);
	                    p += Apex.RECORD_BYTES;
	                }
	            }));
	        }

	        tools.waitForFutures(futures);
	    }
}
