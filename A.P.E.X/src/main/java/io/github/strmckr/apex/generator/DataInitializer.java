package io.github.strmckr.apex.generator;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.concurrent.Future;

import io.github.strmckr.apex.engine.ApexEngine;
import io.github.strmckr.apex.tools.Tools;

public class DataInitializer {
	
	public static void initData(MemorySegment seg, long n, DataMode mode) throws Exception {
	        if (n == 0 || mode == DataMode.EMPTY) {
	            return;
	        }

	        ArrayList<Future<?>> futures = new ArrayList<>(ApexEngine.THREADS);
	        long chunk = n / ApexEngine.THREADS;

	        for (int t = 0; t < ApexEngine.THREADS; t++) {
	            final int tid = t;

	            futures.add(ApexEngine.POOL.submit(() -> {
	                long s = tid * chunk;
	                long e = (tid == ApexEngine.THREADS - 1) ? n : s + chunk;

	                long p = s << 4;

	                for (long i = s; i < e; i++) {
	                    seg.set(ApexEngine.LONG, p, DataGenerator.keyForMode(i, n, mode));
	                    seg.set(ApexEngine.LONG, p + 8, i);
	                    p += ApexEngine.RECORD_BYTES;
	                }
	            }));
	        }

	        Tools.waitForFutures(futures);
	    }
}
