package histogram;

import java.util.Arrays;

import config.configurations.Config;
import main.Apex;

public class histogram {
	
	public static class HistogramResult {
		public   final int[][] histograms;
		public  final long[][] orMasks;
		public  final long[][] andMasks;
		public  final long[] firstKeys;
		public  final long[] lastKeys;
		public  final boolean[] sawKeys;
		public  final boolean[] ascending;
		public  final boolean[] descending;

        HistogramResult(Config cfg) {
            histograms = new int[Apex.THREADS][cfg.msdBucketCount];
            orMasks = new long[Apex.THREADS][cfg.msdBucketCount];
            andMasks = new long[Apex.THREADS][cfg.msdBucketCount];
            firstKeys = new long[Apex.THREADS];
            lastKeys = new long[Apex.THREADS];
            sawKeys = new boolean[Apex.THREADS];
            ascending = new boolean[Apex.THREADS];
            descending = new boolean[Apex.THREADS];

            for (int t = 0; t < Apex.THREADS; t++) {
                Arrays.fill(andMasks[t], ~0L);
                ascending[t] = true;
                descending[t] = true;
            }
        }
    }

}
