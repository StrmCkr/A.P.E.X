package io.github.strmckr.apex.histogram;

import java.util.Arrays;

import io.github.strmckr.apex.config.SortConfig;
import io.github.strmckr.apex.engine.ApexEngine;

public class Histogram {
	
	public static class HistogramResult {
		public   final int[][] histograms;
		public  final long[][] orMasks;
		public  final long[][] andMasks;
		public  final long[] firstKeys;
		public  final long[] lastKeys;
		public  final boolean[] sawKeys;
		public  final boolean[] ascending;
		public  final boolean[] descending;
		public  final long[][] bucketFirstKeys;
		public  final long[][] bucketLastKeys;
		public  final boolean[][] bucketSawKeys;
		public  final boolean[][] bucketAscending;
		public  final boolean[][] bucketDescending;

        HistogramResult(SortConfig cfg) {
            histograms = new int[ApexEngine.THREADS][cfg.msdBucketCount];
            orMasks = new long[ApexEngine.THREADS][cfg.msdBucketCount];
            andMasks = new long[ApexEngine.THREADS][cfg.msdBucketCount];
            firstKeys = new long[ApexEngine.THREADS];
            lastKeys = new long[ApexEngine.THREADS];
            sawKeys = new boolean[ApexEngine.THREADS];
            ascending = new boolean[ApexEngine.THREADS];
            descending = new boolean[ApexEngine.THREADS];
            bucketFirstKeys = new long[ApexEngine.THREADS][cfg.msdBucketCount];
            bucketLastKeys = new long[ApexEngine.THREADS][cfg.msdBucketCount];
            bucketSawKeys = new boolean[ApexEngine.THREADS][cfg.msdBucketCount];
            bucketAscending = new boolean[ApexEngine.THREADS][cfg.msdBucketCount];
            bucketDescending = new boolean[ApexEngine.THREADS][cfg.msdBucketCount];

            for (int t = 0; t < ApexEngine.THREADS; t++) {
                Arrays.fill(andMasks[t], ~0L);
                ascending[t] = true;
                descending[t] = true;
                Arrays.fill(bucketAscending[t], true);
                Arrays.fill(bucketDescending[t], true);
            }
        }
    }

}
