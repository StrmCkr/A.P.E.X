package histogram;

import java.util.Arrays;

import config.configurations.Config;
import main.Apex;

public class histogram {
	
	public static class HistogramResult {
		public   final int[][] histograms;
		public  final long[][] orMasks;
		public  final long[][] andMasks;

        HistogramResult(Config cfg) {
            histograms = new int[Apex.THREADS][cfg.msdBucketCount];
            orMasks = new long[Apex.THREADS][cfg.msdBucketCount];
            andMasks = new long[Apex.THREADS][cfg.msdBucketCount];

            for (int t = 0; t < Apex.THREADS; t++) {
                Arrays.fill(andMasks[t], ~0L);
            }
        }
    }

}
