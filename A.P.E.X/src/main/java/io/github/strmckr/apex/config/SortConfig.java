package io.github.strmckr.apex.config;

public final class SortConfig {
    public final int msdBits;
    public final int msdBucketCount;
    public final int lsdBits;
    public final int lsdRadix;
    public final int lsdShift;
    public final int lsdPasses;
    public final int tinyPartitionThreshold;

    public SortConfig(int msdBits, int lsdBits, int tinyPartitionThreshold) {
        this.msdBits = msdBits;
        this.msdBucketCount = 1 << msdBits;
        this.lsdBits = lsdBits;
        this.lsdRadix = 1 << lsdBits;
        this.lsdShift = lsdBits;
        this.lsdPasses = (64 - msdBits + lsdShift - 1) / lsdShift;
        this.tinyPartitionThreshold = tinyPartitionThreshold;
    }

    @Override
    public String toString() {
        return "MSD_BITS=" + msdBits +
                " LSD_BITS=" + lsdBits +
                " LSD_PASSES=" + lsdPasses +
                " TINY=" + tinyPartitionThreshold;
    }
}
