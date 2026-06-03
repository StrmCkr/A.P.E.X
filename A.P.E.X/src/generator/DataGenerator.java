package generator;

import Tools.tools;

public final class DataGenerator {

   public static long keyForMode(long i, long n, DataMode mode) {
            switch (mode) {
                case RANDOM:
                    return mix64(i);

                case SORTED:
                    return scaleOrderedKey(i, n);

                case REVERSE:
                    return scaleOrderedKey(n - 1 - i, n);

                case NEARLY_SORTED: {
                    long j = i ^ (mix64(i) & 1023L);
                    if (j >= n) {
                        j = i;
                    }
                    return scaleOrderedKey(j, n);
                }

                case DUPLICATES: {
                    long classes = 1L << 20;
                    long x = mix64(i) & (classes - 1);
                    return scaleOrderedKey(x, classes);
                }

                case LOW_BITS_ONLY:
                    return mix64(i) & 0xFFFFFFFFL;

                case HIGH_BITS_ONLY:
                    return mix64(i) & 0xFFFFFFFF00000000L;

                case ZIPFIANISH: {
                    long x = mix64(i);
                    int bucket = Long.numberOfLeadingZeros(x | 1L);
                    return ((long) bucket << 56) | (x & 0x00FFFFFFFFFFFFFFL);
                }

                case ALL_EQUAL:
                    return 0;

                case EMPTY:
                    throw new IllegalArgumentException("EMPTY mode should not generate elements");

                case SINGLE_ELEMENT:
                    return 0;

                case TWO_ELEMENTS_SORTED:
                    return scaleOrderedKey(i, 2);

                case TWO_ELEMENTS_REVERSED:
                    return scaleOrderedKey(1 - i, 2);

                case SAWTOOTH: {
                    long period = 1024;
                    return scaleOrderedKey(i % period, period);
                }

                case ORGAN_PIPE: {
                    long mid = n >>> 1;
                    long x = (i <= mid) ? i : (n - 1 - i);
                    return scaleOrderedKey(x, mid + 1);
                }

                case ROTATED_SORTED: {
                    long shift = n / 3;
                    return scaleOrderedKey((i + shift) % n, n);
                }

                case PARTIALLY_SHUFFLED: {
                    if ((mix64(i) & 15L) == 0) {
                        return mix64(i);
                    }
                    return scaleOrderedKey(i, n);
                }

                case BLOCK_SORTED: {
                    long blockSize = 1024;
                    long block = i / blockSize;
                    long offset = i % blockSize;
                    long x = block * blockSize + offset;
                    return scaleOrderedKey(x, n);
                }

                case STAGGERED_RUNS: {
                    long run = 64;
                    long group = i / run;
                    long pos = i % run;
                    long x = group * run + ((pos * 7) % run);
                    return scaleOrderedKey(x, n);
                }

                case FEW_UNIQUE_VALUES:
                    return mix64(i) & 15;

                case MANY_DUPLICATES_WITH_OUTLIERS:
                    return ((mix64(i) & 1023) == 0) ? mix64(i) : (mix64(i) & 15);

                case ALTERNATING_LOW_HIGH:
                    return (i & 1) == 0
                            ? scaleOrderedKey(i >>> 1, n)
                            : scaleOrderedKey(n - 1 - (i >>> 1), n);

                case MIN_MAX_ALTERNATING:
                    return (i & 1) == 0 ? Long.MIN_VALUE : Long.MAX_VALUE;

                case NEGATIVE_VALUES:
                    return mix64(i);

                case EXTREME_VALUES:
                    return (i & 1) == 0 ? Long.MIN_VALUE : Long.MAX_VALUE;

                case INTEGER_OVERFLOW_RISK:
                    return Long.MAX_VALUE - i;

                case GAUSSIAN: {
                    long x = mix64(i) & 0xFFFF;
                    long y = mix64(i + 1) & 0xFFFF;
                    long z = x + y;
                    return scaleOrderedKey(z, 1 << 17);
                }

                case EXPONENTIAL: {
                    long x = mix64(i);
                    int lz = Long.numberOfLeadingZeros(x | 1L);
                    return scaleOrderedKey(lz, 64);
                }

                case BIMODAL: {
                    long x = mix64(i);
                    return ((x & 1) == 0)
                            ? scaleOrderedKey(x & 0xFFFF, 1 << 16)
                            : scaleOrderedKey((x & 0xFFFF) + (1 << 16), 1 << 17);
                }

                case POWER_LAW: {
                    long x = mix64(i);
                    int lz = Long.numberOfLeadingZeros(x | 1L);
                    return 1L << (63 - lz);
                }

                case DELAYED_ENTROPY: {
                    long base = 0x1234_5678_9ABC_0000L;
                    return base | (mix64(i) & 0xFFFF);
                }

                case CLUSTERED_KEYS: {
                    long cluster = i / 1024;
                    long noise = mix64(i) & 0x3FF;
                    return (cluster << 32) | noise;
                }

                case BIT_REVERSAL:
                    return Long.reverse(i);

                case STRIDED:
                    return i * 0x9E3779B97F4A7C15L;

                case SPIKE_NOISE:
                    return (i & 0xFFFF) == 0 ? mix64(i) : 42;

                case DESCENDING_BLOCKS: {
                    long block = i / 1024;
                    long offset = 1023 - (i % 1024);
                    return (block << 10) | offset;
                }

                case DENSE_16BIT:
                    return mix64(i) & 0xFFFF;

                case ENTROPY_BANDS:
                    return ((i / 1024) & 1) == 0 ? i : mix64(i);

                case TINY_PARTITIONS_STRESS:
                    return ((i % 128) << 32) | (mix64(i) & 0xFFFFFFFFL);
                    
                case ENTROPY_OSCILLATION:
                    if (((i >>> 6) & 1L) == 0L) {
                        return mix64(i);
                    } else {
                        return 0x1234567812345678L;
                    }

                case SINGLE_BIT_TAIL_ENTROPY:
                    return 0xCAFEBABE00000000L | (mix64(i) & 1L);

                case SPARSE_ENTROPY_EXPLOSION: {
                    long x = mix64(i);
                    long k = 0;

                    if ((x & (1L << 0))  != 0) k |= (1L << 0);
                    if ((x & (1L << 1))  != 0) k |= (1L << 7);
                    if ((x & (1L << 2))  != 0) k |= (1L << 13);
                    if ((x & (1L << 3))  != 0) k |= (1L << 24);
                    if ((x & (1L << 4))  != 0) k |= (1L << 31);
                    if ((x & (1L << 5))  != 0) k |= (1L << 40);
                    if ((x & (1L << 6))  != 0) k |= (1L << 52);
                    if ((x & (1L << 7))  != 0) k |= (1L << 61);

                    return k;
                }

                case WORST_CASE_MSD_COLLAPSE:
                    return (i < n - 1000)
                            ? 0x1000000000000000L
                            : mix64(i);

                case ALTERNATING_BUCKET_DESTINATIONS:
                    return ((i & 1L) == 0L)
                            ? 0x0000000000000000L
                            : 0xFF00000000000000L;

                case EXTREME_DUPLICATE_DENSITY: {
                    switch ((int)(mix64(i) & 3L)) {
                        case 0: return 0x1111111111111111L;
                        case 1: return 0x2222222222222222L;
                        case 2: return 0x3333333333333333L;
                        default:return 0x4444444444444444L;
                    }
                }

                case HIGH_ENTROPY_PREFIX_CONSTANT_TAIL: {
                    long upper = mix64(i) & ~((1L << 40) - 1L);
                    long lower = 0x123456789AL;
                    return upper | lower;
                }

                case SIGN_BIT_BOUNDARY:
                    return Long.MIN_VALUE + i;

                case CACHE_THRASH:
                    return mix64(i * 0x9E3779B97F4A7C15L);

                case CROSS_THREAD_BUCKET_SKEW:
                    return (i < ((n * 95L) / 100L))
                            ? 0L
                            : mix64(i);

                case TUPLE_29BIT: {
                    long x = mix64(i);
                    return x & ((1L << 29) - 1L);
                }

                case TUPLE_30BIT: {
                    long x = mix64(i);
                    return x & ((1L << 30) - 1L);
                }

                case TUPLE_31BIT_OVERFLOW: {
                    long x = mix64(i);
                    return x & ((1L << 31) - 1L);
                }

                case PERMUTATION_STRESS:
                    return mix64(i ^ 0x9E3779B97F4A7C15L);

                case SINGLE_HOT_BUCKET:
                    return ((mix64(i) & 0xFFFFL) == 0L)
                            ? mix64(i)
                            : 0x7777777777777777L;

                case RADIX_PATHOLOGICAL:
                    return ((i & 1L) == 0L)
                            ? (mix64(i) & 0x00000000FFFFFFFFL)
                            : (mix64(i) & 0xFFFFFFFF00000000L);

                case PREFIX_CONSTANT_RANDOM_TAIL:
                    return 0xABCDEF1234000000L | (mix64(i) & 0xFFFFFFL);

                case TWO_BUCKET_COLLISION:
                    return ((mix64(i) & 1L) == 0L)
                            ? 0x0100000000000000L
                            : 0x0200000000000000L;

                case LARGE_EQUAL_REGION_WITH_RANDOM_END:
                    return (i < n - 8192)
                            ? 0x5555555555555555L
                            : mix64(i);

                case BIT_SPARSE_POWERLAW: {
                    long x = mix64(i);
                    int bit = Long.numberOfLeadingZeros(x | 1L);
                    return 1L << (63 - bit);
                }

                case MICRO_CLUSTERS: {
                    long cluster = (i / 16);
                    long noise = mix64(i) & 0xFL;
                    return (cluster << 8) | noise;
                }

                case INTERLEAVED_SORTED_RANDOM:
                    return ((i & 1L) == 0L)
                            ? scaleOrderedKey(i >>> 1, n)
                            : mix64(i);

                case SAWTOOTH_DESCENDING: {
                    long period = 1024;
                    long x = period - 1 - (i % period);
                    return scaleOrderedKey(x, period);
                }

                case LOW_CARDINALITY_HIGH_VOLUME:
                    return mix64(i) & 7L;

                case HIGH_BIT_ONLY_MOVING:
                    return (mix64(i) & 1L) << 63;

                case ALMOST_SORTED_WITH_SPIKES:
                    return ((i & 0xFFFFL) == 0L)
                            ? mix64(i)
                            : scaleOrderedKey(i, n);

                default:
                    throw new IllegalArgumentException("Unknown mode: " + mode);
            }
         
    }

    static long scaleOrderedKey(long r, long n) {
        return tools.scaleOrderedKey(r, n);
    }

    static long mix64(long x) {
        return tools.mix64(x);
    }
}
