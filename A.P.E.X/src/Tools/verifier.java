package Tools;

import java.lang.foreign.MemorySegment;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import generator.DataGenerator;
import generator.DataMode;
import main.Apex;

public class verifier {

    private static long mix64(long z) {
        z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdL;
        z = (z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53L;
        return z ^ (z >>> 33);
    }

    public static void verify(MemorySegment data, long n, DataMode mode) {
        verify(data, n, mode, true);
    }

    public static void verify(
            MemorySegment data,
            long n,
            DataMode mode,
            boolean announce) {

        long verifyStart = System.nanoTime();

        if (mode == DataMode.EMPTY || n == 0) {
            if (announce) {
                String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                System.out.println("VERIFIED EMPTY");
                System.out.println("Timestamp         : " + ts);
            }
            return;
        }

        long prev = data.get(Apex.LONG, 0);

        // --- 🚀 8-Way Split Execution Registers to Completely Saturate Hardware Pipelines ---
        long xor0 = 0L, xor1 = 0L, xor2 = 0L, xor3 = 0L, xor4 = 0L, xor5 = 0L, xor6 = 0L, xor7 = 0L;
        long sum0 = 0L, sum1 = 0L, sum2 = 0L, sum3 = 0L, sum4 = 0L, sum5 = 0L, sum6 = 0L, sum7 = 0L;
        long hV0  = 0L, hV1  = 0L, hV2  = 0L, hV3  = 0L, hV4  = 0L, hV5  = 0L, hV6  = 0L, hV7  = 0L;
        long hKV0 = 0L, hKV1 = 0L, hKV2 = 0L, hKV3 = 0L, hKV4 = 0L, hKV5 = 0L, hKV6 = 0L, hKV7 = 0L;

        long min0 = Long.MAX_VALUE, min1 = Long.MAX_VALUE, min2 = Long.MAX_VALUE, min3 = Long.MAX_VALUE;
        long min4 = Long.MAX_VALUE, min5 = Long.MAX_VALUE, min6 = Long.MAX_VALUE, min7 = Long.MAX_VALUE;
        long max0 = Long.MIN_VALUE, max1 = Long.MIN_VALUE, max2 = Long.MIN_VALUE, max3 = Long.MIN_VALUE;
        long max4 = Long.MIN_VALUE, max5 = Long.MIN_VALUE, max6 = Long.MIN_VALUE, max7 = Long.MIN_VALUE;

        long failFlags = 0L;

        // 8 records * 16 bytes per record = 128 bytes total loop stride footprint
        long strideBytes = 8L * Apex.RECORD_BYTES;
        long endBytes = n << 4;
        long unrolledEnd = endBytes - strideBytes;

        long p = 0L;

        // --- 🚀 8-Way Unrolled Memory Stride Core Loop ---
        while (p <= unrolledEnd) {
            long p0 = p;
            long p1 = p0 + 16;
            long p2 = p1 + 16;
            long p3 = p2 + 16;
            long p4 = p3 + 16;
            long p5 = p4 + 16;
            long p6 = p5 + 16;
            long p7 = p6 + 16;

            long k0 = data.get(Apex.LONG, p0);      long v0 = data.get(Apex.LONG, p0 + 8);
            long k1 = data.get(Apex.LONG, p1);      long v1 = data.get(Apex.LONG, p1 + 8);
            long k2 = data.get(Apex.LONG, p2);      long v2 = data.get(Apex.LONG, p2 + 8);
            long k3 = data.get(Apex.LONG, p3);      long v3 = data.get(Apex.LONG, p3 + 8);
            long k4 = data.get(Apex.LONG, p4);      long v4 = data.get(Apex.LONG, p4 + 8);
            long k5 = data.get(Apex.LONG, p5);      long v5 = data.get(Apex.LONG, p5 + 8);
            long k6 = data.get(Apex.LONG, p6);      long v6 = data.get(Apex.LONG, p6 + 8);
            long k7 = data.get(Apex.LONG, p7);      long v7 = data.get(Apex.LONG, p7 + 8);

            // 1. ORDER COMPLETENESS SCANNING
            int cmp01 = Long.compareUnsigned(k0, k1);
            int cmp12 = Long.compareUnsigned(k1, k2);
            int cmp23 = Long.compareUnsigned(k2, k3);
            int cmp34 = Long.compareUnsigned(k3, k4);
            int cmp45 = Long.compareUnsigned(k4, k5);
            int cmp56 = Long.compareUnsigned(k5, k6);
            int cmp67 = Long.compareUnsigned(k6, k7);
            if ((p > 0 && Long.compareUnsigned(prev, k0) > 0) || cmp01 > 0 || cmp12 > 0 || cmp23 > 0 || cmp34 > 0 || cmp45 > 0 || cmp56 > 0 || cmp67 > 0) {
                failFlags |= 1L;
            }

            // 2. PAIR IDENTITY FUNCTION CHECKS
            if (k0 != DataGenerator.keyForMode(v0, n, mode) || 
                k1 != DataGenerator.keyForMode(v1, n, mode) || 
                k2 != DataGenerator.keyForMode(v2, n, mode) || 
                k3 != DataGenerator.keyForMode(v3, n, mode) ||
                k4 != DataGenerator.keyForMode(v4, n, mode) ||
                k5 != DataGenerator.keyForMode(v5, n, mode) ||
                k6 != DataGenerator.keyForMode(v6, n, mode) ||
                k7 != DataGenerator.keyForMode(v7, n, mode)) {
                failFlags |= 2L;
            }

            // 3. LOGICAL BUFFER VALUES CHANNELS
            if (v0 < 0 || v0 >= n || v1 < 0 || v1 >= n || v2 < 0 || v2 >= n || v3 < 0 || v3 >= n ||
                v4 < 0 || v4 >= n || v5 < 0 || v5 >= n || v6 < 0 || v6 >= n || v7 < 0 || v7 >= n) {
                failFlags |= 4L;
            }

            // 4. ACCUMULATE INDEPENDENT PARALLEL SLOTS
            xor0 ^= v0; sum0 += v0; hV0 ^= mix64(v0); hKV0 ^= mix64(k0 ^ (v0 * 0x9E3779B97F4A7C15L));
            xor1 ^= v1; sum1 += v1; hV1 ^= mix64(v1); hKV1 ^= mix64(k1 ^ (v1 * 0x9E3779B97F4A7C15L));
            xor2 ^= v2; sum2 += v2; hV2 ^= mix64(v2); hKV2 ^= mix64(k2 ^ (v2 * 0x9E3779B97F4A7C15L));
            xor3 ^= v3; sum3 += v3; hV3 ^= mix64(v3); hKV3 ^= mix64(k3 ^ (v3 * 0x9E3779B97F4A7C15L));
            xor4 ^= v4; sum4 += v4; hV4 ^= mix64(v4); hKV4 ^= mix64(k4 ^ (v4 * 0x9E3779B97F4A7C15L));
            xor5 ^= v5; sum5 += v5; hV5 ^= mix64(v5); hKV5 ^= mix64(k5 ^ (v5 * 0x9E3779B97F4A7C15L));
            xor6 ^= v6; sum6 += v6; hV6 ^= mix64(v6); hKV6 ^= mix64(k6 ^ (v6 * 0x9E3779B97F4A7C15L));
            xor7 ^= v7; sum7 += v7; hV7 ^= mix64(v7); hKV7 ^= mix64(k7 ^ (v7 * 0x9E3779B97F4A7C15L));

            // 5. PARALLEL MIN-MAX VALUE TRACKING
            if (Long.compareUnsigned(k0, min0) < 0) min0 = k0;
            if (Long.compareUnsigned(k1, min1) < 0) min1 = k1;
            if (Long.compareUnsigned(k2, min2) < 0) min2 = k2;
            if (Long.compareUnsigned(k3, min3) < 0) min3 = k3;
            if (Long.compareUnsigned(k4, min4) < 0) min4 = k4;
            if (Long.compareUnsigned(k5, min5) < 0) min5 = k5;
            if (Long.compareUnsigned(k6, min6) < 0) min6 = k6;
            if (Long.compareUnsigned(k7, min7) < 0) min7 = k7;

            if (Long.compareUnsigned(k0, max0) > 0) max0 = k0;
            if (Long.compareUnsigned(k1, max1) > 0) max1 = k1;
            if (Long.compareUnsigned(k2, max2) > 0) max2 = k2;
            if (Long.compareUnsigned(k3, max3) > 0) max3 = k3;
            if (Long.compareUnsigned(k4, max4) > 0) max4 = k4;
            if (Long.compareUnsigned(k5, max5) > 0) max5 = k5;
            if (Long.compareUnsigned(k6, max6) > 0) max6 = k6;
            if (Long.compareUnsigned(k7, max7) > 0) max7 = k7;

            prev = k7;
            p += strideBytes;
        }

        // Aggregate 8 independent lane streams back into global metrics variables
        long xorV   = xor0 ^ xor1 ^ xor2 ^ xor3 ^ xor4 ^ xor5 ^ xor6 ^ xor7;
        long sumV   = sum0 + sum1 + sum2 + sum3 + sum4 + sum5 + sum6 + sum7;
        long hashV  = hV0  ^ hV1  ^ hV2  ^ hV3  ^ hV4  ^ hV5  ^ hV6  ^ hV7;
        long hashKV = hKV0 ^ hKV1 ^ hKV2 ^ hKV3 ^ hKV4 ^ hKV5 ^ hKV6 ^ hKV7;

        long minKey = Math.min(Math.min(Math.min(min0, min1), Math.min(min2, min3)), Math.min(Math.min(min4, min5), Math.min(min6, min7)));
        long maxKey = Math.max(Math.max(Math.max(max0, max1), Math.max(max2, max3)), Math.max(Math.max(max4, max5), Math.max(max6, max7)));

        // --- 🛬 Residual Scalar Tail Pass ---
        long i = p >>> 4;
        for (; i < n; i++) {
            long currentPos = i << 4;
            long k = data.get(Apex.LONG, currentPos);
            long v = data.get(Apex.LONG, currentPos + 8);

            if (i > 0 && Long.compareUnsigned(prev, k) > 0) failFlags |= 1L;
            if (k != DataGenerator.keyForMode(v, n, mode)) failFlags |= 2L;
            if (v < 0 || v >= n) failFlags |= 4L;

            xorV ^= v;
            sumV += v;
            hashV ^= mix64(v);
            hashKV ^= mix64(k ^ (v * 0x9E3779B97F4A7C15L));

            if (Long.compareUnsigned(k, minKey) < 0) minKey = k;
            if (Long.compareUnsigned(k, maxKey) > 0) maxKey = k;

            prev = k;
        }

        // --- Check Permutation Targets Checksums ---
        long expectedXorV = tools.xorZeroToNMinusOne(n);
        long expectedSumV = tools.triangularZeroToNMinusOne(n);

        if (xorV != expectedXorV)  failFlags |= 8L;
        if (sumV != expectedSumV)  failFlags |= 16L;

        // --- Throw Validation Errors If Broken ---
        if (failFlags != 0L) {
            StringBuilder sb = new StringBuilder();
            sb.append("VERIFICATION FAILED\n");
            if ((failFlags & 1L)  != 0L) sb.append(" - ORDER FAIL\n");
            if ((failFlags & 2L)  != 0L) sb.append(" - PAIR FAIL\n");
            if ((failFlags & 4L)  != 0L) sb.append(" - RANGE FAIL\n");
            if ((failFlags & 8L)  != 0L) sb.append(" - XOR FAIL\n");
            if ((failFlags & 16L) != 0L) sb.append(" - SUM FAIL\n");
            throw new RuntimeException(sb.toString());
        }

        long verifyEnd = System.nanoTime();
        double seconds = (verifyEnd - verifyStart) / 1_000_000_000.0;
        double mps = (n / seconds) / 1_000_000.0;

        if (announce) {
            String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            System.out.println("VERIFIED");
            System.out.println("Timestamp         : " + ts);
            System.out.printf("  order            : PASS%n");
            System.out.printf("  pair integrity   : PASS%n");
            System.out.printf("  range integrity  : PASS%n");
            System.out.printf("  xor              : 0x%016X%n", xorV);
            System.out.printf("  sum              : 0x%016X%n", sumV);
            System.out.printf("  hash(v)          : 0x%016X%n", hashV);
            System.out.printf("  hash(k,v)        : 0x%016X%n", hashKV);
            System.out.printf("  min key          : 0x%016X%n", minKey);
            System.out.printf("  max key          : 0x%016X%n", maxKey);
            System.out.printf("Verification       %.3f sec | %.2f M rec/sec%n", seconds, mps);
        }
    }
}
