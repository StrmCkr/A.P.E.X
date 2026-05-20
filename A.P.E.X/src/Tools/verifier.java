package Tools;

import java.lang.foreign.MemorySegment;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import generator.DataGenerator;
import generator.DataMode;
import main.Apex;

public class verifier {

    // -------------------------------------------------------------
    // 64-bit avalanche mixer
    // -------------------------------------------------------------

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

                String ts = LocalDateTime.now()
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

                System.out.println("VERIFIED EMPTY");
                System.out.println("Timestamp         : " + ts);
            }

            return;
        }

        long prev = data.get(Apex.LONG, 0);

        long xorV = 0;
        long sumV = 0;

        long hashV = 0;
        long hashKV = 0;

        long minKey = Long.MAX_VALUE;
        long maxKey = Long.MIN_VALUE;

        long failFlags = 0;

        for (long i = 0; i < n; i++) {

            long p = i << 4;

            long k = data.get(Apex.LONG, p);
            long v = data.get(Apex.LONG, p + 8);

            // ---------------------------------------------------------
            // ORDER CHECK
            // ---------------------------------------------------------

            if (i > 0 && Long.compareUnsigned(prev, k) > 0) {
                failFlags |= 1L;
            }

            // ---------------------------------------------------------
            // PAIR CHECK
            // ---------------------------------------------------------

            long expectedKey = DataGenerator.keyForMode(v, n, mode);

            if (k != expectedKey) {
                failFlags |= 2L;
            }

            // ---------------------------------------------------------
            // RANGE CHECK
            // ---------------------------------------------------------

            if (v < 0 || v >= n) {
                failFlags |= 4L;
            }

            // ---------------------------------------------------------
            // PERMUTATION CHECKS
            // ---------------------------------------------------------

            xorV ^= v;
            sumV += v;

            hashV ^= mix64(v);
            hashKV ^= mix64(k ^ (v * 0x9E3779B97F4A7C15L));

            // ---------------------------------------------------------
            // KEY RANGE TRACKING
            // ---------------------------------------------------------

            if (Long.compareUnsigned(k, minKey) < 0) {
                minKey = k;
            }

            if (Long.compareUnsigned(k, maxKey) > 0) {
                maxKey = k;
            }

            prev = k;
        }

        // -------------------------------------------------------------
        // EXPECTED VALUES
        // -------------------------------------------------------------

        long expectedXorV = tools.xorZeroToNMinusOne(n);
        long expectedSumV = tools.triangularZeroToNMinusOne(n);

        if (xorV != expectedXorV) {
            failFlags |= 8L;
        }

        if (sumV != expectedSumV) {
            failFlags |= 16L;
        }

        // -------------------------------------------------------------
        // FAILURE REPORTING
        // -------------------------------------------------------------

        if (failFlags != 0) {

            StringBuilder sb = new StringBuilder();

            sb.append("VERIFICATION FAILED\n");

            if ((failFlags & 1L) != 0) {
                sb.append(" - ORDER FAIL\n");
            }

            if ((failFlags & 2L) != 0) {
                sb.append(" - PAIR FAIL\n");
            }

            if ((failFlags & 4L) != 0) {
                sb.append(" - RANGE FAIL\n");
            }

            if ((failFlags & 8L) != 0) {
                sb.append(" - XOR FAIL\n");
            }

            if ((failFlags & 16L) != 0) {
                sb.append(" - SUM FAIL\n");
            }

            throw new RuntimeException(sb.toString());
        }

        // -------------------------------------------------------------
        // TIMING
        // -------------------------------------------------------------

        long verifyEnd = System.nanoTime();

        double seconds = (verifyEnd - verifyStart) / 1_000_000_000.0;
        double mps = (n / seconds) / 1_000_000.0;

        // -------------------------------------------------------------
        // REPORT
        // -------------------------------------------------------------

        if (announce) {

            String ts = LocalDateTime.now()
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

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

            System.out.printf(
                    "Verification       %.3f sec | %.2f M rec/sec%n",
                    seconds,
                    mps);
        }
    }
}