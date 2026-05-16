package Tools;

import java.lang.foreign.MemorySegment;

import generator.DataGenerator;
import generator.DataMode;
import main.Apex;

public class verifier {
    public static void verifyLight(MemorySegment data, long n, DataMode mode) {
        verifyLight(data, n, mode, false);
    }

    public static void verifyLight(MemorySegment data, long n, DataMode mode, boolean announce) {
        if (mode == DataMode.EMPTY || n == 0) {
            if (announce) {
                System.out.println("VERIFIED");
            }
            return;
        }

        long prev = data.get(Apex.LONG, 0);

        long xorV = 0;
        long sumV = 0;

        for (long i = 0; i < n; i++) {
            long p = i << 4;
            long k = data.get(Apex.LONG, p);
            long v = data.get(Apex.LONG, p + 8);

            if (i > 0 && Long.compareUnsigned(prev, k) > 0) {
                throw new RuntimeException("ORDER FAIL at " + i);
            }

            if (k != DataGenerator.keyForMode(v, n, mode)) {
                throw new RuntimeException("PAIR FAIL at " + i);
            }

            if (v < 0 || v >= n) {
                throw new RuntimeException("RANGE FAIL at " + i + ": " + v);
            }

            xorV ^= v;
            sumV += v;

            prev = k;
        }

        long expectedXorV = tools.xorZeroToNMinusOne(n);
        long expectedSumV = tools.triangularZeroToNMinusOne(n);

        if (xorV != expectedXorV) {
            throw new RuntimeException("XOR FAIL");
        }

        if (sumV != expectedSumV) {
            throw new RuntimeException("SUM FAIL");
        }

        if (announce) {
            System.out.println("VERIFIED");
        }
    }

}
