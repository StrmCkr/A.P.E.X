package Tools;

import java.lang.foreign.MemorySegment;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

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

        // Global reduction accumulators
        AtomicLong globalXorV = new AtomicLong(0L);
        AtomicLong globalSumV = new AtomicLong(0L);
        AtomicLong globalHashV = new AtomicLong(0L);
        AtomicLong globalHashKV = new AtomicLong(0L);
        AtomicLong globalFailFlags = new AtomicLong(0L);
        
        AtomicLong globalMinKey = new AtomicLong(Long.MAX_VALUE);
        AtomicLong globalMaxKey = new AtomicLong(Long.MIN_VALUE);

        int tasks = Apex.THREADS;
        long chunk = n / tasks;
        ArrayList<Future<?>> futures = new ArrayList<>(tasks);

        // --- 🚀 Launching Parallel Thread-Local Verification Map Slices ---
        for (int t = 0; t < tasks; t++) {
            final int tid = t;
            futures.add(Apex.POOL.submit(() -> {
                long startRecord = tid * chunk;
                long endRecord = (tid == tasks - 1) ? n : startRecord + chunk;

                if (startRecord >= endRecord) return;

                // Local independent tracking registers to eliminate inter-core thread contention
                long localXor0 = 0L, localXor1 = 0L, localXor2 = 0L, localXor3 = 0L;
                long localSum0 = 0L, localSum1 = 0L, localSum2 = 0L, localSum3 = 0L;
                long localHv0  = 0L, localHv1  = 0L, localHv2  = 0L, localHv3  = 0L;
                long localHkv0 = 0L, localHkv1 = 0L, localHkv2 = 0L, localHkv3 = 0L;

                long localMin = Long.MAX_VALUE;
                long localMax = Long.MIN_VALUE;
                long localFailFlags = 0L;

                long baseBytes = startRecord << 4;
                long endBytes = endRecord << 4;
                
                // 4 records * 16 bytes per record = 64 bytes (1 perfect hardware cache line footprint)
                long strideBytes = 4L * Apex.RECORD_BYTES;
                long unrolledEnd = endBytes - strideBytes;

                long p = baseBytes;
                long prev = data.get(Apex.LONG, p);

                // Prime bounds tracking variables with initial records
                if (Long.compareUnsigned(prev, localMin) < 0) localMin = prev;
                if (Long.compareUnsigned(prev, localMax) > 0) localMax = prev;

                // Core 4-Way Stride Unrolled Cache Line Read Pipeline
                while (p <= unrolledEnd) {
                    long p0 = p;
                    long p1 = p0 + 16;
                    long p2 = p1 + 16;
                    long p3 = p2 + 16;

                    long k0 = data.get(Apex.LONG, p0);      long v0 = data.get(Apex.LONG, p0 + 8);
                    long k1 = data.get(Apex.LONG, p1);      long v1 = data.get(Apex.LONG, p1 + 8);
                    long k2 = data.get(Apex.LONG, p2);      long v2 = data.get(Apex.LONG, p2 + 8);
                    long k3 = data.get(Apex.LONG, p3);      long v3 = data.get(Apex.LONG, p3 + 8);

                    // 1. MONOTONIC SEQUENCE VALIDIATION
                    int cmp01 = Long.compareUnsigned(k0, k1);
                    int cmp12 = Long.compareUnsigned(k1, k2);
                    int cmp23 = Long.compareUnsigned(k2, k3);
                    // Only enforce continuous order scanning across cross-record chunks
                    if ((p > baseBytes && Long.compareUnsigned(prev, k0) > 0) || cmp01 > 0 || cmp12 > 0 || cmp23 > 0) {
                        localFailFlags |= 1L;
                    }

                    // 2. FUNCTIONAL INVERSE IDENTITY INTEGRITY VERIFICATION
                    if (k0 != DataGenerator.keyForMode(v0, n, mode) || 
                        k1 != DataGenerator.keyForMode(v1, n, mode) || 
                        k2 != DataGenerator.keyForMode(v2, n, mode) || 
                        k3 != DataGenerator.keyForMode(v3, n, mode)) {
                        localFailFlags |= 2L;
                    }

                    // 3. LOGICAL RECORD MEMORY SPACE CHANNELS
                    if (v0 < 0 || v0 >= n || v1 < 0 || v1 >= n || v2 < 0 || v2 >= n || v3 < 0 || v3 >= n) {
                        localFailFlags |= 4L;
                    }

                    // 4. PARALLEL PERMUTATION TRACKS
                    localXor0 ^= v0; localSum0 += v0; localHv0 ^= mix64(v0); localHkv0 ^= mix64(k0 ^ (v0 * 0x9E3779B97F4A7C15L));
                    localXor1 ^= v1; localSum1 += v1; localHv1 ^= mix64(v1); localHkv1 ^= mix64(k1 ^ (v1 * 0x9E3779B97F4A7C15L));
                    localXor2 ^= v2; localSum2 += v2; localHv2 ^= mix64(v2); localHkv2 ^= mix64(k2 ^ (v2 * 0x9E3779B97F4A7C15L));
                    localXor3 ^= v3; localSum3 += v3; localHv3 ^= mix64(v3); localHkv3 ^= mix64(k3 ^ (v3 * 0x9E3779B97F4A7C15L));

                    // 5. CACHE LOCAL RECONNAISSANCE MIN-MAX SCAN
                    if (Long.compareUnsigned(k0, localMin) < 0) localMin = k0;
                    if (Long.compareUnsigned(k1, localMin) < 0) localMin = k1;
                    if (Long.compareUnsigned(k2, localMin) < 0) localMin = k2;
                    if (Long.compareUnsigned(k3, localMin) < 0) localMin = k3;

                    if (Long.compareUnsigned(k0, localMax) > 0) localMax = k0;
                    if (Long.compareUnsigned(k1, localMax) > 0) localMax = k1;
                    if (Long.compareUnsigned(k2, localMax) > 0) localMax = k2;
                    if (Long.compareUnsigned(k3, localMax) > 0) localMax = k3;

                    prev = k3;
                    p += strideBytes;
                }

                // Handle remaining thread slice tail residues
                long currentIdx = p >>> 4;
                for (; currentIdx < endRecord; currentIdx++) {
                    long currentPos = currentIdx << 4;
                    long k = data.get(Apex.LONG, currentPos);
                    long v = data.get(Apex.LONG, currentPos + 8);

                    if (currentIdx > startRecord && Long.compareUnsigned(prev, k) > 0) localFailFlags |= 1L;
                    if (k != DataGenerator.keyForMode(v, n, mode)) localFailFlags |= 2L;
                    if (v < 0 || v >= n) localFailFlags |= 4L;

                    localXor0 ^= v;
                    localSum0 += v;
                    localHv0  ^= mix64(v);
                    localHkv0 ^= mix64(k ^ (v * 0x9E3779B97F4A7C15L));

                    if (Long.compareUnsigned(k, localMin) < 0) localMin = k;
                    if (Long.compareUnsigned(k, localMax) > 0) localMax = k;

                    prev = k;
                }

                // --- 🛡️ Execute Cross-Core Border Sequence Integrity Lock Scan ---
                // Ensures terminal elements of thread slices are sorted relative to neighbor beginnings
                if (endRecord < n) {
                    long nextK = data.get(Apex.LONG, endRecord << 4);
                    if (Long.compareUnsigned(prev, nextK) > 0) {
                        localFailFlags |= 1L;
                    }
                }

                // Combine local unrolled registers back into the thread-local instance variables
                long threadXor = localXor0 ^ localXor1 ^ localXor2 ^ localXor3;
                long threadSum = localSum0 + localSum1 + localSum2 + localSum3;
                long threadHv  = localHv0  ^ localHv1  ^ localHv2  ^ localHv3;
                long threadHkv = localHkv0 ^ localHkv1 ^ localHkv2 ^ localHkv3;

                // --- 📥 Reduce Step: Safely accumulate local statistics into atomic global structures ---
                globalXorV.accumulateAndGet(threadXor, (a, b) -> a ^ b);
                globalSumV.addAndGet(threadSum);
                globalHashV.accumulateAndGet(threadHv, (a, b) -> a ^ b);
                globalHashKV.accumulateAndGet(threadHkv, (a, b) -> a ^ b);
                globalFailFlags.accumulateAndGet(localFailFlags, (a, b) -> a | b);

                // Thread-safe min/max reduction checks
                long currentMin;
                while (Long.compareUnsigned(localMin, currentMin = globalMinKey.get()) < 0) {
                    if (globalMinKey.compareAndSet(currentMin, localMin)) break;
                }
                long currentMax;
                while (Long.compareUnsigned(localMax, currentMax = globalMaxKey.get()) > 0) {
                    if (globalMaxKey.compareAndSet(currentMax, localMax)) break;
                }
            }));
        }

        // Synchronize and wait for all execution tasks to finalize
        // --- 🛡️ Fix applied: Bypassed external package mapping using native inline synchronization ---
        for (Future<?> future : futures) {
            try {
                future.get(); // Blocks securely until each individual parallel task completes execution
            } catch (Exception ex) {
                throw new RuntimeException("Parallel verification task loop execution failed", ex);
            }
        }

        long xorV = globalXorV.get();
        long sumV = globalSumV.get();
        long hashV = globalHashV.get();
        long hashKV = globalHashKV.get();
        long failFlags = globalFailFlags.get();
        long minKey = globalMinKey.get();
        long maxKey = globalMaxKey.get();

        // --- 📊 EVALUATE REDUCED PERMUTATION TARGETS ---
        long expectedXorV = tools.xorZeroToNMinusOne(n);
        long expectedSumV = tools.triangularZeroToNMinusOne(n);

        if (xorV != expectedXorV)  failFlags |= 8L;
        if (sumV != expectedSumV)  failFlags |= 16L;

        // --- FAILURE EXCEPTION ORCHESTRATION ---
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
