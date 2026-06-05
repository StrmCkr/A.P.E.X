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
        AtomicLong firstOrderFailIndex = new AtomicLong(Long.MAX_VALUE);
        AtomicLong firstOrderFailPreviousKey = new AtomicLong(0L);
        AtomicLong firstOrderFailKey = new AtomicLong(0L);
        
        // Primed to bitwise hardware boundaries for unsigned evaluations
        AtomicLong globalMinKey = new AtomicLong(Apex.SIGNED_KEYS ? Long.MAX_VALUE : -1L);
        AtomicLong globalMaxKey = new AtomicLong(Apex.SIGNED_KEYS ? Long.MIN_VALUE : 0L);

        int tasks = Apex.THREADS;
        long chunk = n / tasks;
        ArrayList<Future<?>> futures = new ArrayList<>(tasks);

        // Track key boundaries across threads to ensure continuous sort order safely
        long[] threadFirstKeys = new long[tasks];
        long[] threadLastKeys = new long[tasks];
        boolean[] threadHasData = new boolean[tasks];

        // --- 🚀 Launching Parallel Thread-Local Verification Map Slices ---
        for (int t = 0; t < tasks; t++) {
            final int tid = t;
            futures.add(Apex.POOL.submit(() -> {
                long startRecord = tid * chunk;
                long endRecord = (tid == tasks - 1) ? n : startRecord + chunk;

                if (startRecord >= endRecord) return;

                threadHasData[tid] = true;

                // Local variables isolate computations and avoid cross-core cache contention
                long localXor = 0L;
                long localSum = 0L;
                long localHashV = 0L;
                long localHashKV = 0L;
                long localFailFlags = 0L;

                long baseBytes = startRecord << 4;
                long firstKey = data.get(Apex.LONG, baseBytes);
                threadFirstKeys[tid] = firstKey;

                long prev = firstKey;
                long localMin = firstKey;
                long localMax = firstKey;

                // Sequential core check track for the thread chunk
                for (long i = startRecord; i < endRecord; i++) {
                    long p = i << 4;
                    long k = data.get(Apex.LONG, p);
                    long v = data.get(Apex.LONG, p + 8);

                    // 1. ORDER CHECK
                    if (i > startRecord && tools.compareKeys(prev, k) > 0) {
                        localFailFlags |= 1L;
                        recordFirstOrderFailure(firstOrderFailIndex, firstOrderFailPreviousKey, firstOrderFailKey, i, prev, k);
                    }

                    // 2. PAIR CHECK
                    if (k != DataGenerator.keyForMode(v, n, mode)) {
                        localFailFlags |= 2L;
                    }

                    // 3. RANGE CHECK
                    if (v < 0 || v >= n) {
                        localFailFlags |= 4L;
                    }

                    // 4. PERMUTATION ACCUMULATIONS
                    localXor ^= v;
                    localSum += v;
                    localHashV ^= mix64(v);
                    localHashKV ^= mix64(k ^ (v * 0x9E3779B97F4A7C15L));

                    // 5. KEY RANGE TRACKING
                    if (tools.compareKeys(k, localMin) < 0) localMin = k;
                    if (tools.compareKeys(k, localMax) > 0) localMax = k;

                    prev = k;
                }

                // Register the final verified key of this thread slice
                threadLastKeys[tid] = prev;

                // --- 📥 Reduce Step: Atomically merge results into global tracking counters ---
                globalXorV.accumulateAndGet(localXor, (current, update) -> current ^ update);
                globalSumV.addAndGet(localSum);
                globalHashV.accumulateAndGet(localHashV, (current, update) -> current ^ update);
                globalHashKV.accumulateAndGet(localHashKV, (current, update) -> current ^ update);
                globalFailFlags.accumulateAndGet(localFailFlags, (current, update) -> current | update);

                // Unsigned thread-safe atomic min/max comparisons
                long currentMin;
                while (tools.compareKeys(localMin, currentMin = globalMinKey.get()) < 0) {
                    if (globalMinKey.compareAndSet(currentMin, localMin)) break;
                }
                long currentMax;
                while (tools.compareKeys(localMax, currentMax = globalMaxKey.get()) > 0) {
                    if (globalMaxKey.compareAndSet(currentMax, localMax)) break;
                }
            }));
        }

        // Inline synchronization loops block processing until all verification chunks finish safely
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception ex) {
                throw new RuntimeException("Parallel verification task block execution failed", ex);
            }
        }

        // --- 🛡️ Symmetrical Cross-Core Order Scan ---
        // Verifies sorted alignment between separate threads from registered boundary values
        long finalFailFlags = globalFailFlags.get();
        int lastActiveTid = -1;
        
        for (int t = 0; t < tasks; t++) {
            if (!threadHasData[t]) continue;
            if (lastActiveTid != -1) {
                // If the last key of the previous block is larger than the first key of the current block
                if (tools.compareKeys(threadLastKeys[lastActiveTid], threadFirstKeys[t]) > 0) {
                    finalFailFlags |= 1L; // Flag order failure safely
                    recordFirstOrderFailure(
                            firstOrderFailIndex,
                            firstOrderFailPreviousKey,
                            firstOrderFailKey,
                            firstThreadStartIndex(t, chunk),
                            threadLastKeys[lastActiveTid],
                            threadFirstKeys[t]
                    );
                }
            }
            lastActiveTid = t;
        }

        long xorV = globalXorV.get();
        long sumV = globalSumV.get();
        long hashV = globalHashV.get();
        long hashKV = globalHashKV.get();
        long minKey = globalMinKey.get();
        long maxKey = globalMaxKey.get();

        // Evaluate overall global checksum combinations
        long expectedXorV = tools.xorZeroToNMinusOne(n);
        long expectedSumV = tools.triangularZeroToNMinusOne(n);

        if (xorV != expectedXorV)  finalFailFlags |= 8L;
        if (sumV != expectedSumV)  finalFailFlags |= 16L;

        // --- FAILURE EXCEPTION ORCHESTRATION ---
        if (finalFailFlags != 0L) {
            StringBuilder sb = new StringBuilder();
            sb.append("VERIFICATION FAILED\n");
            if ((finalFailFlags & 1L)  != 0L) {
                sb.append(" - ORDER FAIL");
                long orderFail = firstOrderFailIndex.get();
                if (orderFail != Long.MAX_VALUE) {
                    sb.append(" at index ").append(orderFail)
                            .append(" prev=0x").append(String.format("%016X", firstOrderFailPreviousKey.get()))
                            .append(" key=0x").append(String.format("%016X", firstOrderFailKey.get()));
                }
                sb.append('\n');
            }
            if ((finalFailFlags & 2L)  != 0L) sb.append(" - PAIR FAIL\n");
            if ((finalFailFlags & 4L)  != 0L) sb.append(" - RANGE FAIL\n");
            if ((finalFailFlags & 8L)  != 0L) sb.append(" - XOR FAIL\n");
            if ((finalFailFlags & 16L) != 0L) sb.append(" - SUM FAIL\n");
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

    private static void recordFirstOrderFailure(
            AtomicLong failIndex,
            AtomicLong failPreviousKey,
            AtomicLong failKey,
            long index,
            long previousKey,
            long key
    ) {
        long current;
        while (index < (current = failIndex.get())) {
            if (failIndex.compareAndSet(current, index)) {
                failPreviousKey.set(previousKey);
                failKey.set(key);
                return;
            }
        }
    }

    private static long firstThreadStartIndex(int threadId, long chunk) {
        return (long) threadId * chunk;
    }
}
