package io.github.strmckr.apex.benchmark.jmh;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import io.github.strmckr.apex.config.RunOptions;
import io.github.strmckr.apex.config.SortConfig;
import io.github.strmckr.apex.engine.ApexEngine;
import io.github.strmckr.apex.generator.DataInitializer;
import io.github.strmckr.apex.generator.DataMode;
import io.github.strmckr.apex.reporting.Reporters;
import io.github.strmckr.apex.tools.Tools;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1, jvmArgsAppend = {
        "--enable-preview",
        "--enable-native-access=ALL-UNNAMED",
        "--add-modules=jdk.incubator.vector",
        "--sun-misc-unsafe-memory-access=allow",
        "-Dapex.reporting=off"
})
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
public class ApexSortJmhBenchmark {

    @Param({ "RANDOM", "DUPLICATES", "LOW_BITS_ONLY" })
    public String mode;

    @Param({ "100000" })
    public long records;

    @Param({ "13-12-128" })
    public String config;

    @Param({ "2" })
    public int threads;

    private Arena arena;
    private MemorySegment source;
    private MemorySegment destination;
    private DataMode dataMode;
    private SortConfig sortConfig;

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        Reporters.configure("off");
        dataMode = DataMode.valueOf(mode);
        sortConfig = RunOptions.parseConfig(jmhConfigValue(config));

        ApexEngine.THREADS = threads;
        ApexEngine.POOL = java.util.concurrent.Executors.newFixedThreadPool(ApexEngine.THREADS);

        arena = Arena.ofShared();
        long bytes = Tools.bytesForRecords(records);
        source = arena.allocate(bytes, 64);
        destination = arena.allocate(bytes, 64);
    }

    @Setup(Level.Invocation)
    public void setupInvocation() throws Exception {
        DataInitializer.initData(source, records, dataMode);
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        if (ApexEngine.POOL != null) {
            ApexEngine.POOL.shutdown();
            ApexEngine.POOL = null;
        }

        if (arena != null) {
            arena.close();
            arena = null;
        }
    }

    @Benchmark
    public MemorySegment sortPipeline() throws Exception {
        return ApexEngine.sortPipeline(source, destination, records, sortConfig);
    }

    private static String jmhConfigValue(String value) {
        return value.trim()
                .replace('-', ',')
                .replace(':', ',')
                .replace('/', ',');
    }
}
