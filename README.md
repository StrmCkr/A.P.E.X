# A.P.E.X.

A.P.E.X. is a high-performance Java sorting framework for large fixed-width
64-bit key/value record datasets. It uses descriptor-driven radix planning,
parallel scatter, per-bucket dispatch, tuple projection, tiny-sort fallbacks,
and local refinement to sort unsigned 64-bit keys by default, with an optional
signed-key ordering mode, while avoiding unnecessary passes over bits that are
already resolved.

The project also includes an interactive browser visualizer that explains the
execution plan: source array, MSD scatter, tiny routes, tuple routes, LSD
refinement, global/bucket reverse paths, and final sorted placement.

## Name

**A - Adaptive**  
Execution changes with the observed input structure: radix geometry, bucket
routes, tuple use, tiny thresholds, and refinement strategy are selected from
the data rather than fixed up front.

**P - Parallel**  
Histogramming, scatter, bucket refinement, and work scheduling are designed for
multi-core execution.

**E - Extremal**  
A.P.E.X. uses per-bucket extremal descriptors, especially bitwise `OR` and
bitwise `AND`, to identify which key bits still vary. The variable-bit mask is:

```text
VBM = OR ^ AND
```

Only unresolved bits continue into refinement.

**X - Dispatch**  
Buckets are routed to the cheapest applicable path: already done, global
reverse, bucket reverse, tiny sort, tuple projection, LSD cycles, or LSD with a
tuple tail.

A.P.E.X. sorts packed records:

```text
8-byte key + 8-byte value = 16 bytes per record
```

The key is sorted. The value is carried with the key and is normally a row id,
original index, or payload pointer.

Commercial licensing note: A.P.E.X. is available under AGPLv3 for open-source
use and under a proprietary commercial license for closed-source or enterprise
use. Contact `125188053+StrmCkr@users.noreply.github.com` for commercial terms.

## Requirements

- JDK 25 or newer
- Maven
- Preview features enabled
- `jdk.incubator.vector`

Large runs need enough heap and direct memory for source and destination
buffers. A rough record-buffer estimate is:

```text
recordCount * 16 bytes * 2
```

## Project Layout

```text
pom.xml                         Maven parent project
A.P.E.X/                        Core library and CLI jar
A.P.E.X-benchmark/              Comparison benchmark harness
A.P.E.X-jmh/                    Standard JMH benchmarks
A.P.E.X-examples/               Runnable library usage examples
documents/                      Project documentation
visualizer/                     Static browser visualizer
```

Core source packages use the Maven group namespace:

```text
A.P.E.X/src/main/java/io/github/strmckr/apex/
```

## Build

```bash
mvn package
```

The default build is core-only and writes:

```text
A.P.E.X/target/apex-1.0.0-SNAPSHOT.jar
```

Build optional tools explicitly:

```bash
mvn -Ptools package
```

Tool outputs:

```text
A.P.E.X-benchmark/target/apex-benchmark-1.0.0-SNAPSHOT.jar
A.P.E.X-jmh/target/benchmarks.jar
A.P.E.X-examples/target/apex-examples-1.0.0-SNAPSHOT.jar
```

## Run In Eclipse

Import the root project as a Maven project, then use the included launchers:

```text
Run Apex.launch                 Default A.P.E.X run
Run Apex Sweep 100M.launch      Every defined data mode at 100M records
Run Benchmark.launch            Full comparison benchmark triage
Run JMH.launch                  Standard JMH benchmark smoke run
Run Sort Keys Example.launch    Small key-only library example
Run Sort Key Value Records Example.launch
                                Small key/value record library example
```

Use these launchers instead of `Run As > Java Application` when running from
Eclipse. They compile the Maven module first and attach the correct runtime
classpath.

## Use As A Library

Maven dependency:

```xml
<dependency>
    <groupId>io.github.strmckr</groupId>
    <artifactId>apex</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

Sort generated data:

```java
import java.lang.foreign.Arena;

import io.github.strmckr.apex.Apex;
import io.github.strmckr.apex.Configuration;
import io.github.strmckr.apex.generator.DataMode;

try (Arena arena = Arena.ofShared()) {
    Apex.Data data = Apex.Data.generated(DataMode.RANDOM, 1_000_000L, arena);
    Apex.Result result = Apex.sortAndVerify(data, Configuration.defaultConfiguration());
}
```

Sort keys only:

```java
try (Arena arena = Arena.ofShared()) {
    long[] keys = { 5L, 1L, 3L };
    Apex.Data data = Apex.Data.fromKeys(keys, arena);
    Apex.Result result = Apex.sort(data, Configuration.defaultConfiguration());
}
```

Sort key/value records:

```java
try (Arena arena = Arena.ofShared()) {
    long[] keys = { 5L, 1L, 3L };
    long[] rowIds = { 50L, 10L, 30L };

    Apex.Data data = Apex.Data.fromKeyValueArrays(keys, rowIds, arena);
    Configuration config = Configuration.fromCommands("threads=16", "reporting=off");
    Apex.Result result = Apex.sort(data, config);

    for (long i = 0; i < result.recordCount(); i++) {
        long key = Apex.keyAt(result.records(), i);
        long rowId = Apex.valueAt(result.records(), i);
    }
}
```

Use raw packed memory only when both buffers are exactly packed 16-byte records:

```java
long bytes = Apex.bytesForRecordCount(recordCount);
Apex.Data data = Apex.Data.ofPackedRecords(
        sourceSegment.asSlice(0, bytes),
        destinationSegment.asSlice(0, bytes),
        recordCount
);
```

Raw segments must be exact-size, writable, non-overlapping, and accessible by
worker threads. Use `Arena.ofShared()` for A.P.E.X data.

## Run The CLI

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -Xmx16G -XX:MaxDirectMemorySize=80g -jar A.P.E.X/target/apex-1.0.0-SNAPSHOT.jar mode=RANDOM records=10m threads=16
```

Useful options:

```text
mode=RANDOM
modes=RANDOM,DUPLICATES
records=1m,10m,100m
threads=auto
config=13,12,128
reporting=summary
reporting=standard
reporting=off
reportFile=target/apex-run.txt
```

If `config` is omitted, the CLI auto-tunes the MSD/LSD/tiny settings for the
selected run.

The default reporting mode is `summary`. Use `reporting=standard` for the full
planner and verifier diagnostics, `reportFile=...` to save the same report to a
file, or `reporting=off` when embedding A.P.E.X. as a library.

## Modular Runtime Switches

| Area | Option |
| --- | --- |
| Reporting | `reporting=summary`, `reporting=standard`, `reporting=off`, `reportFile=...`, `-Dapex.reportFile=...` |
| Threads | `threads=auto`, `threads=16`, `-Dapex.threads=16` |
| Key order | `signed=true`, `keyOrder=signed` |
| Fixed config | `config=13,12,128` |
| Auto-tune ranges | `msd=11..13`, `lsd=12..17`, `tiny=32..1024` |
| Tuple behavior | `tupleBits`, `tuplePacking`, `staggerTuples` |
| Work scheduling | `workStealing`, `workBatch`, `largePermits` |

## Benchmarks

Comparison harness:

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector -Xmx16G -XX:MaxDirectMemorySize=80g -cp "A.P.E.X/target/classes:A.P.E.X-benchmark/target/classes:A.P.E.X-benchmark/target/dependency/*" io.github.strmckr.apex.benchmark.SortBenchmark triage=full threads=auto config=12,13,128 tuplePacking=true tupleBits=16
```

Use `triage=smoke` for a short development check, or `triage=expanded` for the
larger all-baseline table.

Benchmark runs write a timestamped text report and CSV table under
`A.P.E.X-benchmark/target/benchmark-reports/` when launched from the repository
root. Use `reportFile=...`, `csvFile=...`, or `reportDir=...` to choose another
location. Use `reportFile=off` or `csvFile=off` to disable either artifact.

Standard JMH harness:

```bash
java --enable-preview --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow --add-modules jdk.incubator.vector -jar A.P.E.X-jmh/target/benchmarks.jar -p=records=100000 -p=mode=RANDOM -p=threads=2 ApexSortJmhBenchmark.sortPipeline
```

Use the comparison harness for A.P.E.X. versus other sorters. Use JMH for
standard JVM benchmarking of the A.P.E.X. pipeline itself.

## Documentation

- [Usage Guide](documents/usage.md)
- [Benchmark Guide](documents/benchmark.md)
- [Data and Descriptors](documents/data-descriptors.md)
- [Operation Model](documents/operation-execution.md)
- [Visualizer](visualizer/index.html)

## License

See [LICENSE](LICENSE).
