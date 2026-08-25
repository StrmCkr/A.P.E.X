package io.github.strmckr.apex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import org.junit.jupiter.api.Test;

import io.github.strmckr.apex.generator.DataMode;

class ApexFacadeTest {

    @Test
    void sortsGeneratedDataWithExplicitConfiguration() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            Apex.Data data = Apex.Data.generated(DataMode.RANDOM, 128L, arena);
            Configuration configuration = Configuration.fromCommands(
                    "threads=2",
                    "config=13,12,128",
                    "reporting=off"
            );

            Apex.Result result = Apex.sortAndVerify(data, configuration);

            assertEquals(128L, result.recordCount());
            assertEquals(DataMode.RANDOM, result.generatedMode());
            assertSorted(result.records(), result.recordCount());
        }
    }

    @Test
    void sortsCustomKeyArrays() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            Apex.Data data = Apex.Data.fromKeys(new long[] { 5L, 1L, 3L, 0L }, arena);

            Apex.Result result = Apex.sort(data, Configuration.fromCommands("threads=2", "reporting=off"));

            assertEquals(4L, result.recordCount());
            assertFalse(result.hasGeneratedMode());
            assertSorted(result.records(), result.recordCount());
        }
    }

    @Test
    void sortsPackedLongPairs() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            Apex.Data data = Apex.Data.fromPackedLongPairs(new long[] { 5L, 50L, 1L, 10L, 3L, 30L }, arena);

            Apex.Result result = Apex.sort(data, Configuration.fromCommands("threads=2", "reporting=off"));

            assertEquals(3L, result.recordCount());
            assertSorted(result.records(), result.recordCount());
        }
    }

    @Test
    void rejectsOddPackedLongPairs() {
        try (Arena arena = Arena.ofShared()) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> Apex.Data.fromPackedLongPairs(new long[] { 5L, 50L, 1L }, arena)
            );
        }
    }

    @Test
    void rejectsRawSegmentsThatAreNotExactPackedRecordSize() {
        try (Arena arena = Arena.ofShared()) {
            long records = 4L;
            MemorySegment source = arena.allocate(Apex.bytesForRecordCount(records) + Long.BYTES, 64);
            MemorySegment destination = arena.allocate(Apex.bytesForRecordCount(records), 64);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> Apex.Data.ofPackedRecords(source, destination, records)
            );
        }
    }

    @Test
    void rejectsOverlappingRawSegments() {
        try (Arena arena = Arena.ofShared()) {
            long records = 4L;
            long bytes = Apex.bytesForRecordCount(records);
            MemorySegment backing = arena.allocate(bytes + Long.BYTES, 64);
            MemorySegment source = backing.asSlice(0L, bytes);
            MemorySegment destination = backing.asSlice(Long.BYTES, bytes);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> Apex.Data.ofPackedRecords(source, destination, records)
            );
        }
    }

    @Test
    void rejectsConfinedArenaSegments() {
        try (Arena arena = Arena.ofConfined()) {
            long records = 4L;
            long bytes = Apex.bytesForRecordCount(records);
            MemorySegment source = arena.allocate(bytes, 64);
            MemorySegment destination = arena.allocate(bytes, 64);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> Apex.Data.ofPackedRecords(source, destination, records)
            );
        }
    }

    @Test
    void rejectsReadOnlyRawSegments() {
        try (Arena arena = Arena.ofShared()) {
            long records = 4L;
            long bytes = Apex.bytesForRecordCount(records);
            MemorySegment source = arena.allocate(bytes, 64).asReadOnly();
            MemorySegment destination = arena.allocate(bytes, 64);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> Apex.Data.ofPackedRecords(source, destination, records)
            );
        }
    }

    private static void assertSorted(MemorySegment records, long count) {
        for (long i = 1; i < count; i++) {
            long previous = Apex.keyAt(records, i - 1L);
            long current = Apex.keyAt(records, i);

            if (Long.compareUnsigned(previous, current) > 0) {
                throw new AssertionError("record " + i + " out of order");
            }
        }
    }
}
