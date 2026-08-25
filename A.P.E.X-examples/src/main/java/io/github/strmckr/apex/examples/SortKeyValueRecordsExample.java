package io.github.strmckr.apex.examples;

import java.lang.foreign.Arena;

import io.github.strmckr.apex.Apex;
import io.github.strmckr.apex.Configuration;
import io.github.strmckr.apex.reporting.Reporter;
import io.github.strmckr.apex.reporting.Reporters;

public final class SortKeyValueRecordsExample {

    private SortKeyValueRecordsExample() {
    }

    public static void main(String[] args) throws Exception {
        Reporter reporter = Reporters.standard();

        try (Arena arena = Arena.ofShared()) {
            long[] keys = { 5L, 1L, 3L };
            long[] rowIds = { 50L, 10L, 30L };

            Apex.Data data = Apex.Data.fromKeyValueArrays(keys, rowIds, arena);
            Apex.Result result = Apex.sort(data, Configuration.fromCommands("threads=2", "reporting=off"));

            for (long i = 0; i < result.recordCount(); i++) {
                reporter.println(Apex.keyAt(result.records(), i) + ":" + Apex.valueAt(result.records(), i));
            }
        }
    }
}
