package io.github.strmckr.apex.examples;

import java.lang.foreign.Arena;

import io.github.strmckr.apex.Apex;
import io.github.strmckr.apex.Configuration;
import io.github.strmckr.apex.reporting.Reporter;
import io.github.strmckr.apex.reporting.Reporters;

public final class SortKeysExample {

    private SortKeysExample() {
    }

    public static void main(String[] args) throws Exception {
        Reporter reporter = Reporters.standard();

        try (Arena arena = Arena.ofShared()) {
            Apex.Data data = Apex.Data.fromKeys(new long[] { 5L, 1L, 3L }, arena);
            Apex.Result result = Apex.sort(data, Configuration.fromCommands("threads=2", "reporting=off"));

            for (long i = 0; i < result.recordCount(); i++) {
                reporter.println(Apex.keyAt(result.records(), i) + ":" + Apex.valueAt(result.records(), i));
            }
        }
    }
}
