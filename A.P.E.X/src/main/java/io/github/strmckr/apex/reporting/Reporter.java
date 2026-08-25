package io.github.strmckr.apex.reporting;

import java.util.Locale;

public interface Reporter {
    boolean isEnabled();

    void println();

    void println(String message);

    void printf(String format, Object... args);

    void printf(Locale locale, String format, Object... args);

    default void close() {
    }
}
