package io.github.strmckr.apex.reporting;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Locale;
import java.util.Objects;

public final class Reporters {
    private static volatile Reporter current = standard(System.out);
    private static volatile String currentMode = "standard";

    private Reporters() {
    }

    public static String mode() {
        return currentMode;
    }

    public static Reporter current() {
        return current;
    }

    public static void set(Reporter reporter) {
        current = Objects.requireNonNull(reporter, "reporter");
    }

    public static void configure(String mode) {
        String normalized = normalizeMode(mode);
        currentMode = normalized;
        set(fromMode(normalized));
    }

    public static void configure(String mode, String reportFile) {
        String normalized = normalizeMode(mode);
        currentMode = normalized;

        if ("off".equals(normalized)) {
            set(off());
            return;
        }

        Reporter reporter = fromMode(normalized);
        if (!isDisabledFile(reportFile)) {
            reporter = tee(reporter, file(Path.of(reportFile.trim())));
        }
        set(reporter);
    }

    public static Reporter fromMode(String mode) {
        String normalized = normalizeMode(mode);
        if ("off".equals(normalized)) {
            return off();
        }
        return standard();
    }

    public static String normalizeMode(String mode) {
        if (mode == null || mode.isBlank()) {
            return "standard";
        }

        return switch (mode.trim().toLowerCase(Locale.ROOT).replace("-", "").replace("_", "")) {
            case "standard", "stdout", "console", "on", "true", "yes", "1" -> "standard";
            case "summary", "compact", "brief" -> "summary";
            case "off", "none", "quiet", "silent", "false", "no", "0" -> "off";
            default -> throw new IllegalArgumentException(
                    "Unknown reporting mode: " + mode + " (expected standard, summary, or off)"
            );
        };
    }

    public static Reporter standard() {
        return standard(System.out);
    }

    public static Reporter standard(PrintStream out) {
        return new PrintStreamReporter(out, false);
    }

    public static Reporter file(Path path) {
        return new PrintStreamReporter(openReportStream(path), true);
    }

    public static Reporter tee(Reporter first, Reporter second) {
        return new TeeReporter(first, second);
    }

    public static Reporter off() {
        return NoOpReporter.INSTANCE;
    }

    public static boolean isEnabled() {
        return current.isEnabled();
    }

    public static boolean isSummary() {
        return "summary".equals(currentMode);
    }

    public static boolean isDetailEnabled() {
        return "standard".equals(currentMode);
    }

    public static void println() {
        current.println();
    }

    public static void println(String message) {
        current.println(message);
    }

    public static void printf(String format, Object... args) {
        current.printf(format, args);
    }

    public static void printf(Locale locale, String format, Object... args) {
        current.printf(locale, format, args);
    }

    public static void close() {
        current.close();
    }

    public static boolean isDisabledFile(String reportFile) {
        if (reportFile == null || reportFile.isBlank()) {
            return true;
        }

        String normalized = reportFile.trim().toLowerCase(Locale.ROOT);
        return normalized.equals("off") ||
                normalized.equals("none") ||
                normalized.equals("false") ||
                normalized.equals("no") ||
                normalized.equals("0");
    }

    private static PrintStream openReportStream(Path path) {
        Objects.requireNonNull(path, "path");

        try {
            Path absolute = path.toAbsolutePath();
            Path parent = absolute.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }

            return new PrintStream(
                    new BufferedOutputStream(Files.newOutputStream(
                            absolute,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING,
                            StandardOpenOption.WRITE
                    )),
                    true,
                    StandardCharsets.UTF_8
            );
        } catch (IOException ex) {
            throw new UncheckedIOException("Unable to open report file: " + path, ex);
        }
    }

    private static final class PrintStreamReporter implements Reporter {
        private final PrintStream out;
        private final boolean closeOnClose;

        private PrintStreamReporter(PrintStream out, boolean closeOnClose) {
            this.out = Objects.requireNonNull(out, "out");
            this.closeOnClose = closeOnClose;
        }

        @Override
        public boolean isEnabled() {
            return true;
        }

        @Override
        public void println() {
            out.println();
        }

        @Override
        public void println(String message) {
            out.println(message);
        }

        @Override
        public void printf(String format, Object... args) {
            out.printf(format, args);
        }

        @Override
        public void printf(Locale locale, String format, Object... args) {
            out.printf(locale, format, args);
        }

        @Override
        public void close() {
            if (closeOnClose) {
                out.close();
            } else {
                out.flush();
            }
        }
    }

    private static final class TeeReporter implements Reporter {
        private final Reporter first;
        private final Reporter second;

        private TeeReporter(Reporter first, Reporter second) {
            this.first = Objects.requireNonNull(first, "first");
            this.second = Objects.requireNonNull(second, "second");
        }

        @Override
        public boolean isEnabled() {
            return first.isEnabled() || second.isEnabled();
        }

        @Override
        public void println() {
            first.println();
            second.println();
        }

        @Override
        public void println(String message) {
            first.println(message);
            second.println(message);
        }

        @Override
        public void printf(String format, Object... args) {
            first.printf(format, args);
            second.printf(format, args);
        }

        @Override
        public void printf(Locale locale, String format, Object... args) {
            first.printf(locale, format, args);
            second.printf(locale, format, args);
        }

        @Override
        public void close() {
            RuntimeException failure = null;

            try {
                first.close();
            } catch (RuntimeException ex) {
                failure = ex;
            }

            try {
                second.close();
            } catch (RuntimeException ex) {
                if (failure == null) {
                    failure = ex;
                } else {
                    failure.addSuppressed(ex);
                }
            }

            if (failure != null) {
                throw failure;
            }
        }
    }

    private enum NoOpReporter implements Reporter {
        INSTANCE;

        @Override
        public boolean isEnabled() {
            return false;
        }

        @Override
        public void println() {
        }

        @Override
        public void println(String message) {
        }

        @Override
        public void printf(String format, Object... args) {
        }

        @Override
        public void printf(Locale locale, String format, Object... args) {
        }
    }
}
