package io.github.strmckr.apex;

import java.util.Objects;

import io.github.strmckr.apex.config.Configurations;
import io.github.strmckr.apex.config.RunOptions;
import io.github.strmckr.apex.config.SortConfig;
import io.github.strmckr.apex.reporting.Reporters;

public final class Configuration {
    private final RunOptions.Options options;
    private final SortConfig config;

    private Configuration(RunOptions.Options options, SortConfig config) {
        this.options = Objects.requireNonNull(options, "options");
        this.config = Objects.requireNonNull(config, "config");
        RunOptions.validateOptions(options);
        RunOptions.validateConfig(config);
    }

    public static Configuration defaultConfiguration() {
        return new Configuration(new RunOptions.Options(), Configurations.defaultConfig());
    }

    public static Configuration of(SortConfig config) {
        return new Configuration(new RunOptions.Options(), config);
    }

    public static Configuration of(int msdBits, int lsdBits, int tinyPartitionThreshold) {
        return of(new SortConfig(msdBits, lsdBits, tinyPartitionThreshold));
    }

    public static Configuration fromCommands(String... commands) {
        RunOptions.Options options = RunOptions.parseOptions(commands == null ? new String[0] : commands);
        SortConfig config = options.lockedConfig != null ? options.lockedConfig : Configurations.defaultConfig();
        return new Configuration(options, config);
    }

    public RunOptions.Options options() {
        return options;
    }

    public SortConfig config() {
        return config;
    }

    void apply() {
        Reporters.configure(options.reportingMode, options.reportFile);
        RunOptions.applyApexSettings(options);
    }
}
