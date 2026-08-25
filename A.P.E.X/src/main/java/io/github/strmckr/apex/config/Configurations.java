package io.github.strmckr.apex.config;

public final class Configurations {

    private Configurations() {
    }

    public static SortConfig defaultConfig() {
        return new SortConfig(13, 12, 128);
    }

}
