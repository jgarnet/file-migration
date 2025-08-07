package org.example.file;

import java.util.Map;

public enum MigrationStatus {
    SUCCESS("SUCCESS"), FAIL("FAIL"), RETRYING("RETRYING");
    private final String value;
    private static final Map<String, MigrationStatus> LOOKUP = Map.of(
            "SUCCESS", SUCCESS,
            "FAIL", FAIL,
            "RETRYING", RETRYING
    );

    MigrationStatus(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static MigrationStatus from(String value) {
        return LOOKUP.get(value);
    }
}
