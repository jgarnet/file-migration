package org.example.file;

import java.util.Map;

public enum JobStatus {
    PENDING("PENDING"), PROCESSING("PROCESSING"), COMPLETE("COMPLETE");
    private final String value;
    private static final Map<String, JobStatus> LOOKUP = Map.of(
            "PENDING", PENDING,
            "PROCESSING", PROCESSING,
            "COMPLETE", COMPLETE
    );

    JobStatus(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static JobStatus from(String value) {
        return LOOKUP.get(value);
    }
}
