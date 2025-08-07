package org.example.file;

import java.time.LocalDateTime;

public class MigrationRange {
    private int rangeId;
    private int minId;
    private int maxId;
    private JobStatus status;
    private LocalDateTime lastUpdated;

    public MigrationRange(int rangeId, int minId, int maxId, JobStatus status, LocalDateTime lastUpdated) {
        this.rangeId = rangeId;
        this.minId = minId;
        this.maxId = maxId;
        this.status = status;
        this.lastUpdated = lastUpdated;
    }

    public int getRangeId() {
        return rangeId;
    }

    public void setRangeId(int rangeId) {
        this.rangeId = rangeId;
    }

    public int getMinId() {
        return minId;
    }

    public void setMinId(int minId) {
        this.minId = minId;
    }

    public int getMaxId() {
        return maxId;
    }

    public void setMaxId(int maxId) {
        this.maxId = maxId;
    }

    public JobStatus getStatus() {
        return status;
    }

    public void setStatus(JobStatus status) {
        this.status = status;
    }

    public LocalDateTime getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(LocalDateTime lastUpdated) {
        this.lastUpdated = lastUpdated;
    }
}
