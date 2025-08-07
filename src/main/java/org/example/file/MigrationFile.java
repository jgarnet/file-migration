package org.example.file;

/*
file_id BIGINT PRIMARY KEY,
    old_uri VARCHAR(150) NOT NULL,
    new_uri VARCHAR(150),
    file_name VARCHAR(200),
    create_date DATE,
    status VARCHAR(8) NOT NULL,
    migration_date TIMESTAMP,
    retry_count INTEGER DEFAULT 0,
    last_attempt_date TIMESTAMP
 */

import java.time.LocalDateTime;

public class MigrationFile {
    private int id;
    private String oldUri;
    private String newUri;
    private String fileName;
    private LocalDateTime createDate;
    private MigrationStatus status;
    private int retryCount;
    private LocalDateTime lastAttemptDate;

    public MigrationFile(int id, String oldUri, String newUri, String fileName, LocalDateTime createDate, MigrationStatus status, int retryCount, LocalDateTime lastAttemptDate) {
        this.id = id;
        this.oldUri = oldUri;
        this.newUri = newUri;
        this.fileName = fileName;
        this.createDate = createDate;
        this.status = status;
        this.retryCount = retryCount;
        this.lastAttemptDate = lastAttemptDate;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getOldUri() {
        return oldUri;
    }

    public void setOldUri(String oldUri) {
        this.oldUri = oldUri;
    }

    public String getNewUri() {
        return newUri;
    }

    public void setNewUri(String newUri) {
        this.newUri = newUri;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public LocalDateTime getCreateDate() {
        return createDate;
    }

    public void setCreateDate(LocalDateTime createDate) {
        this.createDate = createDate;
    }

    public MigrationStatus getStatus() {
        return status;
    }

    public void setStatus(MigrationStatus status) {
        this.status = status;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public LocalDateTime getLastAttemptDate() {
        return lastAttemptDate;
    }

    public void setLastAttemptDate(LocalDateTime lastAttemptDate) {
        this.lastAttemptDate = lastAttemptDate;
    }
}
