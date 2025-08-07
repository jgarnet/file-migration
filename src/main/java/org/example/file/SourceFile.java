package org.example.file;

import java.time.LocalDateTime;

public class SourceFile {
    private int id;
    private String name;
    private String uri;
    private LocalDateTime createDate;

    public SourceFile(int id, String name, String uri, LocalDateTime createDate) {
        this.id = id;
        this.name = name;
        this.uri = uri;
        this.createDate = createDate;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public LocalDateTime getCreateDate() {
        return createDate;
    }

    public void setCreateDate(LocalDateTime createDate) {
        this.createDate = createDate;
    }
}