package org.example.logger;

public interface Logger {
    void info(String message, Object...args);
    void error(String error, Object...args);
    void error(Exception e);
}
