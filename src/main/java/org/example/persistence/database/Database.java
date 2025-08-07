package org.example.persistence.database;

import javax.sql.DataSource;

public interface Database {
    DataSource getDataSource();
}
