package org.reactivecouchbase.sql.connection;

import java.sql.Connection;

public interface ConnectionProvider {
    /**
     * Returns the opened connection to the database
     * @return
     */
    Connection get();
    /**
     * Open the connection to the database  if not already done
     */
    void beforeRequest();
    /**
     * Close the connection and suppress it in the thread
     */
    void afterRequest();
    /**
     * Initialize the connection (Load the JDBC driver)
     */
    void start();
    /**
     * Call the afterRequest method
     */
    void stop();
}