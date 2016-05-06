package org.reactivecouchbase.sql.connection;

import org.reactivecouchbase.common.Throwables;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Manage a connection to a database for a thread
 */
class SimpleProvider implements ConnectionProvider {

    private final String url;
    private final String login;
    private final String password;
    private final Driver driver;
    private static ThreadLocal<Connection> connection = new ThreadLocal<Connection>();

    SimpleProvider(Driver driver, String url, String login, String password) {
        this.url = url;
        this.login = login;
        this.password = password;
        this.driver = driver;
    }

    @Override
    public final Connection get() {
        return connection.get();
    }

    @Override
    public final void beforeRequest() {
        if (connection.get() == null) {
            try {
                connection.set(DriverManager.getConnection(url, login, password));
            } catch (SQLException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    @Override
    public final void afterRequest() {
        if (connection.get() != null) {
            try {
                connection.get().close();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
        connection.remove();
    }

    @Override
    public final void start() {
        try {
            DriverManager.registerDriver(driver);
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public final void stop() {
        afterRequest();
    }
}