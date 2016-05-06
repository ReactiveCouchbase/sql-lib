package org.reactivecouchbase.sql.connection;

import org.reactivecouchbase.common.Throwables;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Consumer;
import java.util.function.Function;

public class Database {

    private final ConnectionProvider provider;

    Database(ConnectionProvider provider) {
        this.provider = provider;
        startup();
    }

    private void startup() {
        provider.start();
    }

    public final void close() {
        provider.stop();
    }

    public final void withConnection(Boolean transac, Consumer<Connection> action) {
        provider.beforeRequest();
        try {
            Connection connection = provider.get();
            try {
                action.accept(connection);
                if (transac) {
                    connection.commit();
                }
            } catch (Exception e) {
                try {
                    if (transac) {
                        connection.rollback();
                    }
                } catch (SQLException e1) {
                    throw Throwables.propagate(e1);
                }
                throw Throwables.propagate(e);
            }
        } finally  {
            provider.afterRequest();
        }
    }

    public final <T> T withConnection(Boolean transac, Function<Connection, T> action) {
        provider.beforeRequest();
        try {
            Connection connection = provider.get();
            try {
                T ret = action.apply(connection);
                if (transac) {
                    connection.commit();
                }
                return ret;
            } catch (Exception e) {
                try {
                    if (transac) {
                        connection.rollback();
                    }
                } catch (SQLException e1) {
                    throw Throwables.propagate(e1);
                }
                throw Throwables.propagate(e);
            }
        } finally  {
            provider.afterRequest();
        }
    }
}