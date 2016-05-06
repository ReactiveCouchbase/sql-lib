package org.reactivecouchbase.sql.connection;

import org.reactivecouchbase.common.Throwables;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.function.Function;

public class API {

    public static Database database(ConnectionProvider provider) {
        return new Database(provider);
    }

    public static ConnectionProvider provider(Driver driver, String url, String login, String password) {
        return new SimpleProvider(driver, url, login, password);
    }

    public static <T> T withConnection(Connection connection, Boolean transac, Function<Connection, T> block) {
        try {
            T ret = block.apply(connection);
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
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ignore) {
                    throw Throwables.propagate(ignore);
                }
            }
        }
    }
}
