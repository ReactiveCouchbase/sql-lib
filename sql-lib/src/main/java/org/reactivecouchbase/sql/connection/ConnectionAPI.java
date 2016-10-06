package org.reactivecouchbase.sql.connection;

import org.reactivecouchbase.common.Throwables;
import org.reactivecouchbase.concurrent.Future;
import org.reactivecouchbase.concurrent.Promise;
import rx.Observable;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.function.Function;

public class ConnectionAPI {

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

    public static <T> Future<T> withAsyncConnection(Connection connection, Boolean transac, Function<Connection, Future<T>> block) {
        Promise<T> p = new Promise<>();
        try {
            block.apply(connection).onComplete(ttry -> {
                for (Throwable t : ttry.asFailure()) {
                    try {
                        if (transac) {
                            connection.rollback();
                        }
                    } catch (SQLException e1) {
                        p.tryFailure(e1);
                    }
                }
                for (T res : ttry.asSuccess()) {
                    p.trySuccess(res);
                    if (transac) {
                        try {
                            connection.commit();
                        } catch (Exception e) {
                            p.tryFailure(e);
                        }
                    }
                }
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException ignore) {
                        p.tryFailure(ignore);
                    }
                }
            });
        } catch (Exception eee) {
            try {
                if (transac) {
                    connection.rollback();
                }
            } catch (SQLException e1) {
                p.tryFailure(e1);
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ignore) {
                    p.tryFailure(ignore);
                }
            }
            p.tryFailure(eee);
        }
        return p.future();
    }

    public static <T> Observable<T> withRxConnection(Connection connection, Boolean transac, Function<Connection, Observable<T>> block) {
        return Observable.create(os -> {
            try {
                Observable<T> ret = block.apply(connection);
                ret.doOnError(e -> {
                    os.onError(e);
                    try {
                        if (transac) {
                            connection.rollback();
                        }
                    } catch (SQLException e1) {
                        os.onError(e1);
                    }
                });
                ret.doOnNext(n -> {
                    os.onNext(n);
                    try {
                        if (transac) {
                            connection.commit();
                        }
                    } catch (SQLException e1) {
                        os.onError(e1);
                    }
                });
                ret.doOnCompleted(() -> {
                    os.onCompleted();
                    if (connection != null) {
                        try {
                            connection.close();
                        } catch (SQLException ignore) {
                            os.onError(ignore);
                        }
                    }
                });
            } catch (Exception eee) {
                try {
                    if (transac) {
                        connection.rollback();
                    }
                } catch (SQLException e1) {
                    os.onError(e1);
                }
                os.onError(eee);
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException ignore) {
                        os.onError(ignore);
                    }
                }
            }
        });
    }
}
