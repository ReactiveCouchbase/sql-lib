package org.reactivecouchbase.sql.connection;

import org.reactivecouchbase.common.Throwables;
import org.reactivecouchbase.concurrent.Future;
import org.reactivecouchbase.concurrent.Promise;
import rx.Observable;

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

    public final <T> Future<T> withAsyncConnection(Boolean transac, Function<Connection, Future<T>> block) {
        Promise<T> p = new Promise<>();
        provider.beforeRequest();
        Connection connection = provider.get();
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
                provider.afterRequest();
            });
        } catch (Exception eee) {
            try {
                if (transac) {
                    connection.rollback();
                }
            } catch (SQLException e1) {
                p.tryFailure(e1);
            }
            p.tryFailure(eee);
            provider.afterRequest();
        }
        return p.future();
    }

    public final <T> Observable<T> withRxConnection(Boolean transac, Function<Connection, Observable<T>> block) {
        return Observable.create(os -> {
            provider.beforeRequest();
            Connection connection = provider.get();
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
                    provider.afterRequest();
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
                provider.afterRequest();
            }
        });
    }
}