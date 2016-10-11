package org.reactivecouchbase.sql;

import org.reactivecouchbase.common.Holder;
import org.reactivecouchbase.common.Throwables;
import org.reactivecouchbase.concurrent.Future;
import org.reactivecouchbase.functional.Option;
import org.reactivecouchbase.functional.Tuple;
import org.reactivecouchbase.json.JsArray;
import org.reactivecouchbase.json.JsObject;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;
import rx.Observable;
import rx.Single;
import rx.Subscriber;
import rx.functions.Func1;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Call {

    private final Query preparedQuery;
    private final Connection connection;
    private final Map<String, Tuple<String, Object>> params;
    private final List<Tuple<String, SQLType>> outs;
    private boolean safeMode = API.defaultSafeModeValue;
    private Option<Integer> page = API.defaultPageOfValue;

    public Call unsafe(Boolean u) {
        this.safeMode = u;
        return this;
    }

    Call(Connection connection, Query preparedQuery, List<Tuple<String, Object>> params) {
        this.preparedQuery = preparedQuery;
        this.connection = connection;
        this.params = new HashMap<>();
        this.outs = new ArrayList<>();
        for (Tuple<String, Object> p : params) {
            this.params.put(p._1.trim(), p);
        }
    }

    public final Call withPageOf(int of) {
        page = Option.some(of);
        return this;
    }

    public final Call withNoPage() {
        page = Option.none();
        return this;
    }

    public final Call on(String name, Object value) {
        this.params.put(name.trim(), Tuple.of(name.trim(), value));
        return this;
    }

    public final Call out(String name, SQLType theType) {
        this.outs.add(new Tuple<>(name, theType));
        return this;
    }

    private final <T> List<T> executeQueryWithLimit(Function<CallRow, Option<T>> parser, Long limit) {
        ResultSet resultSet = null;
        CallableStatement pst = null;
        try {
            pst = connection.prepareCall(preparedQuery.getPreparedSqlQuery());
            if (pst != null && page.isDefined()) {
                pst.setFetchSize(page.get());
            }
            pst = API.fillStatement(pst, preparedQuery.getParamNames(), params);
            for (Tuple<String, SQLType> tuple : outs) {
                pst.registerOutParameter(tuple._1, tuple._2);
            }
            resultSet = pst.executeQuery();
            List<T> results = new ArrayList<T>();
            while (resultSet.next()) {
                int index = resultSet.getRow();
                Option<T> opt = parser.apply(new CallRow(new Row(index, resultSet, safeMode), pst, safeMode));
                if (opt.isDefined()) {
                    results.add(opt.get());
                    if (limit != null && results.size() >= limit) {
                        return results;
                    }
                }
            }
            return results;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }
            if (pst != null) {
                try {
                    pst.close();
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }
        }
    }

    public final boolean execute() {
        try {
            CallableStatement pst = connection.prepareCall(preparedQuery.getPreparedSqlQuery());
            pst = API.fillStatement(pst, preparedQuery.getParamNames(), params);
            for (Tuple<String, SQLType> tuple : outs) {
                pst.registerOutParameter(tuple._1, tuple._2);
            }
            return pst.execute();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public final int executeUpdate() {
        try {
            CallableStatement pst = connection.prepareCall(preparedQuery.getPreparedSqlQuery());
            pst = API.fillStatement(pst, preparedQuery.getParamNames(), params);
            for (Tuple<String, SQLType> tuple : outs) {
                pst.registerOutParameter(tuple._1, tuple._2);
            }
            return pst.executeUpdate();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public final <T> List<T> collect(Function<CallRow, Option<T>> parser) {
        return executeQueryWithLimit(parser, null);
    }

    public final <T> Option<T> collectSingle(Function<CallRow, Option<T>> parser) {
        List<T> result = executeQueryWithLimit(parser, 1L);
        if (!result.isEmpty()) {
            return Option.some(result.get(0));
        } else {
            return Option.none();
        }
    }

    public final List<CallRow> all() {
        return collect(Option::apply);
    }

    public final Option<CallRow> single() {
        List<CallRow> result = executeQueryWithLimit(Option::apply, 1L);
        if (!result.isEmpty()) {
            return Option.some(result.get(0));
        } else {
            return Option.none();
        }
    }

    public Single<CallRow> asAsyncSingle(ExecutorService ec) {
        Call sql = this;
        return Single.create(subscriber -> {
            Future.async(() -> {
                try {
                    Option<CallRow> row = sql.single();
                    if (row.isDefined()) {
                        subscriber.onSuccess(row.get());
                    } else {
                        subscriber.onError(new RuntimeException("No value returned"));
                    }
                } catch (Throwable e) {
                    subscriber.onError(e);
                }
            }, ec);
        });
    }

    public Single<CallRow> asBlockingSingle() {
        Call sql = this;
        return Single.create(subscriber -> {
            try {
                Option<CallRow> row = sql.single();
                if (row.isDefined()) {
                    subscriber.onSuccess(row.get());
                } else {
                    subscriber.onError(new RuntimeException("No value returned"));
                }
            } catch (Throwable e) {
                subscriber.onError(e);
            }
        });
    }

    public Observable<CallRow> asAsyncObservable(int pageOf, ExecutorService ec) {
        return this.withPageOf(pageOf).asAsyncObservable(ec);
    }

    public Observable<CallRow> asBlockingObservable(int pageOf) {
        return this.withPageOf(pageOf).asBlockingObservable();
    }

    public Observable<CallRow> asAsyncObservable(ExecutorService ec) {
        Call sql = this;
        return Observable.create(new Observable.OnSubscribe<CallRow>() {
            @Override
            public void call(Subscriber<? super CallRow> subscriber) {
                Future.async(() -> {
                    subscriber.onStart();
                    try {
                        sql.foreach(subscriber::onNext);
                        subscriber.onCompleted();
                    } catch (Throwable e) {
                        e.printStackTrace();
                        subscriber.onError(e);
                    }
                }, ec);
            }
        });
    }

    public Observable<CallRow> asBlockingObservable() {
        Call sql = this;
        return Observable.create(new Observable.OnSubscribe<CallRow>() {
            @Override
            public void call(Subscriber<? super CallRow> subscriber) {
                subscriber.onStart();
                try {
                    sql.foreach(subscriber::onNext);
                    subscriber.onCompleted();
                } catch (Throwable e) {
                    e.printStackTrace();
                    subscriber.onError(e);
                }
            }
        });
    }

    public final JsArray asJsArray() {
        return Json.arr(asJson());
    }

    public final List<JsValue> asJson() {
        List<Map<String, Object>> result = collect(row -> row.asOptMap());
        return result.stream().map(m -> {
            Map<String, JsValue> mResult = new HashMap<>();
            for (Map.Entry<String, Object> entry : m.entrySet()) {
                mResult.put(entry.getKey(), Json.wrap(entry.getValue()));
            }
            return new JsObject(mResult);
        }).collect(Collectors.toList());
    }

    public <K, V> Map<K, V> indexBy(final String colName, final Class<K> clazz, final Function<CallRow, V> parser) {
        return indexBy(input -> input.get(colName, clazz), parser);
    }

    public <K, V> Map<K, V> indexBy(final Function<CallRow, K> grouper, final Function<CallRow, V> parser) {
        final Map<K, V> map = new HashMap<>();
        foreach(row -> {
            K key = grouper.apply(row);
            V value = parser.apply(row);
            if (key != null && value != null) {
                map.put(key, value);
            }
        });
        return map;
    }

    public <K, V> Map<K, List<V>> groupBy(final String colName, final Class<K> clazz, final Function<CallRow, V> parser) {
        return groupBy(input -> input.get(colName, clazz), parser);
    }

    public <K, V> Map<K, List<V>> groupBy(final Function<CallRow, K> grouper, final Function<CallRow, V> parser) {
        final Map<K, List<V>> map = new HashMap<K, List<V>>();
        foreach(row -> {
            K key = grouper.apply(row);
            V value = parser.apply(row);
            if (key != null && value != null) {
                if (!map.containsKey(key)) {
                    map.put(key, new ArrayList<V>());
                }
                map.get(key).add(value);
            }
        });
        return map;
    }


    public final void foreach(final Consumer<CallRow> action) {
        collect(row -> {
            action.accept(row);
            return Option.none();
        });
    }

    public <R> Observable<R> map(final Func1<CallRow, R> function) {
        return this.asBlockingObservable().map(function);
    }

    public Observable<CallRow> filter(final Func1<CallRow, Boolean> predicate) {
        return this.asBlockingObservable().filter(predicate);
    }

    public <B> B reduce(final B from, final BiFunction<B, CallRow, B> function) {
        final Holder<B> tmpFrom = Holder.of(from);
        this.foreach(row -> tmpFrom.set(function.apply(tmpFrom.get(), row)));
        return tmpFrom.get();
    }
}