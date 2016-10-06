package org.reactivecouchbase.sql.representation;

import org.reactivecouchbase.concurrent.Future;
import org.reactivecouchbase.functional.Option;
import org.reactivecouchbase.functional.Unit;
import org.reactivecouchbase.json.JsArray;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.sql.Row;
import org.reactivecouchbase.sql.SQL;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;

public class AsyncSQL {

    private final SQL sql;
    private final ExecutorService ec;

    public AsyncSQL(SQL sql, ExecutorService ec) {
        this.sql = sql;
        this.ec = ec;
    }

    public Future<Boolean> execute() {
        return Future.async(sql::execute, ec);
    }

    public Future<Integer> executeUpdate() {
        return Future.async(sql::executeUpdate, ec);
    }

    public <T> Future<List<T>> collect(Function<Row, Option<T>> parser) {
        return Future.async(() -> sql.collect(parser), ec);
    }

    public <T> Future<Option<T>> collectSingle(Function<Row, Option<T>> parser) {
        return Future.async(() -> sql.collectSingle(parser), ec);
    }

    public Future<List<Row>> all() {
        return Future.async(sql::all, ec);
    }

    public Future<Option<Row>> single() {
        return Future.async(sql::single, ec);
    }

    public Future<JsArray> asJsArray() {
        return Future.async(sql::asJsArray, ec);
    }

    public Future<List<JsValue>> asJson() {
        return Future.async(sql::asJson, ec);
    }

    public <K, V> Future<Map<K, V>> indexBy(String colName, Class<K> clazz, Function<Row, V> parser) {
        return Future.async(() -> sql.indexBy(colName, clazz, parser), ec);
    }

    public <K, V> Future<Map<K, V>> indexBy(Function<Row, K> grouper, Function<Row, V> parser) {
        return Future.async(() -> sql.indexBy(grouper, parser), ec);
    }

    public <K, V> Future<Map<K, List<V>>> groupBy(String colName, Class<K> clazz, Function<Row, V> parser) {
        return Future.async(() -> sql.groupBy(colName, clazz, parser), ec);
    }

    public Future<Unit> foreach(Consumer<Row> action) {
        return Future.async(() -> {
            sql.foreach(action);
            return Unit.unit();
        }, ec);
    }

    public <K, V> Future<Map<K, List<V>>> groupBy(Function<Row, K> grouper, Function<Row, V> parser) {
        return Future.async(() -> sql.groupBy(grouper, parser), ec);
    }
}
