package org.reactivecouchbase.sql.representation;

import org.reactivecouchbase.common.Holder;
import org.reactivecouchbase.functional.Option;
import org.reactivecouchbase.sql.Row;
import org.reactivecouchbase.sql.SQL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public class Stream<T> {

    private final SQL sql;
    private final Function<Row, Option<T>> pipeline;

    public Stream(SQL sql, Function<Row, Option<T>> pipeline) {
        this.sql = sql;
        this.pipeline = pipeline;
    }

    public AsyncStream<T> asAsyncStream() {
        return new AsyncStream<>(this);
    }

    public Stream<T> andThen(final Consumer<T> function) {
        return new Stream<>(sql, pipeline.andThen((Function<Option<T>, Option<T>>) input -> {
            for (T val : input) {
                function.accept(val);
                return input;
            }
            return Option.none();
        }));
    }

    public <R> Stream<R> map(final Function<T, R> function) {
        return new Stream<>(sql, pipeline.andThen((Function<Option<T>, Option<R>>) input -> {
            for (T val : input) {
                return Option.apply(function.apply(val));
            }
            return Option.none();
        }));
    }

    public Stream<T> filter(final Predicate<T> predicate) {
        return new Stream<>(sql, pipeline.andThen((Function<Option<T>, Option<T>>) input -> {
            for (T val : input) {
                if (predicate.test(val)) {
                    return input;
                }
            }
            return Option.none();
        }));
    }


    public <R> Stream<R> collect(final Function<T, Option<R>> function) {
        return new Stream<>(sql, pipeline.andThen((Function<Option<T>, Option<R>>) input -> {
            for (T val : input) {
                return function.apply(val);
            }
            return Option.none();
        }));
    }

    public <B> B reduce(final B from, final BiFunction<B, T, B> function) {
        final Holder<B> tmpFrom = Holder.of(from);
        sql.foreach(row -> {
            Option<T> opt = pipeline.apply(row);
            for (T val : opt) {
                tmpFrom.set(function.apply(tmpFrom.get(), val));
            }
        });
        return tmpFrom.get();
    }


    public <K> Map<K, T> indexBy(final Function<T, K> grouper) {
        return indexBy(grouper, Function.identity());
    }

    public <K> Map<K, List<T>> groupBy(final Function<T, K> grouper) {
        return groupBy(grouper, Function.<T>identity());
    }


    public <K, V> Map<K, V> indexBy(final Function<T, K> grouper, final Function<T, V> extractor) {
        final Map<K, V> map = new HashMap<>();
        sql.collect((Function<Row, Option<T>>) row -> {
            Option<T> opt = pipeline.apply(row);
            if (opt != null && opt.isDefined()) {
                T initValue = opt.get();
                K key = grouper.apply(initValue);
                V value = extractor.apply(initValue);
                map.put(key, value);
            }
            return Option.none();
        });
        return map;
    }

    public <K, V> Map<K, List<V>> groupBy(final Function<T, K> grouper, final Function<T, V> extractor) {
        final Map<K, List<V>> map = new HashMap<>();
        sql.foreach(row -> {
            Option<T> opt = pipeline.apply(row);
            if (opt != null && opt.isDefined()) {
                T initValue = opt.get();
                K key = grouper.apply(initValue);
                V value = extractor.apply(initValue);
                if (!map.containsKey(key)) {
                    map.put(key, new ArrayList<V>());
                }
                map.get(key).add(value);
            }
        });
        return map;
    }

    public List<T> run() {
        return sql.collect(pipeline::apply);
    }

    public Option<T> runSingle() {
        return sql.collectSingle(pipeline::apply);
    }

    public void exec() {
        sql.foreach(pipeline::apply);
    }
}
