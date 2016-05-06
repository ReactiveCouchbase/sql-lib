package org.reactivecouchbase.sql;

import org.reactivecouchbase.concurrent.Future;
import org.reactivecouchbase.functional.Option;
import org.reactivecouchbase.functional.Unit;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;


public class AsyncStream<T> {

    private final Stream<T> stream;

    AsyncStream(Stream<T> stream) {
        this.stream = stream;
    }

    public Stream<T> asStream() {
        return this.stream;
    }

    public <R> Stream<R> collect(Function<T, Option<R>> function) {
        return stream.collect(function);
    }

    public Stream<T> andThen(Consumer<T> function) {
        return stream.andThen(function);
    }

    public <R> Stream<R> map(Function<T, R> function) {
        return stream.map(function);
    }

    public Stream<T> filter(Predicate<T> predicate) {
        return stream.filter(predicate);
    }

    public <K> Future<Map<K, T>> asyncIndexBy(ExecutorService ec, final Function<T, K> grouper) {
        return Future.async(() -> stream.indexBy(grouper), ec);
    }

    public <K> Future<Map<K, List<T>>> asyncGroupBy(ExecutorService ec, final Function<T, K> grouper) {
        return Future.async(() -> stream.groupBy(grouper), ec);
    }

    public <K, V> Future<Map<K, V>> asyncIndexBy(ExecutorService ec, final Function<T, K> grouper, final Function<T, V> extractor) {
        return Future.async(() -> stream.indexBy(grouper, extractor), ec);
    }

    public <K, V> Future<Map<K, List<V>>> asyncGroupBy(ExecutorService ec, final Function<T, K> grouper, final Function<T, V> extractor) {
        return Future.async(() -> stream.groupBy(grouper, extractor), ec);
    }

    public <B> Future<B> asyncReduce(ExecutorService ec, final B from, final BiFunction<B, T, B> function) {
        return Future.async(() -> stream.reduce(from, function), ec);
    }

    public Future<List<T>> asyncRun(ExecutorService ec) {
        return Future.async(stream::run, ec);
    }

    public Future<Option<T>> asyncRunSingle(ExecutorService ec) {
        return Future.async(stream::runSingle, ec);
    }

    public Future<Unit> asyncExec(ExecutorService ec) {
        return Future.async(() -> {
            stream.exec();
            return Unit.unit();
        }, ec);
    }
}
