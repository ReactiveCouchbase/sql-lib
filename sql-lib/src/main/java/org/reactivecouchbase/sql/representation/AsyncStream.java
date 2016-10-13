package org.reactivecouchbase.sql.representation;

import org.reactivecouchbase.concurrent.Future;
import org.reactivecouchbase.functional.Option;
import org.reactivecouchbase.functional.Unit;
import rx.Observable;
import rx.Single;

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

    public <R> AsyncStream<R> collect(Function<T, Option<R>> function) {
        return new AsyncStream<R>(stream.collect(function));
    }

    public AsyncStream<T> andThen(Consumer<T> function) {
        return new AsyncStream<T>(stream.andThen(function));
    }

    public <R> AsyncStream<R> map(Function<T, R> function) {
        return new AsyncStream<R>(stream.map(function));
    }

    public AsyncStream<T> filter(Predicate<T> predicate) {
        return new AsyncStream<T>(stream.filter(predicate));
    }

    public <K> Future<Map<K, T>> indexBy(ExecutorService ec, final Function<T, K> grouper) {
        return Future.async(() -> stream.indexBy(grouper), ec);
    }

    public <K> Future<Map<K, List<T>>> groupBy(ExecutorService ec, final Function<T, K> grouper) {
        return Future.async(() -> stream.groupBy(grouper), ec);
    }

    public <K, V> Future<Map<K, V>> indexBy(ExecutorService ec, final Function<T, K> grouper, final Function<T, V> extractor) {
        return Future.async(() -> stream.indexBy(grouper, extractor), ec);
    }

    public <K, V> Future<Map<K, List<V>>> groupBy(ExecutorService ec, final Function<T, K> grouper, final Function<T, V> extractor) {
        return Future.async(() -> stream.groupBy(grouper, extractor), ec);
    }

    public <B> Future<B> reduce(ExecutorService ec, final B from, final BiFunction<B, T, B> function) {
        return Future.async(() -> stream.reduce(from, function), ec);
    }

    public Future<List<T>> run(ExecutorService ec) {
        return Future.async(stream::run, ec);
    }

    public Future<Option<T>> runSingle(ExecutorService ec) {
        return Future.async(stream::runSingle, ec);
    }

    public Future<Unit> exec(ExecutorService ec) {
        return Future.async(() -> {
            stream.exec();
            return Unit.unit();
        }, ec);
    }
}
