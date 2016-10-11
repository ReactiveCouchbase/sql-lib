package org.reactivecouchbase.sql;

import org.reactivecouchbase.common.Invariant;
import org.reactivecouchbase.common.Throwables;
import org.reactivecouchbase.functional.Tuple;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Batch {
    private final Map<String, Tuple<String, Object>> params;
    private int batchSize;
    private final AtomicReference<PreparedStatement> statement = new AtomicReference<>();
    private final Query preparedQuery;
    private final AtomicInteger counter = new AtomicInteger(0);
    private final List<Batch> triggerBefore = new ArrayList<>();
    private final List<Batch> triggerAfter = new ArrayList<>();

    public Integer enqueued() {
        return counter.get();
    }

    Batch(Connection connection, Query preparedQuery, List<Tuple<String, Object>> params, int batchSize) {
        this.preparedQuery = preparedQuery;
        this.params = new HashMap<>();
        this.batchSize = batchSize;
        for (Tuple<String, Object> p : params) {
            this.params.put(p._1, p);
        }
        if (statement.get() == null) {
            try {
                statement.set(connection.prepareStatement(preparedQuery.getPreparedSqlQuery()));
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private void cancelAutoBatch() {
        this.batchSize = -1;
    }

    public Batch triggerBeforeSelf(Batch... batches) {
        Invariant.checkNotNull(batches);
        for (Batch batch : batches) {
            if (batch != this) {
                batch.cancelAutoBatch();
                triggerBefore.add(batch);
            }
        }
        return this;
    }

    public Batch triggerAfterSelf(Batch... batches) {
        Invariant.checkNotNull(batches);
        for (Batch batch : batches) {
            if (batch != this) {
                batch.cancelAutoBatch();
                triggerAfter.add(batch);
            }
        }
        return this;
    }

    public final Batch on(Tuple<String, Object>... pairs) {
        params.clear();
        add(pairs);
        return this;
    }

    public final Batch on(String name, Object value) {
        this.params.put(name.trim(), Tuple.of(name.trim(), value));
        return this;
    }

    public final Batch on(List<Tuple<String, Object>> pairs) {
        params.clear();
        add(pairs);
        return this;
    }

    public final Batch add(Tuple<String, Object>... pairs) {
        add(Arrays.asList(pairs));
        return this;
    }

    public final Batch add(List<Tuple<String, Object>> pairs) {
        for (Tuple<String, Object> p : pairs) {
            this.params.put(p._1, p);
        }
        return this;
    }

    public final List<Integer> batch() {
        statement.set(API.fillStatement(statement.get(), preparedQuery.getParamNames(), params));
        try {
            statement.get().addBatch();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        List<Integer> results = new ArrayList<>();
        counter.incrementAndGet();
        if (batchSize > 0 && counter.get() >= batchSize) {
            results = executeBatch();
        }
        params.clear();
        return results;
    }

    public final void clearBatch() {
        try {
            counter.set(0);
            params.clear();
            statement.get().clearBatch();
            statement.get().clearParameters();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public final List<Integer> executeBatch() {
        for (Batch batch : triggerBefore) {
            batch.executeBatch();
        }
        try {
            int[] ret = statement.get().executeBatch();
            clearBatch();
            if (ret != null) {
                List<Integer> result = new ArrayList<>();
                for (Integer i : ret) {
                    result.add(i);
                }
                return result;
            } else {
                return Collections.emptyList();
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        } finally {
            for (Batch batch : triggerAfter) {
                batch.executeBatch();
            }
        }
    }
}