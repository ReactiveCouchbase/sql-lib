package org.reactivecouchbase.sql;

import org.reactivecouchbase.common.Invariant;
import org.reactivecouchbase.common.Throwables;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SQLBatch {
    private final Map<String, Pair> params;
    private int batchSize;
    private final AtomicReference<PreparedStatement> statement = new AtomicReference<PreparedStatement>();
    private final Query preparedQuery;
    private final AtomicInteger counter = new AtomicInteger(0);
    private final List<SQLBatch> triggerBefore = new ArrayList<SQLBatch>();
    private final List<SQLBatch> triggerAfter = new ArrayList<SQLBatch>();

    public Integer enqueued() {
        return counter.get();
    }

    SQLBatch(Connection connection, Query preparedQuery, List<Pair> params, int batchSize) {
        this.preparedQuery = preparedQuery;
        this.params = new HashMap<>();
        this.batchSize = batchSize;
        for (Pair p : params) {
            this.params.put(p.key, p);
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

    public SQLBatch triggerBeforeSelf(SQLBatch... batches) {
        Invariant.checkNotNull(batches);
        for (SQLBatch batch : batches) {
            if (batch != this) {
                batch.cancelAutoBatch();
                triggerBefore.add(batch);
            }
        }
        return this;
    }

    public SQLBatch triggerAfterSelf(SQLBatch... batches) {
        Invariant.checkNotNull(batches);
        for (SQLBatch batch : batches) {
            if (batch != this) {
                batch.cancelAutoBatch();
                triggerAfter.add(batch);
            }
        }
        return this;
    }

    public final SQLBatch on(Pair... pairs) {
        params.clear();
        add(pairs);
        return this;
    }

    public final SQLBatch on(String name, Object value) {
        this.params.put(name.trim(), new Pair(name.trim(), value));
        return this;
    }

    public final SQLBatch on(List<Pair> pairs) {
        params.clear();
        add(pairs);
        return this;
    }

    public final SQLBatch add(Pair... pairs) {
        add(Arrays.asList(pairs));
        return this;
    }

    public final SQLBatch add(List<Pair> pairs) {
        for (Pair p : pairs) {
            this.params.put(p.key, p);
        }
        return this;
    }

    public final List<Integer> batch() {
        statement.set(API.fillPreparedStatement(statement.get(), preparedQuery.getParamNames(), params));
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
        for (SQLBatch batch : triggerBefore) {
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
            for (SQLBatch batch : triggerAfter) {
                batch.executeBatch();
            }
        }
    }
}