package org.reactivecouchbase.sql;

import org.joda.time.DateTime;
import org.reactivecouchbase.common.Invariant;
import org.reactivecouchbase.common.Throwables;
import org.reactivecouchbase.functional.Option;
import org.reactivecouchbase.functional.Tuple;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class API {

    private API() {
    }

    static boolean defaultSafeModeValue = false;
    static Option<Integer> defaultPageOfValue = Option.none();

    public static void globalSafeMode(boolean defaultSafeModeValue) {
        API.defaultSafeModeValue = defaultSafeModeValue;
    }

    public static void globalPageOf(Option<Integer> defaultPageOfValue) {
        API.defaultPageOfValue = defaultPageOfValue;
    }

    public static SQL sql(Connection connection, String sql) {
        return new SQL(connection, Query.preparedQuery(sql), new ArrayList<>());
    }

    public static SQL sql(Connection connection, Query preparedQuery) {
        return new SQL(connection, preparedQuery, new ArrayList<>());
    }

    public static Batch batch(Connection connection, String sql) {
        return new Batch(connection, Query.preparedQuery(sql), new ArrayList<>(), -1);
    }

    public static Batch batch(Connection connection, Query preparedQuery) {
        return new Batch(connection, preparedQuery, new ArrayList<>(), -1);
    }

    public static Batch batch(Connection connection, int batchSize, String sql) {
        return new Batch(connection, Query.preparedQuery(sql), new ArrayList<>(), batchSize);
    }

    public static Batch batch(Connection connection, int batchSize, Query preparedQuery) {
        return new Batch(connection, preparedQuery, new ArrayList<>(), batchSize);
    }

    static PreparedStatement fillPreparedStatement(PreparedStatement pst, List<String> names, Map<String, Tuple<String, Object>> params) {
        int i = 1;
        for (String name : names) {
            pst = fillOneParam(pst, i, name, params);
            i++;
        }
        return pst;
    }

    private static PreparedStatement fillOneParam(PreparedStatement pst, int index, String name, Map<String, Tuple<String, Object>> params) {
        if (params.containsKey(name)) {
            Object value = params.get(name)._2;
            try {
                if (value instanceof String) {
                    pst.setString(index, (String) value);
                } else if (value instanceof Time) {
                    pst.setTime(index, (Time) value);
                } else if (value instanceof Timestamp) {
                    pst.setTimestamp(index, (Timestamp) value);
                } else if (value instanceof java.util.Date) {
                    pst.setDate(index, new java.sql.Date(((java.util.Date) value).getTime()));
                } else if (value instanceof DateTime) {
                    pst.setDate(index, new java.sql.Date(((DateTime) value).toDate().getTime()));
                } else {
                    pst.setObject(index, value);
                }
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }

        return pst;
    }

    public static Function<Row, Option<Long>> longParser(final String name) {
        return row -> Option.apply(row.lng(name));
    }

    public static Function<Row, Option<String>> stringParser(final String name) {
        return row -> Option.apply(row.str(name));
    }

    public static Function<Row, Option<Integer>> integerParser(final String name) {
        return row -> Option.apply(row.intgr(name));
    }

    public static Function<Row, Option<Date>> dateParser(final String name) {
        return row -> Option.apply(row.date(name));
    }

    public static Function<Row, Option<Boolean>> booleanParser(final String name) {
        return row -> Option.apply(row.bool(name));
    }

    public static Function<Row, Option<Double>> doubleParser(final String name) {
        return row -> Option.apply(row.dbl(name));
    }

    public static Function<Row, Option<BigDecimal>> bigdecimalParser(final String name) {
        return row -> Option.apply(row.bigDec(name));
    }

    public static Function<Row, Option<Float>> floatParser(final String name) {
        return row -> Option.apply(row.flt(name));
    }

    public static Function<Row, Option<Blob>> blobParser(final String name) {
        return row -> Option.apply(row.blob(name));
    }

    public static Function<Row, Option<Clob>> clobParser(final String name) {
        return row -> Option.apply(row.clob(name));
    }

    public static Function<Row, Option<Object>> objectParser(final String name) {
        return row -> Option.apply(row.obj(name));
    }

    public static <T> Function<Row, Option<T>> unsafeParser(final Function<Row, T> parser) {
        Invariant.checkNotNull(parser);
        return row -> Option.apply(parser.apply(row));
    }

    public static <T>  Function<Row, Option<T>> safeParser(final Function<Row, T> parser) {
        Invariant.checkNotNull(parser);
        return row -> {
            try {
                return Option.apply(parser.apply(row));
            } catch (Exception e) {
                return Option.none();
            }
        };
    }
}