package org.reactivecouchbase.sql;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.reactivecouchbase.common.Throwables;
import org.reactivecouchbase.functional.Option;
import org.reactivecouchbase.functional.Tuple;

import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public class Row {

    private final ResultSet set;
    private final Boolean safeMode;
    private final int index;

    Row(int index, ResultSet set, Boolean safeMode) {
        this.index = index;
        this.set = set;
        this.safeMode = safeMode;
    }

    public int index() {
        return index;
    }

    public final boolean isPresent(String name) {
        try {
            for (int i = 1; i < set.getMetaData().getColumnCount() + 1; i++) {
                if (name.equalsIgnoreCase(set.getMetaData().getColumnName(i))) {
                    return true;
                }
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }


    public final <T> T get(String key, Class<T> clazz) {
        try {
            return clazz.cast(set.getObject("key"));
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public final String str(String key) {
        try {
            return set.getString(key);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public final Integer intgr(String key) {
        try {
            int value = set.getInt(key);
            if (safeMode && set.wasNull()) {
                return null;
            }
            return value;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public final Long lng(String key) {
        try {
            Long value = set.getLong(key);
            if (safeMode && set.wasNull()) {
                return null;
            }
            return value;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public final Date date(String key) {
        try {
            return set.getDate(key);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public final Timestamp timestamp(String key) {
        try {
            return set.getTimestamp(key);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public final DateTime datetime(String key) {
        try {
            Timestamp timestamp = timestamp(key);
            if (timestamp == null) {
                return null;
            }
            return new DateTime(timestamp);
        } catch (Exception e) {
            try {
                return new DateTime(date(key));
            } catch (Exception e2) {
                throw Throwables.propagate(e2);
            }
        }
    }

    public final String formattedDate(String key, final String format) {
        try {
            return datetime(key).toString(format);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public final String formattedDate(String key, final DateTimeFormatter format) {
        try {
            return datetime(key).toString(format);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public final Time time(String key) {
        try {
            return set.getTime(key);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public final Boolean bool(String key) {
        try {
            Boolean value = set.getBoolean(key);
            if (safeMode && set.wasNull()) {
                return null;
            }
            return value;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public final Double dbl(String key) {
        try {
            Double value = set.getDouble(key);
            if (safeMode && set.wasNull()) {
                return null;
            }
            return value;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public final BigDecimal bigDec(String key) {
        try {
            return set.getBigDecimal(key);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public final Float flt(String key) {
        try {
            Float value = set.getFloat(key);
            if (safeMode && set.wasNull()) {
                return null;
            }
            return value;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public final <T> Option<List<T>> listOpt(String key, Class<T> of) {
        try {
            return Option.some(list(key, of));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public final <T> List<T> list(String key, Class<T> of) {
        try {
            Array arr = set.getArray(key);
            if (safeMode && set.wasNull()) {
                return Collections.emptyList();
            }
            Object actualArray = arr.getArray();
            if (actualArray == null) {
                return Collections.emptyList();
            }
            Class<?> actualClass = actualArray.getClass();
            if (of.equals(String.class) && String[].class.isAssignableFrom(actualClass)) {
                return (List<T>) Arrays.asList((String[]) actualArray);
            } else if (of.equals(Integer.class) && Integer[].class.isAssignableFrom(actualClass)) {
                return (List<T>) Arrays.asList((Integer[]) actualArray);
            } else if (of.equals(Double.class) && Double[].class.isAssignableFrom(actualClass)) {
                return (List<T>) Arrays.asList((Double[]) actualArray);
            } else if (of.equals(Long.class) && Long[].class.isAssignableFrom(actualClass)) {
                return (List<T>) Arrays.asList((Long[]) actualArray);
            } else if (of.equals(Float.class) && Float[].class.isAssignableFrom(actualClass)) {
                return (List<T>) Arrays.asList((Float[]) actualArray);
            } else if (of.equals(BigDecimal.class) && BigDecimal[].class.isAssignableFrom(actualClass)) {
                return (List<T>) Arrays.asList((BigDecimal[]) actualArray);
            } else if (of.equals(Boolean.class) && Boolean[].class.isAssignableFrom(actualClass)) {
                return (List<T>) Arrays.asList((Boolean[]) actualArray);
            } else if (of.equals(Date.class) && Date[].class.isAssignableFrom(actualClass)) {
                return (List<T>) Arrays.asList((Date[]) actualArray);
            } else if (of.equals(Time.class) && Time[].class.isAssignableFrom(actualClass)) {
                return (List<T>) Arrays.asList((Time[]) actualArray);
            } else if (of.equals(Timestamp.class) && Timestamp[].class.isAssignableFrom(actualClass)) {
                return (List<T>) Arrays.asList((Timestamp[]) actualArray);
            } else {
                throw new IllegalArgumentException("Cannot bind arrays of " + of.getName());
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public final Object obj(String key) {
        try {
            return set.getObject(key);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public final Blob blob(String key) {
        try {
            return set.getBlob(key);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public final Clob clob(String key) {
        try {
            return set.getClob(key);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public final <T> Option<T> getOpt(String key, Class<T> clazz) {
        try {
            return Option.apply(get(key, clazz));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public final Option<String> strOpt(String key) {
        try {
            return Option.apply(str(key));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public final Option<Integer> intOpt(String key) {
        try {
            return Option.apply(intgr(key));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public final Option<Long> lngOpt(String key) {
        try {
            return Option.apply(lng(key));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public final Option<Date> dateOpt(String key) {
        try {
            return Option.apply(date(key));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public final Option<Timestamp> timestampOpt(String key) {
        try {
            return Option.apply(set.getTimestamp(key));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public final Option<String> formattedDateOpt(String key, final String format) {
        return datetimeOpt(key).map(dateTime -> dateTime.toString(format));
    }

    public final Option<String> formattedDateOpt(String key, final DateTimeFormatter format) {
        return datetimeOpt(key).map(dateTime -> dateTime.toString(format));
    }

    public final Option<DateTime> datetimeOpt(String key) {
        try {
            Timestamp timestamp = timestamp(key);
            if (timestamp == null) {
                return Option.none();
            }
            return Option.apply(new DateTime(timestamp));
        } catch (Exception e) {
            try {
                Date date = date(key);
                if (date == null) {
                    return Option.none();
                }
                return Option.apply(new DateTime(date));
            } catch (Exception e2) {
                return Option.none();
            }
        }
    }

    public final Option<Time> timeOpt(String key) {
        try {
            return Option.apply(time(key));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public final Option<Boolean> boolOpt(String key) {
        try {
            return Option.apply(bool(key));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public final Option<Double> dblOpt(String key) {
        try {
            return Option.apply(dbl(key));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public final Option<BigDecimal> bigDecOpt(String key) {
        try {
            return Option.apply(bigDec(key));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public final Option<Float> fltOpt(String key) {
        try {
            return Option.apply(flt(key));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public final Option<Object> objOpt(String key) {
        try {
            return Option.apply(obj(key));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public final Option<Blob> blobOpt(String key) {
        try {
            return Option.apply(blob(key));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public final Option<Clob> clobOpt(String key) {
        try {
            return Option.apply(clob(key));
        } catch (Exception e) {
            return Option.none();
        }
    }


    public final Option<Map<String, Object>> asOptMap() {
        return Option.some(asMap());
    }

    public final Option<List<Tuple<String, Object>>> asOptList() {
        return Option.some(asList());
    }

    public final Map<String, Object> asMap() {
        Map<String, Object> row = new HashMap<>();
        try {
            int columns = set.getMetaData().getColumnCount();
            for (int i = 1; i < columns + 1; i++) {
                String name = set.getMetaData().getColumnName(i);
                row.put(name, set.getObject(i));
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        return row;
    }

    public final List<Tuple<String, Object>> asList() {
        List<Tuple<String, Object>> row = new ArrayList<>();
        try {
            int columns = set.getMetaData().getColumnCount();
            for (int i = 1; i < columns + 1; i++) {
                String name = set.getMetaData().getColumnName(i);
                row.add(Tuple.of(name, set.getObject(i)));
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        return row;
    }
}