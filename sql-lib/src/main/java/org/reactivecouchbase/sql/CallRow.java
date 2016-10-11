package org.reactivecouchbase.sql;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.reactivecouchbase.common.Throwables;
import org.reactivecouchbase.functional.Option;
import org.reactivecouchbase.functional.Tuple;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

public class CallRow {

    private final Row row;
    private final CallableStatement cst;
    private final boolean safeMode;

    CallRow(Row row, CallableStatement cst, boolean safeMode) {
        this.row = row;
        this.cst = cst;
        this.safeMode = safeMode;
    }

    public int index() {
        return row.index();
    }

    public Double dbl(String key) {
        return row.dbl(key);
    }

    public Option<Map<String, Object>> asOptMap() {
        return row.asOptMap();
    }

    public Timestamp timestamp(String key) {
        return row.timestamp(key);
    }

    public Boolean bool(String key) {
        return row.bool(key);
    }

    public List<Tuple<String, Object>> asList() {
        return row.asList();
    }

    public <T> T get(String key, Class<T> clazz) {
        return row.get(key, clazz);
    }

    public boolean isPresent(String name) {
        return row.isPresent(name);
    }

    public Option<Integer> intOpt(String key) {
        return row.intOpt(key);
    }

    public Option<BigDecimal> bigDecOpt(String key) {
        return row.bigDecOpt(key);
    }

    public Option<Timestamp> timestampOpt(String key) {
        return row.timestampOpt(key);
    }

    public Option<String> strOpt(String key) {
        return row.strOpt(key);
    }

    public Float flt(String key) {
        return row.flt(key);
    }

    public Integer intgr(String key) {
        return row.intgr(key);
    }

    public Option<String> formattedDateOpt(String key, DateTimeFormatter format) {
        return row.formattedDateOpt(key, format);
    }

    public Option<List<Tuple<String, Object>>> asOptList() {
        return row.asOptList();
    }

    public String formattedDate(String key, String format) {
        return row.formattedDate(key, format);
    }

    public Time time(String key) {
        return row.time(key);
    }

    public Object obj(String key) {
        return row.obj(key);
    }

    public String formattedDate(String key, DateTimeFormatter format) {
        return row.formattedDate(key, format);
    }

    public BigDecimal bigDec(String key) {
        return row.bigDec(key);
    }

    public Option<Long> lngOpt(String key) {
        return row.lngOpt(key);
    }

    public <T> Option<List<T>> listOpt(String key, Class<T> of) {
        return row.listOpt(key, of);
    }

    public Map<String, Object> asMap() {
        return row.asMap();
    }

    public <T> List<T> list(String key, Class<T> of) {
        return row.list(key, of);
    }

    public Option<String> formattedDateOpt(String key, String format) {
        return row.formattedDateOpt(key, format);
    }

    public Clob clob(String key) {
        return row.clob(key);
    }

    public Option<Double> dblOpt(String key) {
        return row.dblOpt(key);
    }

    public Option<Float> fltOpt(String key) {
        return row.fltOpt(key);
    }

    public String str(String key) {
        return row.str(key);
    }

    public Blob blob(String key) {
        return row.blob(key);
    }

    public Option<DateTime> datetimeOpt(String key) {
        return row.datetimeOpt(key);
    }

    public Option<Time> timeOpt(String key) {
        return row.timeOpt(key);
    }

    public Option<Date> dateOpt(String key) {
        return row.dateOpt(key);
    }

    public Long lng(String key) {
        return row.lng(key);
    }

    public Option<Blob> blobOpt(String key) {
        return row.blobOpt(key);
    }

    public Date date(String key) {
        return row.date(key);
    }

    public Option<Boolean> boolOpt(String key) {
        return row.boolOpt(key);
    }

    public Option<Object> objOpt(String key) {
        return row.objOpt(key);
    }

    public DateTime datetime(String key) {
        return row.datetime(key);
    }

    public <T> Option<T> getOpt(String key, Class<T> clazz) {
        return row.getOpt(key, clazz);
    }

    public Option<Clob> clobOpt(String key) {

        return row.clobOpt(key);
    }


    // out params


    public String outputString(String parameterName) {
        try {
            String ret = cst.getString(parameterName);
            if (safeMode && cst.wasNull()) {
                return null;
            }
            return ret;
        } catch (Throwable t) {
            throw Throwables.propagate(t);
        }
    }

    public Boolean outputBoolean(String parameterName) {
        try {
            boolean ret = cst.getBoolean(parameterName);
            if (safeMode && cst.wasNull()) {
                return null;
            }
            return ret;
        } catch (Throwable t) {
            throw Throwables.propagate(t);
        }
    }

    public Byte outputByte(String parameterName) {
        try {
            byte ret = cst.getByte(parameterName);
            if (safeMode && cst.wasNull()) {
                return null;
            }
            return ret;
        } catch (Throwable t) {
            throw Throwables.propagate(t);
        }
    }

    public Short outputShort(String parameterName) {
        try {
            short ret = cst.getShort(parameterName);
            if (safeMode && cst.wasNull()) {
                return null;
            }
            return ret;
        } catch (Throwable t) {
            throw Throwables.propagate(t);
        }
    }

    public Integer outputInt(String parameterName) {
        try {
            int ret = cst.getInt(parameterName);
            if (safeMode && cst.wasNull()) {
                return null;
            }
            return ret;
        } catch (Throwable t) {
            throw Throwables.propagate(t);
        }
    }

    public Long outputLong(String parameterName) {
        try {
            long ret = cst.getLong(parameterName);
            if (safeMode && cst.wasNull()) {
                return null;
            }
            return ret;
        } catch (Throwable t) {
            throw Throwables.propagate(t);
        }
    }

    public Float outputFloat(String parameterName) {
        try {
            float ret = cst.getFloat(parameterName);
            if (safeMode && cst.wasNull()) {
                return null;
            }
            return ret;
        } catch (Throwable t) {
            throw Throwables.propagate(t);
        }
    }

    public Double outputDouble(String parameterName) {
        try {
            double ret = cst.getDouble(parameterName);
            if (safeMode && cst.wasNull()) {
                return null;
            }
            return ret;
        } catch (Throwable t) {
            throw Throwables.propagate(t);
        }
    }

    public Date outputDate(String parameterName) {
        try {
            Date ret = cst.getDate(parameterName);
            if (safeMode && cst.wasNull()) {
                return null;
            }
            return ret;
        } catch (Throwable t) {
            throw Throwables.propagate(t);
        }
    }

    public Time outputTime(String parameterName) {
        try {
            Time ret = cst.getTime(parameterName);
            if (safeMode && cst.wasNull()) {
                return null;
            }
            return ret;
        } catch (Throwable t) {
            throw Throwables.propagate(t);
        }
    }

    public Timestamp outputTimestamp(String parameterName) {
        try {
            Timestamp ret = cst.getTimestamp(parameterName);
            if (safeMode && cst.wasNull()) {
                return null;
            }
            return ret;
        } catch (Throwable t) {
            throw Throwables.propagate(t);
        }
    }

    public Object outputObject(String parameterName) {
        try {
            Object ret = cst.getObject(parameterName);
            if (safeMode && cst.wasNull()) {
                return null;
            }
            return ret;
        } catch (Throwable t) {
            throw Throwables.propagate(t);
        }
    }

    public BigDecimal outputBigDecimal(String parameterName) {
        try {
            BigDecimal ret = cst.getBigDecimal(parameterName);
            if (safeMode && cst.wasNull()) {
                return null;
            }
            return ret;
        } catch (Throwable t) {
            throw Throwables.propagate(t);
        }
    }


    public Blob outputBlob(String parameterName) {
        try {
            Blob ret = cst.getBlob(parameterName);
            if (safeMode && cst.wasNull()) {
                return null;
            }
            return ret;
        } catch (Throwable t) {
            throw Throwables.propagate(t);
        }
    }

    public Clob outputClob(String parameterName) {
        try {
            Clob ret = cst.getClob(parameterName);
            if (safeMode && cst.wasNull()) {
                return null;
            }
            return ret;
        } catch (Throwable t) {
            throw Throwables.propagate(t);
        }
    }


    public Date outputDate(String parameterName, Calendar cal) {
        try {
            Date ret = cst.getDate(parameterName, cal);
            if (safeMode && cst.wasNull()) {
                return null;
            }
            return ret;
        } catch (Throwable t) {
            throw Throwables.propagate(t);
        }
    }

    public Time outputTime(String parameterName, Calendar cal) {
        try {
            Time ret = cst.getTime(parameterName, cal);
            if (safeMode && cst.wasNull()) {
                return null;
            }
            return ret;
        } catch (Throwable t) {
            throw Throwables.propagate(t);
        }
    }

    public Timestamp outputTimestamp(String parameterName, Calendar cal) {
        try {
            Timestamp ret = cst.getTimestamp(parameterName, cal);
            if (safeMode && cst.wasNull()) {
                return null;
            }
            return ret;
        } catch (Throwable t) {
            throw Throwables.propagate(t);
        }
    }


    public Option<String> outputStringOpt(String parameterName) throws SQLException {
        try {
            return Option.apply(outputString(parameterName));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public Option<Boolean> outputBooleanOpt(String parameterName) throws SQLException {
        try {
            return Option.apply(outputBoolean(parameterName));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public Option<Byte> outputByteOpt(String parameterName) throws SQLException {
        try {
            return Option.apply(outputByte(parameterName));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public Option<Short> outputShortOpt(String parameterName) throws SQLException {
        try {
            return Option.apply(outputShort(parameterName));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public Option<Integer> outputIntOpt(String parameterName) throws SQLException {
        try {
            return Option.apply(outputInt(parameterName));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public Option<Long> outputLongOpt(String parameterName) throws SQLException {
        try {
            return Option.apply(outputLong(parameterName));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public Option<Float> outputFloatOpt(String parameterName) throws SQLException {
        try {
            return Option.apply(outputFloat(parameterName));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public Option<Double> outputDoubleOpt(String parameterName) throws SQLException {
        try {
            return Option.apply(outputDouble(parameterName));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public Option<Date> outputDateOpt(String parameterName) throws SQLException {
        try {
            return Option.apply(outputDate(parameterName));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public Option<Time> outputTimeOpt(String parameterName) throws SQLException {
        try {
            return Option.apply(outputTime(parameterName));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public Option<Timestamp> outputTimestampOpt(String parameterName) throws SQLException {
        try {
            return Option.apply(outputTimestamp(parameterName));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public Option<BigDecimal> outputBigDecimalOpt(String parameterName) throws SQLException {
        try {
            return Option.apply(outputBigDecimal(parameterName));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public Option<Blob> outputBlobOpt(String parameterName) throws SQLException {
        try {
            return Option.apply(outputBlob(parameterName));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public Option<Clob> outputClobOpt(String parameterName) throws SQLException {
        try {
            return Option.apply(outputClob(parameterName));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public Option<Date> outputDateOpt(String parameterName, Calendar cal) throws SQLException {
        try {
            return Option.apply(outputDate(parameterName));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public Option<Time> outputTimeOpt(String parameterName, Calendar cal) throws SQLException {
        try {
            return Option.apply(outputTime(parameterName));
        } catch (Exception e) {
            return Option.none();
        }
    }

    public Option<Timestamp> outputTimestampOpt(String parameterName, Calendar cal) throws SQLException {
        try {
            return Option.apply(outputTimestamp(parameterName));
        } catch (Exception e) {
            return Option.none();
        }
    }
}
