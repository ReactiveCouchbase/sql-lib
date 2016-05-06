package org.reactivecouchbase.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Query {

    private final String preparedSqlQuery;
    private final List<String> paramNames;

    private Query(String preparedSqlQuery, List<String> paramNames) {
        this.preparedSqlQuery = preparedSqlQuery;
        this.paramNames = paramNames;
    }

    public String getPreparedSqlQuery() {
        return preparedSqlQuery;
    }

    public List<String> getParamNames() {
        return paramNames;
    }

    public static Query preparedQuery(String baseSql) {
        String finalSql = baseSql;
        List<String> names = new ArrayList<>();
        Pattern p = Pattern.compile("\\{[a-zA-Z0-9 \\-_]+\\}");
        Matcher m = p.matcher(baseSql);
        while (m.find()) {
            String name = m.group().replace("{", "").replace("}", "");
            names.add(name.trim());
            finalSql = finalSql.replace("{" + name + "}", "?");
        }
        return new Query(finalSql, names);
    }

}