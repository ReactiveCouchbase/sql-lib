package org.reactivecouchbase.sql.test;

import org.h2.Driver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.reactivecouchbase.sql.connection.Database;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.reactivecouchbase.sql.API.sql;
import static org.reactivecouchbase.sql.connection.ConnectionAPI.database;
import static org.reactivecouchbase.sql.connection.ConnectionAPI.provider;

public class RxTest {

    public static final Database DB = database(provider(new Driver(), "jdbc:h2:/tmp/test", "sa", ""));
    public static final ExecutorService EC = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

    @Before
    public void start() throws Exception {
        DB.withConnection(true, c -> {
            sql(c, "drop table if exists persons;").executeUpdate();
            sql(c, "create table persons (\n" +
                    "ID                    bigint not null,\n" +
                    "name                  varchar(1000) not null,\n" +
                    "surname               varchar(1000) not null,\n" +
                    "age                   bigint not null,\n" +
                    "cell                  varchar(1000),\n" +
                    "address               varchar(1000) not null,\n" +
                    "email                 varchar(1000) not null,\n" +
                    "constraint pk_person  primary key (id))\n" +
                    ";"
            ).executeUpdate();
            sql(c, "insert into persons values ( {id}, {name}, {surname}, {age}, {cell}, {address}, {email} );")
                    .on("id", 1)
                    .on("name", "John")
                    .on("surname", "Doe")
                    .on("age", 42)
                    .on("cell", "0606060606")
                    .on("address", "Here")
                    .on("email", "john.doe@gmail.com")
                    .executeUpdate();
            sql(c, "insert into persons values ( {id}, {name}, {surname}, {age}, {cell}, {address}, {email} );")
                    .on("id", 2)
                    .on("name", "John")
                    .on("surname", "Doe")
                    .on("age", 16)
                    .on("cell", "0606060606")
                    .on("address", "Here")
                    .on("email", "john.doe@gmail.com")
                    .executeUpdate();
            sql(c, "insert into persons values ( {id}, {name}, {surname}, {age}, {cell}, {address}, {email} );")
                    .on("id", 3)
                    .on("name", "John")
                    .on("surname", "Doe")
                    .on("age", 90)
                    .on("cell", "0606060606")
                    .on("address", "Here")
                    .on("email", "john.doe@gmail.com")
                    .executeUpdate();
        });
    }

    @Test
    public void testBasicFlow() throws Exception {
        DB.withConnection(false, c -> {
            List<SQLToolsTest.Person> list = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch latch = new CountDownLatch(1);
            sql(c, "select * from persons")
                    .asObservable(1, EC)
                    .map(row -> new SQLToolsTest.Person(
                        row.lng("id"),
                        row.str("name"),
                        row.str("surname"),
                        row.lng("age"),
                        row.str("cell"),
                        row.str("address"),
                        row.str("email")
                    ))
                    .filter(person -> person.age > 18L)
                    .subscribe(
                        person -> {
                            System.out.println("ping");
                            list.add(person);
                            latch.countDown();
                        },
                        error -> {
                            System.out.println("Handled error");
                            error.printStackTrace();
                            latch.countDown();
                        }
                    );
            try {
                latch.await(10, TimeUnit.SECONDS);
                for (SQLToolsTest.Person p : list) {
                    Assert.assertEquals("John", p.name);
                    Assert.assertEquals("Doe", p.surname);
                }
            } catch (Exception e) {
                System.out.println("Unhandled error");
                e.printStackTrace();
            }
        });
    }

}
