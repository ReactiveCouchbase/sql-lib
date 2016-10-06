package org.reactivecouchbase.sql.test;

import org.h2.Driver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.reactivecouchbase.functional.Option;
import org.reactivecouchbase.json.Json;
import org.reactivecouchbase.sql.Batch;
import org.reactivecouchbase.sql.connection.Database;
import rx.Observable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.reactivecouchbase.sql.API.*;
import static org.reactivecouchbase.sql.connection.ConnectionAPI.database;
import static org.reactivecouchbase.sql.connection.ConnectionAPI.provider;

public class SQLToolsTest {

    public static final Database DB = database(provider(new Driver(), "jdbc:h2:/tmp/test", "sa", ""));

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
        DB.withConnection(false, c -> {
            sql(c, "select * from persons").foreach(row -> {
                assert row.isPresent("id");
                assert row.isPresent("name");
                assert row.isPresent("age");
                assert row.isPresent("address");
                assert row.isPresent("email");
                assert row.isPresent("surname");
                assert !row.isPresent("wouuuuhhh");
            });
        });
    }

    @Test
    public void testReflectParser() {
        Assert.assertEquals(3, Person.findAll().size());
        Assert.assertEquals(1, Person.findAllBetween(18, 80).size());
        Assert.assertEquals(2, Person.findAllBetween(18, 100).size());
        Assert.assertEquals(2, Person.findAllBetween(10, 80).size());
        Assert.assertEquals(3, Person.findAllBetween(10, 100).size());
        Assert.assertEquals(3, Person.count());
    }

    @Test
    public void testManualParser() {
        List<Person> persons = DB.withConnection(false, c -> {
            return sql(c, "SELECT id, name, surname, age, cell, address, email FROM Persons")
                .collect(row -> {
                    return Option.some(
                        new Person(
                            row.lng("id"),
                            row.str("name"),
                            row.str("surname"),
                            row.lng("age"),
                            row.str("cell"),
                            row.str("address"),
                            row.str("email")
                        )
                    );
                });
        });
        Assert.assertEquals(3, persons.size());
    }

    @Test
    public void testIntParser() {
        List<Integer> ids = DB.withConnection(false, c -> {
            return sql(c, "SELECT id FROM Persons").collect(integerParser("id"));
        });
        Assert.assertEquals(3, ids.size());
        int total = 0;
        for (Integer i : ids) {
            total += i;
        }
        Assert.assertEquals(6, total);
    }

    @Test
    public void testBatchInsertion() {
        DB.withConnection(false, c -> {
            Batch personBatch = batch(c, 10, "insert into persons values ( {id}, {name}, {surname}, {age}, {cell}, {address}, {email} );");
            for (int i = 0; i < 25; i++) {
                personBatch
                    .on("id", i + 500)
                    .on("name", "John")
                    .on("surname", "Doe")
                    .on("age", 42)
                    .on("cell", "0606060606")
                    .on("address", "Here")
                    .on("email", "bob@bob.com")
                    .batch();
            }
            Integer howmany = sql(c, "SELECT COUNT(*) as howmany from Persons where email = 'bob@bob.com'").collectSingle(integerParser("howmany")).getOrElse(0);
            Assert.assertEquals(new Integer(20), howmany);
            personBatch.executeBatch();
            howmany = sql(c, "SELECT COUNT(*) as howmany from Persons where email = 'bob@bob.com'").collectSingle(integerParser("howmany")).getOrElse(0);
            Assert.assertEquals(new Integer(25), howmany);
        });
    }

    @Test
    public void testBatchInsertionWithTriggers() {
        DB.withConnection(true, c -> {
            sql(c, "drop table if exists table1;").executeUpdate();
            sql(c, "drop table if exists table2;").executeUpdate();
            sql(c, "drop table if exists table3;").executeUpdate();
            sql(c,
                    "create table table1 (\n" +
                "ID                    bigint not null,\n" +
                "name                  varchar(1000) not null,\n" +
                "surname               varchar(1000) not null,\n" +
                "age                   bigint not null,\n" +
                "cell                  varchar(1000),\n" +
                "address               varchar(1000) not null,\n" +
                "email                 varchar(1000) not null,\n" +
                "constraint pk_table1  primary key (id))\n" +
                ";"
            ).executeUpdate();
            sql(c,
                    "create table table2 (\n" +
                "ID                    bigint not null,\n" +
                "name                  varchar(1000) not null,\n" +
                "table1                bigint not null,\n" +
                "constraint pk_table2  primary key (id))\n" +
                ";"
            ).executeUpdate();
            sql(c,
                    "create table table3 (\n" +
                "ID                    bigint not null,\n" +
                "name                  varchar(1000) not null,\n" +
                "table1                bigint not null,\n" +
                "constraint pk_table3  primary key (id))\n" +
                ";"
            ).executeUpdate();

            Batch table1Batch = batch(c, 10, "insert into table1 values ( {id}, {name}, {surname}, {age}, {cell}, {address}, {email} );");
            Batch table2Batch = batch(c, 10, "insert into table2 values ( {id}, {name}, {table1} );");
            Batch table3Batch = batch(c, 10, "insert into table3 values ( {id}, {name}, {table1} );");
            table1Batch.triggerBeforeSelf(table2Batch, table3Batch);
            //table1Batch.triggerAfterSelf(table2Batch, table3Batch);
            for (int i = 0; i < 25; i++) {
                int id = i + 500;
                table2Batch.on("id", id).on("name", "stuff").on("table1", id).batch();
                table3Batch.on("id", id).on("name", "stuff").on("table1", id).batch();
                table1Batch
                    .on("id", id)
                    .on("name", "John")
                    .on("surname", "Doe")
                    .on("age", 42)
                    .on("cell", "0606060606")
                    .on("address", "Here")
                    .on("email", "bob@bob.com")
                    .batch();
            }
            Integer howmany1 = sql(c, "SELECT COUNT(*) as howmany from table1").collectSingle(integerParser("howmany")).getOrElse(0);
            Integer howmany2 = sql(c, "SELECT COUNT(*) as howmany from table2").collectSingle(integerParser("howmany")).getOrElse(0);
            Integer howmany3 = sql(c, "SELECT COUNT(*) as howmany from table3").collectSingle(integerParser("howmany")).getOrElse(0);
            Assert.assertEquals(new Integer(20), howmany1);
            Assert.assertEquals(new Integer(20), howmany2);
            Assert.assertEquals(new Integer(20), howmany3);
            table1Batch.executeBatch();
            howmany1 = sql(c, "SELECT COUNT(*) as howmany from table1").collectSingle(integerParser("howmany")).getOrElse(0);
            howmany2 = sql(c, "SELECT COUNT(*) as howmany from table2").collectSingle(integerParser("howmany")).getOrElse(0);
            howmany3 = sql(c, "SELECT COUNT(*) as howmany from table3").collectSingle(integerParser("howmany")).getOrElse(0);
            Assert.assertEquals(new Integer(25), howmany1);
            Assert.assertEquals(new Integer(25), howmany2);
            Assert.assertEquals(new Integer(25), howmany3);
        });
    }

    @Test
    public void testBatchInsertionWithTriggersLinkedBatches() {
        DB.withConnection(true, c -> {
            sql(c, "drop table if exists table1;").executeUpdate();
            sql(c, "drop table if exists table2;").executeUpdate();
            sql(c, "drop table if exists table3;").executeUpdate();
            sql(c,
                    "create table table1 (\n" +
                            "ID                    bigint not null,\n" +
                            "name                  varchar(1000) not null,\n" +
                            "surname               varchar(1000) not null,\n" +
                            "age                   bigint not null,\n" +
                            "cell                  varchar(1000),\n" +
                            "address               varchar(1000) not null,\n" +
                            "email                 varchar(1000) not null,\n" +
                            "constraint pk_table1  primary key (id))\n" +
                            ";"
            ).executeUpdate();
            sql(c,
                    "create table table2 (\n" +
                            "ID                    bigint not null,\n" +
                            "name                  varchar(1000) not null,\n" +
                            "table1                bigint not null,\n" +
                            "constraint pk_table2  primary key (id))\n" +
                            ";"
            ).executeUpdate();
            sql(c,
                    "create table table3 (\n" +
                            "ID                    bigint not null,\n" +
                            "name                  varchar(1000) not null,\n" +
                            "table1                bigint not null,\n" +
                            "constraint pk_table3  primary key (id))\n" +
                            ";"
            ).executeUpdate();

            Batch table1Batch = batch(c, 10, "insert into table1 values ( {id}, {name}, {surname}, {age}, {cell}, {address}, {email} );");
            Batch table2Batch = batch(c, "insert into table2 values ( {id}, {name}, {table1} );");
            Batch table3Batch = batch(c, "insert into table3 values ( {id}, {name}, {table1} );");
            table2Batch.triggerBeforeSelf(table3Batch);
            table1Batch.triggerBeforeSelf(table2Batch);
            //table1Batch.triggerAfterSelf(table2Batch, table3Batch);
            for (int i = 0; i < 25; i++) {
                int id = i + 500;
                table2Batch.on("id", id).on("name", "stuff").on("table1", id).batch();
                table3Batch.on("id", id).on("name", "stuff").on("table1", id).batch();
                table1Batch
                    .on("id", id)
                    .on("name", "John")
                    .on("surname", "Doe")
                    .on("age", 42)
                    .on("cell", "0606060606")
                    .on("address", "Here")
                    .on("email", "bob@bob.com")
                    .batch();
            }
            Integer howmany1 = sql(c, "SELECT COUNT(*) as howmany from table1").collectSingle(integerParser("howmany")).getOrElse(0);
            Integer howmany2 = sql(c, "SELECT COUNT(*) as howmany from table2").collectSingle(integerParser("howmany")).getOrElse(0);
            Integer howmany3 = sql(c, "SELECT COUNT(*) as howmany from table3").collectSingle(integerParser("howmany")).getOrElse(0);
            Assert.assertEquals(new Integer(20), howmany1);
            Assert.assertEquals(new Integer(20), howmany2);
            Assert.assertEquals(new Integer(20), howmany3);
            table1Batch.executeBatch();
            howmany1 = sql(c, "SELECT COUNT(*) as howmany from table1").collectSingle(integerParser("howmany")).getOrElse(0);
            howmany2 = sql(c, "SELECT COUNT(*) as howmany from table2").collectSingle(integerParser("howmany")).getOrElse(0);
            howmany3 = sql(c, "SELECT COUNT(*) as howmany from table3").collectSingle(integerParser("howmany")).getOrElse(0);
            Assert.assertEquals(new Integer(25), howmany1);
            Assert.assertEquals(new Integer(25), howmany2);
            Assert.assertEquals(new Integer(25), howmany3);
        });
    }

    @Test
    public void testBatchInsertionWithNothing() {
        DB.withConnection(false, c -> {
            Batch personBatch = batch(c, 10, "insert into persons values ( {id}, {name}, {surname}, {age}, {cell}, {address}, {email} );");
            List<Integer> result = personBatch.executeBatch();
            Assert.assertEquals(0, result.size());
        });
    }

    public <T> ArrayList<T> append(ArrayList<T> list, T elem) {
        list.add(elem);
        return list;
    }

    @Test
    public void testStreams1() throws Exception {
        final ExecutorService ec = Executors.newFixedThreadPool(8);
        CountDownLatch latch = new CountDownLatch(1);
        DB.withRxConnection(false, c -> {
            return sql(c, "SELECT id, name, surname, age, cell, address, email FROM Persons")
                .asObservable(ec)
                .map(input -> "")
                .map(input -> input + "a")
                .map(input -> input + "b")
                .map(input -> input + "c")
                .map(String::toUpperCase)
                .reduce(new ArrayList<String>(), (a, b) -> this.append(a, b));
        }).subscribe(values -> {
            for (String str : values) {
                Assert.assertEquals("ABC", str);
            }
            Assert.assertEquals(3, values.size());
            latch.countDown();
        });
        latch.await();
    }

    @Test
    public void testStreams2() {
        final StringBuilder builder = new StringBuilder();
        final AtomicInteger counter = new AtomicInteger(0);
        DB.withConnection(true, c -> {
            sql(c, "insert into persons values ( {id}, {name}, {surname}, {age}, {cell}, {address}, {email} );")
                .on("id", 4)
                .on("name", "John")
                .on("surname", "Doe")
                .on("age", 42)
                .on("cell", "0606060606")
                .on("address", "Here")
                .on("email", "john.doe@gmail.com")
                .executeUpdate();
        });
        DB.withConnection(false, c -> {
            List<String> values = sql(c, "SELECT id, name, surname, age, cell, address, email FROM Persons")
                    .withPageOf(1)
                    .asStream()
                    .map(input -> counter.incrementAndGet())
                    .filter(input -> input != 2)
                    .map(input -> input + " : a")
                    .collect(input -> {
                        if (input.startsWith("4")) {
                            return Option.none();
                        } else if (input.startsWith("1")) {
                            return Option.some(input.toUpperCase());
                        } else {
                            return Option.some(input);
                        }
                    })
                    .map(input -> input + "b")
                    .andThen(builder::append)
                    .map(input -> input + "c")
                    .map(String::toUpperCase)
                    .run();
            for (String str : values) {
                Assert.assertTrue(str.endsWith(" : ABC"));
            }
            Assert.assertEquals(2, values.size());
            Assert.assertEquals("1 : Ab3 : ab", builder.toString());
        });
    }

    @Test
    public void testToJson() {
        DB.withConnection(false, c -> {
            System.out.println(Json.prettyPrint(sql(c, "SELECT id, name, surname, age, cell, address, email FROM Persons").asJsArray()));
        });
    }

    @Test
    public void testPrepared() {
        DB.withConnection(false, c -> {
            for (int i = 0; i < 5000; i ++) {
                 Json.prettyPrint(sql(c, "SELECT id, name, surname, age, cell, address, email FROM Persons where id = {id}").on("id", i).asJsArray());
            }
        });
    }

    @Test
    public void testInsertNull() {
        DB.withConnection(true, c -> {
            sql(c, "insert into persons values ( {id}, {name}, {surname}, {age}, {cell}, {address}, {email} );")
                .on("id", 520)
                .on("name", "John")
                .on("surname", "Doe")
                .on("age", 42)
                .on("cell", null)
                .on("address", "Here")
                .on("email", "john.doe@gmail.com")
                .executeUpdate();
            sql(c, "insert into persons values ( {id}, {name}, {surname}, {age}, {cell}, {address}, {email} );")
                .on("id", 521)
                .on("name", "John")
                .on("surname", "Doe")
                .on("age", 16)
                .on("cell", null)
                .on("address", "Here")
                .on("email", "john.doe@gmail.com")
                .executeUpdate();
            sql(c, "insert into persons values ( {id}, {name}, {surname}, {age}, {cell}, {address}, {email} );")
                .on("id", 522)
                .on("name", "John")
                .on("surname", "Doe")
                .on("age", 90)
                .on("cell", null)
                .on("address", "Here")
                .on("email", "john.doe@gmail.com")
                .executeUpdate();
        });
    }

    public static class Person {

        public Long id;
        public String name;
        public String surname;
        public Long age;
        public String cell;
        public String address;
        public String email;

        public Person() {}

        public Person(Long id, String name, String surname, Long age,
                      String cell, String address, String email) {
            this.id = id;
            this.name = name;
            this.surname = surname;
            this.age = age;
            this.cell = cell;
            this.address = address;
            this.email = email;
        }

        public static int count() {
            return DB.withConnection(false, c -> {
                return sql(c, "select count(*) as p from persons").collectSingle(integerParser("p")).getOrElse(0);
            });
        }

        public static List<Person> findAll() {
            return DB.withConnection(false, c -> {
                return sql(c, "SELECT id, name, surname, age, cell, address, email FROM Persons").collect(row -> {
                    return Option.some(new Person(
                            row.lng("id"),
                            row.str("name"),
                            row.str("surname"),
                            row.lng("age"),
                            row.str("cell"),
                            row.str("address"),
                            row.str("email")
                    ));
                });
            });
        }

        public static List<Person> findAllBetween(final int low, final int high) {
            return DB.withConnection(false, conn -> {
                return sql(conn,
                        "SELECT id, name, surname, age, cell, address, email " +
                                "FROM Persons WHERE age > { low} AND age < {high}")
                        .on("low", low)
                        .on("high", high)
                        .collect(row -> {
                            return Option.some(new Person(
                                    row.lng("id"),
                                    row.str("name"),
                                    row.str("surname"),
                                    row.lng("age"),
                                    row.str("cell"),
                                    row.str("address"),
                                    row.str("email")
                            ));
                        });
            });
        }

        @Override
        public String toString() {
            return "Person{" + "id=" + id + ", name=" + name
                    + ", surname=" + surname + ", age=" + age
                    + ", cell=" + cell + ", address=" + address
                    + ", email=" + email + '}';
        }
    }
}
