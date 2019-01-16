# postgres-async-driver - Asynchronous PostgreSQL Java driver

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.alaisi.pgasync/postgres-async-driver/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.alaisi.pgasync/postgres-async-driver/)

Postgres-async-driver is a non-blocking Java driver for PostgreSQL. The driver supports connection pooling, prepared statements, transactions, all standard SQL types and custom column types. 

## Download

Postgres-async-driver is available on [Maven Central](http://search.maven.org/#search|ga|1|g%3A%22com.github.alaisi.pgasync%22).

```xml
<dependency>
    <groupId>com.github.alaisi.pgasync</groupId>
    <artifactId>postgres-async-driver</artifactId>
    <version>0.9</version>
</dependency>
```

## Usage

### Hello world

Querying for a set returns an [rx.Observable](http://reactivex.io/documentation/observable.html) that emits a single [ResultSet](https://github.com/alaisi/postgres-async-driver/blob/master/src/main/java/com/github/pgasync/ResultSet.java).

```java
Db db = ...;
db.querySet("select 'Hello world!' as message")
    .map(result -> result.row(0).getString("message"))
    .subscribe(System.out::println)

// => Hello world
```

Querying for rows returns an [rx.Observable](http://reactivex.io/documentation/observable.html) that emits 0-n [Rows](https://github.com/alaisi/postgres-async-driver/blob/master/src/main/java/com/github/pgasync/Row.java). The rows are emitted immediately as they are received from the server instead of waiting for the entire query to complete.

```java
Db db = ...;
db.queryRows("select unnest('{ hello, world }'::text[] as message)")
    .map(row -> row.getString("message"))
    .subscribe(System.out::println)

// => hello
// => world
```

### Creating a Db

Db is a connection pool that is created with [`ConnectionPoolBuilder`](https://github.com/alaisi/postgres-async-driver/blob/master/src/main/java/com/github/pgasync/ConnectionPoolBuilder.java)

```java
Db db = new ConnectionPoolBuilder()
    .hostname("localhost")
    .port(5432)
    .database("db")
    .username("user")
    .password("pass")
    .poolSize(20)
    .build();
```

Each connection *pool* will start only one IO thread used in communicating with PostgreSQL backend and executing callbacks for all connections.

### Prepared statements

Prepared statements use native PostgreSQL syntax `$index`. Supported parameter types are all primitive types, `String`, `BigDecimal`, `BigInteger`, `UUID`, temporal types in `java.sql` package and `byte[]`.

```java
db.querySet("insert into message(id, body) values($1, $2)", 123, "hello")
    .subscribe(result -> out.printf("Inserted %d rows", result.affectedRows() ));
```

### Transactions

A transactional unit of work is started with `begin()`. Queries issued to the emitted [Transaction](https://github.com/alaisi/postgres-async-driver/blob/master/src/main/java/com/github/pgasync/Transaction.java) are executed in the same transaction and the tx is automatically rolled back on query failure.

```java
db.begin()
    .flatMap(tx -> tx.querySet("insert into products (name) values ($1) returning id", "saw")
        .map(productsResult -> productsResult.row(0).getLong("id"))
        .flatMap(id -> tx.querySet("insert into promotions (product_id) values ($1)", id))
        .flatMap(promotionsResult -> tx.commit())
    ).subscribe(
        __ -> System.out.println("Transaction committed"),
        Throwable::printStackTrace);

```

### Custom data types

Support for additional data types requires registering converters to [`ConnectionPoolBuilder`](https://github.com/alaisi/postgres-async-driver/blob/master/src/main/java/com/github/pgasync/ConnectionPoolBuilder.java)

```java
class JsonConverter implements Converter<example.Json> {
    @Override
    public Class<example.Json> type() {
        return example.Json.class;
    }
    @Override
    public byte[] from(example.Json json) {
        return json.toBytes();
    }
    @Override
    public example.Json to(Oid oid, byte[] value) {
        return new example.Json(new String(value, UTF_8));
    }
}

Db db = return new ConnectionPoolBuilder()
    ...
    .converters(new JsonConverter())
    .build();
```

## Used in

* [postgres.async for Clojure](https://github.com/alaisi/postgres.async)

## References
* [Scala postgresql-async](https://github.com/mauricio/postgresql-async)
* [PostgreSQL JDBC Driver](http://jdbc.postgresql.org/about/about.html)

