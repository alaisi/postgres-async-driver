# postgres-async-driver - Asynchronous PostgreSQL Java driver

Postgres-async-driver is a non-blocking Java driver for PostgreSQL. The driver supports connection pooling, prepared statements, transactions, all standard SQL types and custom column types. 

## Download

Postgres-async-driver is available on [Maven Central](http://search.maven.org/#search|ga|1|g%3A%22com.github.alaisi.pgasync%22).

```xml
<dependency>
    <groupId>com.github.alaisi.pgasync</groupId>
    <artifactId>postgres-async-driver</artifactId>
    <version>0.6</version>
</dependency>
```

## Usage

### Hello world

Queries are submitted to a `Db` with success and failure callbacks.

```java
Db db = ...;
db.query("select 'Hello world!' as message",
    result -> out.println(result.row(0).getString("message") ),
    error  -> error.printStackTrace() );
```

### Creating a Db

Db is usually a connection pool that is created with [`com.github.pgasync.ConnectionPoolBuilder`](https://github.com/alaisi/postgres-async-driver/blob/master/src/main/java/com/github/pgasync/ConnectionPoolBuilder.java)

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
db.query("insert into message(id, body) values($1, $2)", Arrays.asList(123, "hello"),
    result -> out.printf("Inserted %d rows", result.updatedRows() ),
    error  -> error.printStackTrace() );
```

### Transactions

A transactional unit of work is started with `begin()`. Queries issued to transaction passed to callback are executed in the same transaction and the tx is automatically rolled back on query failure.

```java
Consumer<Throwable> onError = error -> error.printStackTrace();
db.begin(transaction -> {
    transaction.query("select 1 as id",
        result -> {
            out.printf("Result is %d", result.row(0).getLong("id"));
            transaction.commit(() -> out.println("Transaction committed"), onError);
        },
        error -> err.println("Query failed, tx is now rolled back"))
}, onError)
```

### Custom data types

Support for additional data types requires registering converters to [`com.github.pgasync.ConnectionPoolBuilder`](https://github.com/alaisi/postgres-async-driver/blob/master/src/main/java/com/github/pgasync/ConnectionPoolBuilder.java)

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

