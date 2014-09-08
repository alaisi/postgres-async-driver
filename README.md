# pg-async-driver - Asynchronous PostgreSQL Java driver

Pg-async-driver is a non-blocking Java driver for PostgreSQL. The driver supports connection pooling, prepared statements, transactions and all standard SQL types. 

## Download

Pg-async-driver is available on [Maven Central](http://search.maven.org/#search|ga|1|g%3A%22com.github.alaisi.pgasync%22).

```xml
<dependency>
    <groupId>com.github.alaisi.pgasync</groupId>
    <artifactId>pg-async-driver</artifactId>
    <version>0.1</version>
</dependency>
```

## Usage

### Hello world

Queries are submitted to a `Db` with success and failure callbacks.

```java
Db db = ...;
db.query("select 'Hello world!' as message",
    result -> out.println(result.get(0).getString("message") ),
    error  -> error.printStackTrace() );
```

### Connection pools

Connection pools are created with [`com.github.pgasync.ConnectionPoolBuilder`](https://github.com/alaisi/postgres-async-driver/blob/master/src/main/java/com/github/pgasync/ConnectionPoolBuilder.java)

```java
Db db = return new ConnectionPoolBuilder()
    .hostname("localhost")
    .port(5432)
    .database("db")
    .username("user")
    .password("pass")
    .poolSize(20)
    .build();
```

Each connection pool will start one IO thread used in communicating with PostgreSQL backend and executing callbacks.

### Prepared statements

Prepared statements use native PostgreSQL syntax `$index`. Supported parameter types are all primitive types, `String`, `BigDecimal`, `BigInteger`, temporal types in `java.sql` package and `byte[]`.

```java
db.query("insert into message(id, body) values($1, $2)", Arrays.asList(123, "hello"),
    result -> out.printf("Inserted %d rows", result.updatedRows() ),
    error  -> error.printStackTrace() );
```

### Transactions

A transactional unit of work is started with `begin()`. Queries issued to transaction passed to callback are executed in the same transaction and the tx is automatically rolled back on query failure.

```java
ErrorHandler err = (error) -> error.printStackTrace();
db.begin((transaction) -> {
    transaction.query("select 1 as id",
        result -> {
            out.printf("Result is %d", result.get(0).getLong("id"));
            transaction.commit(() -> System.out.println("Transaction committed"), err);
        },
        error -> err.println("Query failed, tx is rolled back"))
}, err)
```

## Used in

* [clj-postgres-async for Clojure](https://github.com/alaisi/clj-postgres-async)

## References
* [Scala postgresql-async](https://raw.github.com/mauricio/postgresql-async)
* [PostgreSQL JDBC Driver](http://jdbc.postgresql.org/about/about.html)

