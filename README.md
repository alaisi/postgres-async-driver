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

```java
ConnectionPool pool = ...;
pool.query("select 'Hello world!' as message",
    result -> System.out.println(result.get(0).getString("message") ),
    error  -> error.printStackTrace() );
```
Or with Java <= 7:

```java
ConnectionPool pool = ...;
pool.query("select 'Hello world!' as message", new ResultHandler() {
    @Override
    public void onResult(ResultSet result) {
        System.out.println(result.get(0).getString("message"));
    }
}, new ErrorHandler() {
    @Override
    public void onError(Throwable error) {
        error.printStackTrace();
    }
});
```

### Connection pools

Connection pools are created with [`com.github.pgasync.ConnectionPoolBuilder`](https://github.com/alaisi/pg-async-driver/blob/master/src/main/java/com/github/pgasync/ConnectionPoolBuilder.java)

```java
ConnectionPool pool = return new ConnectionPoolBuilder()
    .database("db")
    .username("user")
    .password("pass")
    .poolSize(20)
    .build();
```

Each connection pool will start one IO thread used in communicating with PostgreSQL backend and executing callbacks.

### Prepared statements

Prepared statements use native PostgreSQL syntax $<index>. Supported parameter types are all primitive types, `String`, temporal types in `java.sql` package and `byte[]`.

```java
pool.query("insert into message(id, body) values($1, $2)", Arrays.asList(123, "hello"),
    result -> System.out.printf("Inserted %d rows", result.updatedRows() ),
    error  -> error.printStackTrace() );
```

### Transactions

A transactional unit of work is started with `begin()`. Queries issued against connection passed to callback are executed in the same transaction. The transaction must always be committed or rolled back.

```java
ErrorHandler err = error -> error.printStackTrace();
pool.begin((connection, transaction) -> {
    connection.query("select 1 as id",
        result -> {
            System.out.println("Result is %d", result.get(0).getLong("id"));
            transaction.commit(() -> System.out.println("Transaction committed"), err);
        },
        error -> {
            System.out.println("Query failed");
            transaction.rollback(() -> System.out.println("Transaction rolled back"), err);
        })
}, err)
```

## References
* [Scala postgresql-async](https://raw.github.com/mauricio/postgresql-async)
* [PostgreSQL JDBC Driver](http://jdbc.postgresql.org/about/about.html)

