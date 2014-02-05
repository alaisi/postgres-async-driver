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
pool.query("SELECT 'Hello world!' AS message", new ResultHandler() {
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

Or with Java 8:

```java
ConnectionPool pool = ...;
pool.query("SELECT 'Hello world!' AS message",
    (result) -> System.out.println(result.get(0).getString("message")),
    (error) -> error.printStackTrace() );
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

Each connection pool will start one IO thread used in communicating with PostgreSQL backend.

### Prepared statements

TODO

### Transactions

TODO

## References
* [Scala postgresql-async](https://raw.github.com/mauricio/postgresql-async)
* [PostgreSQL JDBC Driver](http://jdbc.postgresql.org/about/about.html)

