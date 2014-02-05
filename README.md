# pg-async-driver - Asynchronous PostgreSQL Java driver

Pg-async-driver is a non-blocking Java driver for PostgreSQL. The driver supports connection pooling, prepared statements, transactions and all standard SQL types. 

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

TODO

## References
* [Scala postgresql-async](https://raw.github.com/mauricio/postgresql-async)
* [PostgreSQL JDBC Driver](http://jdbc.postgresql.org)

