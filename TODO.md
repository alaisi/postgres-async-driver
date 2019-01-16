## Refactoring
* Add arguments checks to most core methods.
* Make Connection#shutdown to use futures properly 

## Tests
* Make tests for all message flow scenarios.
+ Review other test cases.

### Test plan
* Errors while authentication
* Errors while simple query process
* Errors while extended query process
* Multiple result sets test
* Termination process test
* Prepared statement eviction test
+ Auto rollback transaction review
+ Ssl interaction test
+ Notifications test
+ Transactions general test
+ Connection pool general test
+ Queries pipelining vs errors test
* Connection pool failed connections and abandoned waiters tests
