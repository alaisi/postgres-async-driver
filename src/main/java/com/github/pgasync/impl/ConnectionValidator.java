/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.pgasync.impl;


import com.github.pgasync.Connection;
import com.github.pgasync.Row;
import rx.Observable;
import rx.Subscriber;

/**
 * @author Antti Laisi
 */
public class ConnectionValidator {

    final String validationQuery;
    final boolean validateSocket;

    public ConnectionValidator(String validationQuery, boolean validateSocket) {
        // Trimmed as empty means no query for backwards compatibility
        this.validationQuery = validationQuery == null || validationQuery.trim().isEmpty() ? null : validationQuery;
        this.validateSocket = validateSocket;
    }

    public Observable<Connection> validate(Connection connection) {
        Observable<Connection> ret = Observable.just(connection);
        if (validationQuery != null) {
            ret = ret.flatMap(conn -> connection.queryRows(validationQuery)
                    .lift(subscriber -> new Subscriber<Row>() {
                        @Override
                        public void onError(Throwable e) {
                            subscriber.onError(e);
                        }
                        @Override
                        public void onCompleted() {
                            subscriber.onNext(connection);
                            subscriber.onCompleted();
                        }
                        @Override
                        public void onNext(Row row) { }
                    }));
        }
        if (validateSocket) {
            ret = ret.doOnNext(conn -> {
                if (conn instanceof PgConnection && !((PgConnection) conn).isConnected()) {
                    throw new IllegalStateException("Channel is closed");
                }
            });
        }
        return ret;
    }

}
