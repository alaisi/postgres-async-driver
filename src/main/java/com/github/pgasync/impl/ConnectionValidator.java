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

import java.util.concurrent.TimeUnit;

/**
 * @author Antti Laisi
 */
public class ConnectionValidator {
    final String query;
    final int timeout;

    public ConnectionValidator(String query, int timeout) {
        this.query = query;
        this.timeout = timeout;
    }

    public Observable<Connection> validate(Connection connection) {
        return connection
                .queryRows(query)
                .timeout(timeout, TimeUnit.MILLISECONDS)
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
                });
    }

}
