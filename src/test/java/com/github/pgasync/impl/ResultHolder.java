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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.github.pgasync.ResultSet;
import com.github.pgasync.callback.ErrorHandler;
import com.github.pgasync.callback.ResultHandler;

/**
 * Helper for waiting query completion.
 * 
 * @author Antti Laisi
 */
class ResultHolder implements ResultHandler, ErrorHandler {

    CountDownLatch latch = new CountDownLatch(1);
    ResultSet resultSet;
    Throwable error;

    @Override
    public void onResult(ResultSet result) {
        resultSet = result;
        latch.countDown();
    }

    @Override
    public void onError(Throwable t) {
        error = t;
        latch.countDown();
    }

    public ResultSet result() {
        try {
            if (!latch.await(500, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for result");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        latch = new CountDownLatch(1);
        if (error != null) {
            throw error instanceof RuntimeException ? (RuntimeException) error : new RuntimeException(error);
        }
        return resultSet;
    }
}