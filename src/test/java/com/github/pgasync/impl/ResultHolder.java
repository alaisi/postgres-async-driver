package com.github.pgasync.impl;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.github.pgasync.ResultSet;
import com.github.pgasync.callback.ErrorHandler;
import com.github.pgasync.callback.ResultHandler;

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