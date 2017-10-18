package com.github.pgasync.impl.netty;

import com.github.pgasync.SqlException;
import com.github.pgasync.impl.message.DataRow;
import com.github.pgasync.impl.message.Message;
import com.github.pgasync.impl.message.ReadyForQuery;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.*;
import rx.functions.Cancellable;
import rx.internal.operators.BackpressureUtils;

import java.util.Optional;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.github.pgasync.impl.netty.ProtocolUtils.asSqlException;

class BackpressuredEmitter implements Emitter<Message>, Subscription, Producer {
    private static final int BUFFER_SIZE = 256;
    private static final Logger LOG = LoggerFactory.getLogger(BackpressuredEmitter.class);

    private final BlockingDeque<Message> buffer = new LinkedBlockingDeque<>(BUFFER_SIZE);
    private final AtomicLong requested = new AtomicLong();

    private final ChannelHandlerContext context;
    private final Subscriber<Message> subscriber;

    private Cancellable cancellable;
    private volatile boolean completed;
    private SqlException sqlException;

    private BackpressuredEmitter(ChannelHandlerContext context, Subscriber<Message> subscriber) {
        this.context = context;
        this.subscriber = subscriber;
    }

    boolean completed() {
        return completed;
    }

    @Override
    public void setSubscription(Subscription s) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void setCancellation(Cancellable c) {
        cancellable = c;
    }

    @Override
    public long requested() {
        return requested.get();
    }

    @Override
    public void onCompleted() {
        if (completed)
            return;

        drain();
        completed = true;
        subscriber.onCompleted();
    }

    @Override
    public void onError(Throwable e) {
        if (completed)
            return;

        drain();
        completed = true;
        subscriber.onError(e);
    }

    @Override
    public synchronized void onNext(Message t) {
        try {
            buffer.addFirst(t);
            drain();
        } catch (Exception e) {
            onError(e);
        }
    }

    @Override
    public void request(long n) {
        BackpressureUtils.getAndAddRequest(requested, n);
        drain();
    }

    @Override
    public void unsubscribe() {
        Optional.ofNullable(cancellable)
                .ifPresent(c -> {
                    try {
                        c.cancel();
                    } catch (Exception e) {
                        if (completed)
                            LOG.error("Failed to call cancelable", e);
                        else
                            subscriber.onError(e);
                    }
                });

        buffer.clear();
    }

    @Override
    public boolean isUnsubscribed() {
        return subscriber.isUnsubscribed();
    }

    private synchronized void drain() {
        while (requested.get() > 0 && buffer.size() > 0) {
            Optional.ofNullable(buffer.pollLast())
                    .ifPresent(message -> {
                        Optional<SqlException> maybeSqlException = asSqlException(message);

                        if (!maybeSqlException.isPresent()) {
                            if (sqlException != null && message == ReadyForQuery.INSTANCE)
                                onError(sqlException);
                            else
                                subscriber.onNext(message);
                            if (message instanceof DataRow)
                                requested.decrementAndGet();
                        } else
                            maybeSqlException.ifPresent(e -> sqlException = e);
                    });
        }

        if (requested.get() - buffer.size() > 0)
            context.executor().submit(() -> context.channel().read());
    }

    @SuppressWarnings("unchecked")
    static Observable.OnSubscribe<Message> create(Consumer<BackpressuredEmitter> emitter, ChannelHandlerContext context) {
        return subscriber -> {
            BackpressuredEmitter extEmitter = new BackpressuredEmitter(context, (Subscriber<Message>) subscriber);
            subscriber.add(extEmitter);
            subscriber.setProducer(extEmitter);
            emitter.accept(extEmitter);
        };
    }
}
