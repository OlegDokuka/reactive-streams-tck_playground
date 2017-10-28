package com.example.operators;

import java.util.*;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class StreamPublisher<T> implements Flow.Publisher<T> {
    private final Supplier<Stream<? extends T>> streamSupplier;

    public StreamPublisher(Supplier<Stream<? extends T>> streamSupplier) {
        this.streamSupplier = streamSupplier;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) {
        Iterator<? extends T> iterator = null;

        try {
            iterator = streamSupplier.get().iterator();
        } catch (Throwable e) {
            subscriber.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long n) {

                }

                @Override
                public void cancel() {

                }
            });
            subscriber.onError(e);
            return;
        }

        StreamSubscription subscription = new StreamSubscription(subscriber, iterator);
        subscriber.onSubscribe(subscription);
    }

    private class StreamSubscription implements Flow.Subscription {
        private final Flow.Subscriber<? super T> subscriber;
        private final Iterator<? extends T> iterator;
        private final AtomicBoolean isTerminated = new AtomicBoolean();
        private final AtomicLong demand = new AtomicLong();
        private final AtomicReference<Throwable> error = new AtomicReference<>();

        StreamSubscription(Flow.Subscriber<? super T> subscriber, Iterator<? extends T> iterator) {
            this.subscriber = subscriber;
            this.iterator = iterator;
        }

        @Override
        public void request(long n) {
            if (n <= 0) {
                error.compareAndSet(null, new IllegalArgumentException("negative subscription request"));
                n = 1;
            }

            for (;;) {
                long r = demand.get();
                long u = r + n;
                if (u < 0L) {
                    u = Long.MAX_VALUE;
                }
                if (demand.compareAndSet(r, u)) {
                    if (r != 0L) {
                        return;
                    }
                    break;
                }
            }

            for (;;) {
                long e = 0L;

                while (e != n) {
                    if (isTerminated()) {
                        return;
                    }

                    if (error.get() != null) {
                        subscriber.onError(error.get());
                        return;
                    }

                    boolean has = false;
                    T value = null;

                    try {
                        has = iterator.hasNext();
                        if (has) {
                            value = iterator.next();
                        }
                    } catch (Throwable ex) {
                        subscriber.onError(ex);
                        return;
                    }

                    if (!has) {
                        subscriber.onComplete();
                        return;
                    }

                    subscriber.onNext(value);
                    e++;
                }

                n = demand.addAndGet(-e);
                if (n == 0) {
                    break;
                }
            }
        }

        @Override
        public void cancel() {
            terminate();
        }

        private boolean terminate() {
            return isTerminated.getAndSet(true);
        }

        private boolean isTerminated() {
            return isTerminated.get();
        }
    }

    public static void main(String[] args) {
        new StreamPublisher<>(() -> Stream.of(1, 2, 3, 4, 5, 6))
                .subscribe(new Flow.Subscriber<>() {
                    @Override
                    public void onSubscribe(Flow.Subscription subscription) {
                        System.out.println("Subscribed");
                        subscription.request(6);
                    }

                    @Override
                    public void onNext(Integer item) {
                        System.out.println(item);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        System.out.println("Error: " + throwable);
                    }

                    @Override
                    public void onComplete() {
                        System.out.println("Complete");
                    }
                });
    }
}
