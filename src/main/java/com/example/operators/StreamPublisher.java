package com.example.operators;

import java.util.Iterator;
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
        StreamSubscription subscription = new StreamSubscription(subscriber);
        subscriber.onSubscribe(subscription);
        subscription.doOnSubscribed();
    }

    private class StreamSubscription implements Flow.Subscription {
        private final Flow.Subscriber<? super T> subscriber;
        private final Iterator<? extends T> iterator;
        private final AtomicBoolean isTerminated = new AtomicBoolean(false);
        private final AtomicLong demand = new AtomicLong();
        private final AtomicReference<Throwable> error = new AtomicReference<>();

        StreamSubscription(Flow.Subscriber<? super T> subscriber) {
            this.subscriber = subscriber;
            Iterator<? extends T> iterator = null;

            try {
                iterator = streamSupplier.get().iterator();
            } catch (Throwable e) {
                error.set(e);
            }

            this.iterator = iterator;
        }

        @Override
        public void request(long n) {
            if (n <= 0 && !terminate()) {
                subscriber.onError(new IllegalArgumentException("negative subscription request"));
                return;
            }

            for (; ; ) {
                long currentDemand = demand.getAcquire();

                if (currentDemand == Long.MAX_VALUE) {
                    return;
                }

                long adjustedDemand = currentDemand + n;

                if (adjustedDemand < 0L) {
                    adjustedDemand = Long.MAX_VALUE;
                }

                if (demand.compareAndSet(currentDemand, adjustedDemand)) {
                    if (currentDemand > 0) {
                        return;
                    }

                    break;
                }
            }

            for (; demand.get() > 0 && iterator.hasNext() && !isTerminated(); demand.decrementAndGet()) {
                try {
                    subscriber.onNext(iterator.next());
                } catch (Throwable e) {
                    if (!terminate()) {
                        subscriber.onError(e);
                    }
                }
            }

            if (!iterator.hasNext() && !terminate()) {
                subscriber.onComplete();
            }
        }

        @Override
        public void cancel() {
            terminate();
        }

        void doOnSubscribed() {
            Throwable throwable = error.get();
            if (throwable != null && !terminate()) {
                subscriber.onError(throwable);
            }
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
