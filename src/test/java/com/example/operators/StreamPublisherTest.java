package com.example.operators;


import com.example.utils.ReactiveStreamsFlowBridge;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

public class StreamPublisherTest extends PublisherVerification<Integer> {
    private final TestEnvironment env;

    public StreamPublisherTest() throws NoSuchFieldException, IllegalAccessException {
        super(new TestEnvironment(400));

        Field env = PublisherVerification.class.getDeclaredField("env");
        env.setAccessible(true);
        this.env = (TestEnvironment) env.get(this);
    }

    @Override
    public Publisher<Integer> createPublisher(long elements) {
        return ReactiveStreamsFlowBridge.toReactiveStreams(
                new StreamPublisher<>(() -> Stream.iterate(0, UnaryOperator.identity()).limit(elements))
        );
    }

    @Override
    public Publisher<Integer> createFailedPublisher() {
        return ReactiveStreamsFlowBridge.toReactiveStreams(
                new StreamPublisher<Integer>(() -> {
                    throw new RuntimeException();
                })
        );
    }

    @Test
    public void required_spec317_mustSupportACumulativePendingElementCountGreaterThenLongMaxValue() throws Throwable {
        final int totalElements = 50;

        activePublisherTest(totalElements, true, pub -> {
            final TestEnvironment.ManualSubscriber<Integer> sub = env.newManualSubscriber(pub);
            new Thread(() -> sub.request(Long.MAX_VALUE)).start();
            new Thread(() -> sub.request(Long.MAX_VALUE)).start();

            sub.nextElements(totalElements);
            sub.expectCompletion();

            try {
                env.verifyNoAsyncErrorsNoDelay();
            } finally {
                sub.cancel();
            }

        });
    }
}
