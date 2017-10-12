package com.example.operators;


import com.example.utils.ReactiveStreamsFlowBridge;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.SkipException;

import java.util.function.UnaryOperator;
import java.util.stream.Stream;

public class StreamPublisherTest extends PublisherVerification<Integer> {

    public StreamPublisherTest() {
        super(new TestEnvironment());
    }

    @Override
    public Publisher<Integer> createPublisher(long elements) {
        return ReactiveStreamsFlowBridge.toReactiveStreams(
                new StreamPublisher<Integer>(() -> Stream.iterate(0, UnaryOperator.identity()).limit(elements))
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
}
