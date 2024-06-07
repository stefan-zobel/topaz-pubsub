/*
 * Copyright 2024 Stefan Zobel
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.topaz.grpc.server;

import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import net.topaz.protobuf.ConsumerEvent;
import net.topaz.protobuf.ProducerEvent;
import net.topaz.protobuf.PublishRequest;
import net.topaz.protobuf.PublishResponse;
import net.topaz.protobuf.PublishResult;
import net.topaz.protobuf.SubscribeResponse;

public class PublishRequestSender implements StreamObserver<PublishRequest> {

    private final AtomicBoolean isCancelled = new AtomicBoolean();
    private final AtomicBoolean alreaydCompleted = new AtomicBoolean();
    private final Consumer<PublishRequestSender> deregisterCallback;
    private final String uuid;
    private SubmissionPublisher<SubscribeResponse> subscribeResponsePublisher;
    private StreamObserver<PublishResponse> observer;

    public PublishRequestSender(StreamObserver<PublishResponse> responseObserver,
            Consumer<PublishRequestSender> deregisterCallback,
            SubmissionPublisher<SubscribeResponse> subscribeResponsePublisher, String uuid) {
        this.observer = Objects.requireNonNull(responseObserver);
        this.deregisterCallback = Objects.requireNonNull(deregisterCallback);
        this.uuid = Objects.requireNonNull(uuid);
        this.subscribeResponsePublisher = Objects.requireNonNull(subscribeResponsePublisher);
    }

    @Override
    public void onNext(PublishRequest value) {
        if (isCancelled.get()) {
            return;
        }
        String topic = value.getTopic();
        int eventsCount = value.getEventsCount();
        ArrayList<ConsumerEvent> consumerEvents = new ArrayList<>(eventsCount);
        ArrayList<PublishResult> publishResults = new ArrayList<>(eventsCount);
        for (ProducerEvent event : value.getEventsList()) {
            ConsumerEvent consumerEvent = ConsumerEvent.newBuilder().setTopic(topic).setEvent(event).build();
            consumerEvents.add(consumerEvent);
            PublishResult result = PublishResult.newBuilder().setSucceeded(true).setCorrelationId(event.getId())
                    .build();
            publishResults.add(result);
        }
        SubscribeResponse subscribeResponse = SubscribeResponse.newBuilder().addAllEvents(consumerEvents).build();
        if (!isCancelled.get() && !subscribeResponsePublisher.isClosed()) {
            subscribeResponsePublisher.submit(subscribeResponse);
        }
        PublishResponse response = PublishResponse.newBuilder().addAllResults(publishResults).build();
        if (!isCancelled.get() && !alreaydCompleted.get()) {
            observer.onNext(response);
        }
    }

    @Override
    public void onError(Throwable t) {
        if (!isCancelled.get() && !alreaydCompleted.get()) {
            observer.onError(Status.fromThrowable(t).asRuntimeException());
        }
    }

    @Override
    public void onCompleted() {
        if (!isCancelled.get() && alreaydCompleted.compareAndSet(false, true)) {
            observer.onCompleted();
        }
    }

    public void cancel() {
        if (isCancelled.compareAndSet(false, true)) {
            if (alreaydCompleted.compareAndSet(false, true)) {
                observer.onCompleted();
            }
            observer = null;
            subscribeResponsePublisher = null;
            deregisterCallback.accept(this);
        }
    }

    public String getUUID() {
        return uuid;
    }
}
