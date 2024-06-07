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
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SubmissionPublisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import net.topaz.protobuf.ConsumerEvent;
import net.topaz.protobuf.ProducerEvent;
import net.topaz.protobuf.PubSubGrpc.PubSubImplBase;
import net.topaz.protobuf.PublishRequest;
import net.topaz.protobuf.PublishResponse;
import net.topaz.protobuf.PublishResult;
import net.topaz.protobuf.SubscribeRequest;
import net.topaz.protobuf.SubscribeResponse;

public class GrpcService extends PubSubImplBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(GrpcService.class.getName());

    private SubmissionPublisher<SubscribeResponse> subscribeResponsePublisher;

    private final ConcurrentHashMap<String, SubscribeResponseReceiver> receivers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, PublishRequestSender> senders = new ConcurrentHashMap<>();

    public GrpcService(SubmissionPublisher<SubscribeResponse> subscribeResponsePublisher) {
        this.subscribeResponsePublisher = Objects.requireNonNull(subscribeResponsePublisher);
    }

    public void deregisterSubscribeResponseReceiver(SubscribeResponseReceiver receiver) {
        if (receivers.remove(receiver.getUUID()) != null) {
            LOGGER.info("SubscribeResponseReceiver '" + receiver.getUUID() + "' has deregistered");
        }
    }

    public void deregisterPublishRequestSender(PublishRequestSender sender) {
        if (senders.remove(sender.getUUID()) != null) {
            LOGGER.info("PublishRequestSender '" + sender.getUUID() + "' has deregistered");
        }
    }

    @Override
    public void publish(PublishRequest request, StreamObserver<PublishResponse> responseObserver) {
        String topic = request.getTopic();
        int eventsCount = request.getEventsCount();
        ArrayList<ConsumerEvent> consumerEvents = new ArrayList<>(eventsCount);
        ArrayList<PublishResult> publishResults = new ArrayList<>(eventsCount);
        for (ProducerEvent event : request.getEventsList()) {
            ConsumerEvent consumerEvent = ConsumerEvent.newBuilder().setTopic(topic).setEvent(event).build();
            consumerEvents.add(consumerEvent);
            PublishResult result = PublishResult.newBuilder().setSucceeded(true).setCorrelationId(event.getId())
                    .build();
            publishResults.add(result);
        }
        SubscribeResponse subscribeResponse = SubscribeResponse.newBuilder().addAllEvents(consumerEvents).build();
        if (!subscribeResponsePublisher.isClosed()) {
            subscribeResponsePublisher.submit(subscribeResponse);
        }
        PublishResponse response = PublishResponse.newBuilder().addAllResults(publishResults).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<PublishRequest> publishStream(StreamObserver<PublishResponse> responseObserver) {
        String uuid = ServerUuidInterceptor.UUID_CONTEXT.get();
        PublishRequestSender sender = new PublishRequestSender(responseObserver, this::deregisterPublishRequestSender,
                subscribeResponsePublisher, uuid);
        senders.put(uuid, sender);
        LOGGER.info("PublishRequestSender '" + sender.getUUID() + "' is registering");
        Context.current().addListener(new Context.CancellationListener() {
            @Override
            public void cancelled(Context context) {
                sender.cancel();
                context.removeListener(this);
            }
        }, MoreExecutors.directExecutor());
        return sender;
    }

    @Override
    public void subscribe(SubscribeRequest request, StreamObserver<SubscribeResponse> responseObserver) {
        List<ByteString> topics = request.getTopicsList().asByteStringList();
        List<String> topicList = topics.stream().map(bs -> bs.toStringUtf8()).toList();
        String uuid = ServerUuidInterceptor.UUID_CONTEXT.get();
        SubscribeResponseReceiver receiver = new SubscribeResponseReceiver(responseObserver,
                this::deregisterSubscribeResponseReceiver, topicList, uuid);
        receivers.put(uuid, receiver);
        LOGGER.info("SubscribeResponseReceiver '" + receiver.getUUID() + "' is registering");
        Context.current().addListener(new Context.CancellationListener() {
            @Override
            public void cancelled(Context context) {
                receiver.cancel();
                context.removeListener(this);
            }
        }, MoreExecutors.directExecutor());
        if (!subscribeResponsePublisher.isClosed()) {
            subscribeResponsePublisher.subscribe(receiver);
        }
    }

    @Override
    public void unsubscribe(SubscribeRequest request, StreamObserver<SubscribeResponse> responseObserver) {
        String uuid = ServerUuidInterceptor.UUID_CONTEXT.get();
        SubscribeResponseReceiver sub = receivers.get(uuid);
        if (sub == null) {
            responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
            return;
        }
        List<ByteString> topics = request.getTopicsList().asByteStringList();
        List<String> topicList = topics.stream().map(bs -> bs.toStringUtf8()).toList();
        sub.removeTopics(topicList);
        if (!sub.hasTopics()) {
            sub.cancel();
            SubscribeResponse empty = SubscribeResponse.newBuilder().addEvents(ConsumerEvent.newBuilder().build())
                    .build();
            responseObserver.onNext(empty);
        } else {
            // return the topics still subscribed to
            SubscribeResponse.Builder builder = SubscribeResponse.newBuilder();
            for (String topic : sub.getTopics()) {
                ConsumerEvent e = ConsumerEvent.newBuilder().setTopic(topic).build();
                builder.addEvents(e);
            }
            LOGGER.info("SubscribeResponseReceiver '" + sub.getUUID() + "' has unsubscribed from " + topicList);
            responseObserver.onNext(builder.build());
        }
        responseObserver.onCompleted();
    }

    public void initiateServiceShutdown() {
        LOGGER.info("Initiating " + GrpcService.class.getSimpleName() + " shutdown");
        subscribeResponsePublisher.close();
        Set<SubscribeResponseReceiver> rcvSet = Set.copyOf(receivers.values());
        for (SubscribeResponseReceiver rcv : rcvSet) {
            rcv.cancel();
        }
        Set<PublishRequestSender> sndSet = Set.copyOf(senders.values());
        for (PublishRequestSender snd : sndSet) {
            snd.cancel();
        }
        receivers.clear();
        senders.clear();
    }
}
