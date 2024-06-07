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

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import io.grpc.stub.StreamObserver;
import net.topaz.protobuf.ConsumerEvent;
import net.topaz.protobuf.SubscribeResponse;

public class SubscribeResponseReceiver implements Subscriber<SubscribeResponse> {

    private final AtomicBoolean isCancelled = new AtomicBoolean();
    private final AtomicBoolean alreaydCompleted = new AtomicBoolean();
    private final Consumer<SubscribeResponseReceiver> deregisterCallback;
    private final String uuid;
    private Set<String> topicSet;
    private Subscription subscription;
    private StreamObserver<SubscribeResponse> observer;

    public SubscribeResponseReceiver(StreamObserver<SubscribeResponse> observer,
            Consumer<SubscribeResponseReceiver> deregisterCallback, List<String> topicList, String uuid) {
        this.observer = Objects.requireNonNull(observer);
        this.deregisterCallback = Objects.requireNonNull(deregisterCallback);
        this.uuid = Objects.requireNonNull(uuid);
        KeySetView<String, Boolean> set = ConcurrentHashMap.newKeySet(Objects.requireNonNull(topicList).size());
        set.addAll(topicList);
        this.topicSet = set;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = Objects.requireNonNull(subscription);
        requestOne();
    }

    @Override
    public void onNext(SubscribeResponse item) {
        if (isCancelled.get()) {
            return;
        }
        if (item.getEventsList().stream().allMatch(e -> topicSet != null && topicSet.contains(e.getTopic()))) {
            checkedOnNext(item);
        } else {
            SubscribeResponse.Builder builder = SubscribeResponse.newBuilder();
            List<ConsumerEvent> events = item.getEventsList();
            for (ConsumerEvent e : events) {
                String topic = e.getTopic();
                if (topicSet != null && topicSet.contains(topic)) {
                    builder.addEvents(e);
                }
            }
            if (builder.getEventsCount() > 0) {
                SubscribeResponse response = builder.build();
                checkedOnNext(response);
            }
        }
        requestOne();
    }

    @Override
    public void onError(Throwable t) {
        if (!isCancelled.get() && !alreaydCompleted.get()) {
            observer.onError(t);
        }
    }

    @Override
    public void onComplete() {
        if (!isCancelled.get()) {
            try {
                if (alreaydCompleted.compareAndSet(false, true)) {
                    observer.onCompleted();
                }
            } finally {
                cancel();
            }
        }
    }

    public String getUUID() {
        return uuid;
    }

    public boolean hasTopics() {
        if (!isCancelled.get()) {
            return !topicSet.isEmpty();
        }
        return false;
    }

    public void removeTopics(List<String> topics) {
        if (!isCancelled.get()) {
            topicSet.removeAll(topics);
        }
    }

    public Set<String> getTopics() {
        if (!isCancelled.get()) {
            return Set.copyOf(topicSet);
        }
        return Set.of();
    }

    public void cancel() {
        if (isCancelled.compareAndSet(false, true)) {
            subscription.cancel();
            subscription = null;
            if (alreaydCompleted.compareAndSet(false, true)) {
                observer.onCompleted();
            }
            observer = null;
            topicSet = null;
            deregisterCallback.accept(this);
        }
    }

    private void checkedOnNext(SubscribeResponse item) {
        if (!isCancelled.get() && !alreaydCompleted.get()) {
            observer.onNext(item);
        }
    }

    private void requestOne() {
        if (!isCancelled.get()) {
            subscription.request(1L);
        }
    }
}
