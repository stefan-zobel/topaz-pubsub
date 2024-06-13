package net.topaz.grpc.client;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.Empty;

import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import net.topaz.grpc.UUID128;
import net.topaz.protobuf.PubSubGrpc;
import net.topaz.protobuf.PubSubGrpc.PubSubBlockingStub;
import net.topaz.protobuf.PubSubGrpc.PubSubStub;
import net.topaz.protobuf.PublishRequest;
import net.topaz.protobuf.PublishResponse;
import net.topaz.protobuf.SubscribeRequest;
import net.topaz.protobuf.SubscribeResponse;

public class ClientConnection implements AutoCloseable {

    private final ManagedChannel channel;
    private final PubSubBlockingStub blocking;
    private final PubSubStub async;

    public ClientConnection(String host, int port) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        Channel interceptedChannel = ClientInterceptors.intercept(channel,
                new ClientUuidInterceptor(UUID128.random128BitHex()));
        // async publishStream, async subscribe
        PubSubStub asyncStub = PubSubGrpc.newStub(interceptedChannel);
        // blocking publish, blocking subscribe, blocking unsubscribe
        PubSubBlockingStub blockStub = PubSubGrpc.newBlockingStub(interceptedChannel);
        this.channel = channel;
        this.blocking = blockStub;
        this.async = asyncStub;
    }

    public PublishResponse publishBlocking(PublishRequest request) {
        return blocking.publish(request);
    }

    public Iterator<SubscribeResponse> subscribeBlocking(SubscribeRequest request) {
        return blocking.subscribe(request);
    }

    public SubscribeResponse unsubscribeBlocking(SubscribeRequest request) {
        return blocking.unsubscribe(request);
    }

    public void subscribeAsync(SubscribeRequest request, StreamObserver<SubscribeResponse> responseObserver) {
        async.subscribe(request, responseObserver);
    }

    public StreamObserver<PublishRequest> publishStreamAsync(StreamObserver<PublishResponse> responseObserver) {
        return async.publishStream(responseObserver);
    }

    public boolean isConnected() {
        try {
            blocking.ping(Empty.getDefaultInstance());
        } catch (Exception ignore) {
            return false;
        }
        return true;
    }

    public void close() {
        if (!channel.isTerminated()) {
            channel.shutdownNow();
            try {
                channel.awaitTermination(16L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
