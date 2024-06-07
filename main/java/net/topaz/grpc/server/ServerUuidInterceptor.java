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

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

public class ServerUuidInterceptor implements ServerInterceptor {

    private static final String KEY_NAME = "uuid";
    private static final Metadata.Key<String> UUID_METADATA = Metadata.Key.of(KEY_NAME,
            Metadata.ASCII_STRING_MARSHALLER);
    @SuppressWarnings("rawtypes")
    private static final ServerCall.Listener<?> NOOP_LISTENER = new ServerCall.Listener() {
    };

    public static final Context.Key<String> UUID_CONTEXT = Context.key(KEY_NAME);

    public ServerUuidInterceptor() {
    }

    @Override
    public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {
        String uuid = headers.get(UUID_METADATA);
        if (uuid != null) {
            Context context = Context.current().withValue(UUID_CONTEXT, uuid);
            return Contexts.interceptCall(context, call, headers, next);
        } else {
            // client has not identified herself
            call.close(Status.UNAUTHENTICATED.withDescription("no uuid provided"), new Metadata());
            @SuppressWarnings("unchecked")
            ServerCall.Listener<ReqT> listener = (ServerCall.Listener<ReqT>) NOOP_LISTENER;
            return listener;
        }
    }
}
