/*
 * Copyright 2019 Expedia, Inc.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 *
 */
import com.expedia.blobs.grpc.agent.api.DispatchResult;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

abstract public class BaseGrpcClient<R, T> implements Client<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseGrpcClient.class);

    protected final ManagedChannel channel;
    protected final T stub;
    protected final long shutdownTimeoutMS;
    protected final StreamObserver<DispatchResult> observer;

    public BaseGrpcClient(ManagedChannel channel,
                          T stub,
                          long shutdownTimeoutMS,
                          StreamObserver<DispatchResult> observer) {
        this.channel = channel;
        this.stub = stub;
        this.shutdownTimeoutMS = shutdownTimeoutMS;
        this.observer = observer;
    }

    @Override
    public void close() throws ClientException {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(shutdownTimeoutMS, TimeUnit.SECONDS)) {
                LOGGER.warn("Channel failed to terminate, forcibly closing it.");
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOGGER.error("Unable to close the channel.", e);
        }
    }

    @Override
    public void flush() throws ClientException {

    }

    public static abstract class Builder {
        protected StreamObserver<DispatchResult> observer;

        protected String host;
        protected int port;
        protected long keepAliveTimeMS = TimeUnit.SECONDS.toMillis(30);
        protected long keepAliveTimeoutMS = TimeUnit.SECONDS.toMillis(30);
        protected boolean keepAliveWithoutCalls = true;
        protected NegotiationType negotiationType = NegotiationType.PLAINTEXT;

        protected ManagedChannel channel;

        protected long shutdownTimeoutMS = TimeUnit.SECONDS.toMillis(30);

        public Builder(ManagedChannel channel) {
            this.channel = channel;
        }

        public Builder(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public Builder withObserver(StreamObserver<DispatchResult> observer) {
            this.observer = observer;
            return this;
        }

        public Builder withKeepAliveTimeMS(long keepAliveTimeMS) {
            this.keepAliveTimeMS = keepAliveTimeMS;
            return this;
        }

        public Builder withKeepAliveTimeoutMS(long keepAliveTimeoutMS) {
            this.keepAliveTimeoutMS = keepAliveTimeoutMS;
            return this;
        }

        public Builder withKeepAliveWithoutCalls(boolean keepAliveWithoutCalls) {
            this.keepAliveWithoutCalls = keepAliveWithoutCalls;
            return this;
        }

        public Builder withNegotiationType(NegotiationType negotiationType) {
            this.negotiationType = negotiationType;
            return this;
        }

        public Builder withShutdownTimeoutMS(long shutdownTimeoutMS) {
            this.shutdownTimeoutMS = shutdownTimeoutMS;
            return this;
        }

        protected ManagedChannel buildManagedChannel() {
            return NettyChannelBuilder.forAddress(host, port)
                    .keepAliveTime(keepAliveTimeMS, TimeUnit.MILLISECONDS)
                    .keepAliveTimeout(keepAliveTimeoutMS, TimeUnit.MILLISECONDS)
                    .keepAliveWithoutCalls(keepAliveWithoutCalls)
                    .negotiationType(negotiationType)
                    .build();
        }

        public abstract BaseGrpcClient build();
    }
}
