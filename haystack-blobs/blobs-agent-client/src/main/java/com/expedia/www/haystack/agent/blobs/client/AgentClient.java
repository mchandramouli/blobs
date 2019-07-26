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
package com.expedia.www.haystack.agent.blobs.client;

import com.expedia.blobs.core.BlobReadWriteException;
import com.expedia.blobs.core.BlobWriterImpl;
import com.expedia.blobs.core.io.AsyncSupport;
import com.expedia.www.blobs.model.Blob;
import com.expedia.www.haystack.agent.blobs.api.BlobAgentGrpc;
import com.expedia.www.haystack.agent.blobs.api.DispatchResult;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class AgentClient extends AsyncSupport {
    private static final Logger LOGGER = LoggerFactory.getLogger(AgentClient.class);

    private BlobAgentGrpc.BlobAgentStub stub;
    private ManagedChannel channel;
    private final long channelShutdownTimeoutMS;
    private final StreamObserver<DispatchResult> observer;
    private Thread shutdownHook = new Thread(() -> this.close());

    @VisibleForTesting
    Boolean shutdownHookAdded = false;

    AgentClient(int threadPoolSize,
                int threadPoolShutdownWaitInSec,
                BlobAgentGrpc.BlobAgentStub stub,
                ManagedChannel channel,
                long channelShutdownTimeoutMS,
                StreamObserver<DispatchResult> observer,
                boolean closeOnShutdown) {
        super(threadPoolSize, threadPoolShutdownWaitInSec);
        this.stub = stub;
        this.channel = channel;
        this.channelShutdownTimeoutMS = channelShutdownTimeoutMS;
        this.observer = observer;

        if (closeOnShutdown) {
            this.shutdownHookAdded = closeOnShutdown;
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        } else {
            LOGGER.info("No shutdown hook registered: Please call close() manually on application shutdown.");
        }
    }

    @Override
    public void storeInternal(BlobWriterImpl.BlobBuilder blobBuilder) {
        Blob blob = blobBuilder.build();
        try {
            stub.dispatch(blob, observer);
        } catch (Exception e) {
            final String message = String.format("Unable to send blob to haystack-agent for  key %s : %s",
                    blob.getKey(),
                    e.getMessage());
            throw new BlobReadWriteException(message, e);
        }
    }

    @Override
    public Optional<Blob> readInternal(String key) {
        throw new NotImplementedException("This function is not implemented currently");
    }

    @Override
    public void close() {
        super.close();
        channel.shutdown();
        try {
            if (!channel.awaitTermination(channelShutdownTimeoutMS, TimeUnit.SECONDS)) {
                LOGGER.warn("Channel failed to terminate, forcibly closing it.");
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOGGER.error("Unable to close the channel.", e);
        }
    }

    public static class GRPCAgentClientStreamObserver implements StreamObserver<DispatchResult> {

        private Logger LOGGER;

        public GRPCAgentClientStreamObserver(Logger logger) {
            this.LOGGER = logger;
        }

        @Override
        public void onCompleted() {
            LOGGER.debug("Dispatching blob completed");
        }

        @Override
        public void onError(Throwable t) {
            LOGGER.error("Dispatching blob failed with error: " + t, t);
        }

        @Override
        public void onNext(DispatchResult value) {
            switch (value.getCode()) {
                case SUCCESS:
                    // do nothing
                    break;
                case RATE_LIMIT_ERROR:
                    LOGGER.error("Rate limit error received from agent");
                    break;
                case UNKNOWN_ERROR:
                    LOGGER.error("Unknown error received from agent");
                    break;
                case MAX_SIZE_EXCEEDED_ERROR:
                    LOGGER.error("Size of the blog is greater than the set maximum size");
                    break;
                default:
                    LOGGER.error("Unknown result received from agent: {}", value.getCode());
            }
        }
    }

    public static class Builder {
        private ManagedChannel channel;
        private String host;
        private int port;
        private StreamObserver<DispatchResult> observer;

        private long channelShutdownTimeoutMS = TimeUnit.SECONDS.toMillis(30);
        private long channelKeepAliveTimeMS = TimeUnit.SECONDS.toMillis(30);
        private long channelKeepAliveTimeoutMS = TimeUnit.SECONDS.toMillis(30);
        private boolean channelKeepAliveWithoutCalls = true;
        private NegotiationType channelNegotiationType = NegotiationType.PLAINTEXT;

        private int threadPoolSize = Runtime.getRuntime().availableProcessors();
        private int threadPoolShutdownWaitInSec = 60;
        private boolean closeOnShutdown = true;

        private Builder() {
            this.observer = new GRPCAgentClientStreamObserver(LOGGER);
        }

        public Builder(ManagedChannel managedChannel) {
            this();
            this.channel = managedChannel;
        }

        public Builder(String host, int port) {
            this();
            this.host = host;
            this.port = port;
        }

        public Builder withObserver(StreamObserver<DispatchResult> observer) {
            this.observer = observer;
            return this;
        }

        public Builder withChannelShutdownTimeoutMS(long channelShutdownTimeoutMS) {
            this.channelShutdownTimeoutMS = channelShutdownTimeoutMS;
            return this;
        }

        public Builder withChannelKeepAliveTimeMS(long channelKeepAliveTimeMS) {
            this.channelKeepAliveTimeMS = channelKeepAliveTimeMS;
            return this;
        }

        public Builder withChannelKeepAliveTimeoutMS(long channelKeepAliveTimeoutMS) {
            this.channelKeepAliveTimeoutMS = channelKeepAliveTimeoutMS;
            return this;
        }

        public Builder disableChannelKeepAliveWithoutCalls() {
            this.channelKeepAliveWithoutCalls = false;
            return this;
        }

        public Builder withThreadPoolSize(int threadPoolSize) {
            this.threadPoolSize = threadPoolSize;
            return this;
        }

        public Builder withThreadPoolShutdownWaitInSec(int threadPoolShutdownWaitInSec) {
            this.threadPoolShutdownWaitInSec = threadPoolShutdownWaitInSec;
            return this;
        }

        public Builder disableAutoShutdown() {
            this.closeOnShutdown = false;
            return this;
        }

        public AgentClient build() {
            if (this.channel == null) {
                this.channel = buildManagedChannel();
            }

            BlobAgentGrpc.BlobAgentStub stub = BlobAgentGrpc.newStub(this.channel);

            return new AgentClient(threadPoolSize, threadPoolShutdownWaitInSec, stub, channel, channelShutdownTimeoutMS, observer, closeOnShutdown);
        }

        private ManagedChannel buildManagedChannel() {
            return NettyChannelBuilder.forAddress(host, port)
                    .keepAliveTime(channelKeepAliveTimeMS, TimeUnit.MILLISECONDS)
                    .keepAliveTimeout(channelKeepAliveTimeoutMS, TimeUnit.MILLISECONDS)
                    .keepAliveWithoutCalls(channelKeepAliveWithoutCalls)
                    .negotiationType(channelNegotiationType)
                    .build();
        }
    }
}
