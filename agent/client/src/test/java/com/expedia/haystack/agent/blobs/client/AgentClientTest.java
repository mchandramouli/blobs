package com.expedia.haystack.agent.blobs.client;

import com.expedia.blobs.core.BlobWriterImpl;
import com.expedia.blobs.model.Blob;
import com.expedia.haystack.agent.blobs.api.BlobAgentGrpc;
import com.expedia.haystack.agent.blobs.api.DispatchResult;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AgentClientTest {
    @Rule
    public final GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor();

    private final BlobAgentGrpc.BlobAgentImplBase serviceImpl = spy(new BlobAgentGrpc.BlobAgentImplBase() {
        @Override
        public void dispatch(Blob request, StreamObserver<DispatchResult> responseObserver) {
            final DispatchResult.Builder result = DispatchResult.newBuilder().setCode(DispatchResult.ResultCode.SUCCESS);
            responseObserver.onNext(result.build());
            responseObserver.onCompleted();
        }
    });

    private AgentClient client;

    private Blob blob;

    @Before
    public void setup() {
        grpcServerRule.getServiceRegistry().addService(serviceImpl);

        client = new AgentClient.Builder(grpcServerRule.getChannel())
                .withThreadPoolSize(8)
                .withThreadPoolShutdownWaitInSec(60)
                .withChannelKeepAliveTimeMS(30)
                .withChannelKeepAliveTimeoutMS(30)
                .withChannelShutdownTimeoutMS(30).disableChannelKeepAliveWithoutCalls()

                .build();

        Map<String, String> metadata = new HashMap<>();
        metadata.put("content-type", "application/json");
        metadata.put("blob-type", "request");
        metadata.put("a", "b");

        blob = Blob.newBuilder()
                .setKey("key1")
                .setContent(ByteString.copyFrom(new String("{'key':'value'}").getBytes()))
                .putAllMetadata(metadata)
                .build();
    }

    @After
    public void teardown() {
        client.close();
    }

    @Test
    public void testDispatcher() {
        BlobWriterImpl.BlobBuilder blobBuilder = Mockito.mock(BlobWriterImpl.BlobBuilder.class);
        Mockito.when(blobBuilder.build()).thenReturn(blob);

        final ArgumentCaptor<Blob> blobCaptor = ArgumentCaptor.forClass(Blob.class);
        final ArgumentCaptor<StreamObserver<DispatchResult>> streamObserverCaptor = ArgumentCaptor.forClass(StreamObserver.class);
        client.storeInternal(blobBuilder);

        verify(serviceImpl, times(1)).dispatch(blobCaptor.capture(), streamObserverCaptor.capture());
    }

    @Test
    public void autoShutdownHookAdded() {
        AgentClient client = new AgentClient.Builder(Mockito.mock(ManagedChannel.class)).build();
        Assert.assertEquals(true, client.shutdownHookAdded);
    }

    @Test
    public void autoShutdownHookDisabled() {
        AgentClient client = new AgentClient.Builder(Mockito.mock(ManagedChannel.class)).disableAutoShutdown().build();
        Assert.assertEquals(false, client.shutdownHookAdded);
    }

    @Test
    public void observerShouldLogForRateLimitError() {

        Logger logger = Mockito.mock(Logger.class);

        AgentClient.GRPCAgentClientStreamObserver streamObserver = new AgentClient.GRPCAgentClientStreamObserver(
                logger
        );
        streamObserver.onNext(DispatchResult.newBuilder().setCodeValue(2).build());

        verify(logger, times(1)).error("Rate limit error received from agent");
    }

    @Test
    public void observerShouldLogForMaxSizeExceededError() {

        Logger logger = Mockito.mock(Logger.class);

        AgentClient.GRPCAgentClientStreamObserver streamObserver = new AgentClient.GRPCAgentClientStreamObserver(
                logger
        );
        streamObserver.onNext(DispatchResult.newBuilder().setCodeValue(3).build());

        verify(logger, times(1)).error("Size of the blog is greater than the set maximum size");
    }

    @Test
    public void observerShouldLogForUnknownErrors() {

        Logger logger = Mockito.mock(Logger.class);

        AgentClient.GRPCAgentClientStreamObserver streamObserver = new AgentClient.GRPCAgentClientStreamObserver(
                logger
        );
        streamObserver.onNext(DispatchResult.newBuilder().setCodeValue(1).build());

        verify(logger, times(1)).error("Unknown error received from agent");
    }

}
