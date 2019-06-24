package com.expedia.www.haystack.agent.blobs.client;

import com.expedia.blobs.core.BlobWriterImpl;
import com.expedia.blobs.core.ContentType;
import com.expedia.www.haystack.agent.blobs.grpc.Blob;
import com.expedia.www.haystack.agent.blobs.grpc.api.BlobAgentGrpc;
import com.expedia.www.haystack.agent.blobs.grpc.api.DispatchResult;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import org.junit.*;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

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

        client = new AgentClient.Builder(grpcServerRule.getChannel()).build();

        Map<String, String> metadata = new HashMap<>();
        metadata.put("content-type", "application/json");
        metadata.put("blob-type", "request");
        metadata.put("a", "b");

        blob = Blob.newBuilder()
                .setKey("key1")
                .setContent(ByteString.copyFrom(new String("{'key':'value'}").getBytes()))
                .putAllMetadata(metadata)
                .setBlobType(Blob.BlobType.REQUEST)
                .setContentType(ContentType.JSON.getType())
                .setServiceName("service")
                .setOperationName("Operation")
                .setOperationID("abcd")
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

}