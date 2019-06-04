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
import com.expedia.blobs.core.BlobContext;
import com.expedia.blobs.core.BlobType;
import com.expedia.blobs.grpc.Blob;
import com.expedia.blobs.grpc.agent.api.BlobAgentGrpc;
import com.expedia.blobs.grpc.agent.api.DispatchResult;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import java.util.Map;

public class BlobGrpcClient extends BaseGrpcClient<Blob, BlobAgentGrpc.BlobAgentStub> {

    public BlobGrpcClient(ManagedChannel channel,
                          BlobAgentGrpc.BlobAgentStub stub,
                          long shutdownTimeoutMS,
                          StreamObserver<DispatchResult> observer) {
        super(channel, stub, shutdownTimeoutMS, observer);
    }

    public boolean send(BlobContext blobContext, com.expedia.blobs.core.Blob blob, Map<String, String> metadata) {
        Blob grpcBlob = parseBlob(blobContext, blob, metadata);
        return send(grpcBlob);
    }

    private Blob parseBlob(BlobContext blobContext, com.expedia.blobs.core.Blob blob, Map<String, String> metadata) {
        BlobType blobType = BlobType.from(metadata.get("blob-type"));

        return Blob.newBuilder()
                .setKey(blob.getKey())
                .setServiceName(blobContext.getServiceName())
                .setOperationName(blobContext.getOperationName())
                .setContentType(metadata.get("content-type"))
                .setBlobType(
                        blobType.getType() == BlobType.REQUEST.getType() ? Blob.BlobType.REQUEST : Blob.BlobType.RESPONSE
                )
                .setContent(ByteString.copyFrom(blob.getData()))
                .setTimestamp(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC))
                .putAllMetadata(metadata)
                .build();
    }

    /**
     * @param blob Blob to send off to the endpoint with metadata
     * @return Returns <code>true</code> if the operation was successful,
     * <code>false</code> if it was unsuccessful
     * @throws ClientException throws a <code>ClientException</code> if an exception occured
     */
    @Override
    public boolean send(Blob blob) throws ClientException {
        try {
            stub.dispatch(blob, observer);
        } catch (Exception e) {
            throw new ClientException(e.getMessage(), e);
        }

        //always true
        return true;
    }

    /**
     * Extension of {@link BaseGrpcClient.Builder}
     */

    public static final class Builder extends BaseGrpcClient.Builder {

        public Builder(ManagedChannel channel) {
            super(channel);
        }

        public Builder(String host, int port) {
            super(host, port);
        }

        public BlobGrpcClient build() {
            ManagedChannel managedChannel = this.channel;

            if (managedChannel == null) {
                managedChannel = buildManagedChannel();
            }

            BlobAgentGrpc.BlobAgentStub stub = BlobAgentGrpc.newStub(managedChannel);

            return new BlobGrpcClient(managedChannel, stub, shutdownTimeoutMS, observer);
        }
    }
}