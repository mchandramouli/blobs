/*
 *  Copyright 2019 Expedia, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.expedia.www.haystack.agent.blobs.server.api;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.expedia.blobs.core.ContentType;
import com.expedia.www.blobs.model.Blob;
import com.expedia.www.haystack.agent.blobs.api.*;
import com.expedia.www.haystack.agent.blobs.dispatcher.core.BlobDispatcher;
import com.expedia.www.haystack.agent.blobs.dispatcher.core.RateLimitException;
import com.expedia.www.haystack.agent.core.metrics.SharedMetricRegistry;
import com.google.protobuf.ByteString;
import com.sun.xml.fastinfoset.stax.StAXDocumentParser;
import io.grpc.stub.StreamObserver;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.*;

public class BlobAgentGrpcServer extends BlobAgentGrpc.BlobAgentImplBase {
    private final Logger LOGGER = LoggerFactory.getLogger(BlobAgentGrpcServer.class);

    private final List<BlobDispatcher> dispatchers;
    private final int maxBlobSizeInBytes;
    private final Timer dispatchTimer;
    private final Meter dispatchFailureMeter;

    private final static String CONTENT_TYPE = "content-type";

    public BlobAgentGrpcServer(final List<BlobDispatcher> dispatchers, final int maxBlobSizeInBytes) {
        Validate.notEmpty(dispatchers, "Dispatchers can't be empty");
        this.maxBlobSizeInBytes = maxBlobSizeInBytes;
        this.dispatchers = dispatchers;
        this.dispatchTimer = SharedMetricRegistry.newTimer("blob.agent.dispatch.timer");
        this.dispatchFailureMeter = SharedMetricRegistry.newMeter("blob.agent.dispatch.failures");

    }

    public void dispatch(final Blob blob,
                         final StreamObserver<DispatchResult> responseObserver) {

        final DispatchResult.Builder result = DispatchResult.newBuilder().setCode(DispatchResult.ResultCode.SUCCESS);
        final Timer.Context timer = dispatchTimer.time();
        final StringBuilder failedDispatchers = new StringBuilder();

        final long blobPayloadSize = blob.getContent().size();

        if (blobPayloadSize > maxBlobSizeInBytes) {
            result.setCode(DispatchResult.ResultCode.MAX_SIZE_EXCEEDED_ERROR);
            result.setErrorMessage(
                    String.format("Fail to dispatch as the blob size=%d exceeds the limit of %d bytes",
                            blob.getContent().size(),
                            maxBlobSizeInBytes));
        } else {

            for (final BlobDispatcher d : dispatchers) {
                try {
                    d.dispatch(blob);
                } catch (final RateLimitException r) {
                    result.setCode(DispatchResult.ResultCode.RATE_LIMIT_ERROR);
                    dispatchFailureMeter.mark();
                    LOGGER.error("Fail to dispatch the blobs due to rate limit errors", r);
                    failedDispatchers.append(d.getName()).append(',');
                } catch (final Exception ex) {
                    result.setCode(DispatchResult.ResultCode.UNKNOWN_ERROR);
                    dispatchFailureMeter.mark();
                    LOGGER.error("Fail to dispatch the blob to the dispatcher with name={}", d.getName(), ex);
                    failedDispatchers.append(d.getName()).append(',');
                }
            }
        }

        if (failedDispatchers.length() > 0) {
            result.setErrorMessage("Fail to dispatch the blob to the dispatchers=" +
                    StringUtils.removeEnd(failedDispatchers.toString(), ","));
        }

        timer.close();
        responseObserver.onNext(result.build());
        responseObserver.onCompleted();
    }

    public void read(final BlobSearch blobSearch, final StreamObserver<BlobReadResponse> responseObserver) {
        final String blobKey = blobSearch.getKey();

        BlobReadResponse.Builder result = BlobReadResponse.newBuilder();


        Optional<Blob> currentBlob = dispatchers.stream().map(blobDispatcher -> blobDispatcher.read(blobKey)).filter(Optional::isPresent)
                .map(Optional::get).findFirst();

        if (currentBlob.isPresent()) {
            result.setBlob(currentBlob.get()).setCode(BlobReadResponse.ResultCode.SUCCESS);
        } else {
            result.setErrorMessage(String.format("Failed to read blob with key %s from ", blobKey) +
                    Arrays.toString(dispatchers.stream().map(d -> d.getName()).toArray()));
            result.setCode(BlobReadResponse.ResultCode.UNKNOWN_ERROR);
        }

        responseObserver.onNext(result.build());
        responseObserver.onCompleted();
    }

    public void readBlobAsString(final BlobSearch blobSearch, final StreamObserver<FormattedBlobReadResponse> responseObserver) {
        final String blobKey = blobSearch.getKey();

        FormattedBlobReadResponse.Builder result = FormattedBlobReadResponse.newBuilder();


        Optional<Blob> currentBlob = dispatchers.stream().map(blobDispatcher -> blobDispatcher.read(blobKey)).filter(Optional::isPresent)
                .map(Optional::get).findFirst();

        if (currentBlob.isPresent()) {
            final Map<String, String> metadata = currentBlob.get().getMetadataMap();
            final String contentType = metadata.get(CONTENT_TYPE);

            String blobString = parseBlob(currentBlob.get().getContent(), contentType);

            result.setData(blobString);
        } else {
            result.setData("");
        }

        responseObserver.onNext(result.build());
        responseObserver.onCompleted();
    }

    String parseBlob(ByteString content, String contentType) {
        String blob = null;
        try {
            if (Objects.equals(contentType, ContentType.FAST_INFOSET.getType())) {
                blob = parseFastInfosetToString(content);
            } else {
                blob = IOUtils.toString(content.toByteArray());
            }
        } catch (Exception ex) {
            LOGGER.error("Error parsing blob data to string");
        }
        return blob;
    }

    private String parseFastInfosetToString(ByteString content) throws TransformerException {
        final InputStream fiDocument = new ByteArrayInputStream(content.toByteArray());

        final XMLStreamReader streamReader = new StAXDocumentParser(fiDocument);

        final Transformer transformer = TransformerFactory.newInstance().newTransformer();
        final StringWriter stringWriter = new StringWriter();
        transformer.transform(new StAXSource(streamReader), new StreamResult(stringWriter));

        return stringWriter.toString();
    }
}
