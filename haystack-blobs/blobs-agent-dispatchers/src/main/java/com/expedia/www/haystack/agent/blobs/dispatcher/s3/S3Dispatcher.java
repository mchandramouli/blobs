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
package com.expedia.www.haystack.agent.blobs.dispatcher.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;

import com.amazonaws.services.s3.transfer.Upload;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.expedia.blobs.core.*;
import com.expedia.blobs.core.io.BlobInputStream;
import com.expedia.blobs.core.support.CompressDecompressService;
import com.expedia.www.haystack.agent.blobs.dispatcher.core.BlobDispatcher;
import com.expedia.www.haystack.agent.blobs.dispatcher.core.RateLimitException;
import com.expedia.www.haystack.agent.blobs.grpc.Blob;

import com.expedia.www.haystack.agent.core.metrics.SharedMetricRegistry;
import com.google.common.annotations.VisibleForTesting;

import com.google.protobuf.ByteString;
import com.typesafe.config.Config;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

public class S3Dispatcher implements BlobDispatcher, AutoCloseable {
    private final static Logger LOGGER = LoggerFactory.getLogger(S3Dispatcher.class);

    private final static String BUCKET_NAME_PROPERTY = "bucketName";
    private final static String REGION_PROPERTY = "region";
    private final static String RETRY_COUNT = "retryCount";
    private final static String AWS_ACCESS_KEY = "awsAccessKey";
    private final static String AWS_SECRET_KEY = "awsSecretKey";
    private final static String MAX_CONNECTIONS = "maxConnections";
    private final static String KEEP_ALIVE = "keepAlive";

    private final static String SHOULD_WAIT_FOR_UPLOAD = "shouldWaitForUpload";

    private final static Long MULTIPART_UPLOAD_THRESHOLD = 5L * 1024 * 1024;

    private final static String MAX_OUTSTANDING_REQUESTS = "maxOutstandingRequests";

    private final static String COMPRESSION_TYPE = "compressionType";

    private TransferManager transferManager;
    private String bucketName;
    private Boolean shouldWaitForUpload;
    private int maxOutstandingRequests;
    private Semaphore parallelUploadSemaphore;
    private Executor executor;
    private CompressDecompressService compressDecompressService;
    private Meter dispatchFailureMeter;
    private Timer dispatchTimer;

    @VisibleForTesting
    S3Dispatcher(TransferManager transferManager, String bucketName, Boolean shouldWaitForUpload, int maxOutstandingRequests, CompressDecompressService compressDecompressService) {
        this.transferManager = transferManager;
        this.bucketName = bucketName;
        this.shouldWaitForUpload = shouldWaitForUpload;
        this.maxOutstandingRequests = maxOutstandingRequests;
        this.parallelUploadSemaphore = new Semaphore(maxOutstandingRequests);
        this.executor = directExecutor();
        this.dispatchFailureMeter = SharedMetricRegistry.newMeter("s3.dispatch.failure");
        this.dispatchTimer = SharedMetricRegistry.newTimer("s3.dispatch.timer");
        this.compressDecompressService = compressDecompressService;
    }

    @VisibleForTesting
    public S3Dispatcher() {
    }

    @Override
    public String getName() {
        return "s3";
    }

    @Override
    public void dispatch(Blob blob) {
        if (parallelUploadSemaphore.tryAcquire()) {
            CompletableFuture.runAsync(() -> dispatchInternal(blob), executor)
                    .whenComplete((v, t) -> {
                        if (t != null) {
                            LOGGER.error(this.getClass().getSimpleName() + " failed to store blob ", t);
                        }
                        parallelUploadSemaphore.release();
                    });
        } else {
            throw new RateLimitException("RateLimit is hit with outstanding(pending) requests=" + maxOutstandingRequests);
        }
    }

    void dispatchInternal(Blob blob) {

        try {
            final ObjectMetadata metadata = new ObjectMetadata();
            blob.getMetadataMap().forEach(metadata::addUserMetadata);

            final BlobInputStream blobInputStream = compressDecompressService.compressData(blob.getContent().toByteArray());

            metadata.setContentLength(blobInputStream.getLength());
            metadata.addUserMetadata(COMPRESSION_TYPE, compressDecompressService.getCompressionType());

            final PutObjectRequest putRequest =
                    new PutObjectRequest(bucketName, blob.getKey(), blobInputStream.getStream(), metadata)
                            .withCannedAcl(CannedAccessControlList.BucketOwnerFullControl)
                            .withGeneralProgressListener(new UploadProgressListener(LOGGER, blob.getKey(), dispatchFailureMeter, dispatchTimer.time()));

            final Upload upload = transferManager.upload(putRequest);

            if (shouldWaitForUpload) {
                upload.waitForUploadResult();
            }
        } catch (Exception e) {
            final String message = String.format("Unable to upload blob to S3 for  key %s : %s",
                    blob.getKey(),
                    e.getMessage());
            throw new BlobReadWriteException(message, e);
        }
    }

    @Override
    public Optional<Blob> read(String key) {
        try {
            final S3Object s3Object = transferManager.getAmazonS3Client().getObject(bucketName, key);
            final Map<String, String> objectMetadata = s3Object.getObjectMetadata().getUserMetadata();

            Map<String, String> metadata = objectMetadata == null ? Collections.emptyMap() : objectMetadata;

            final String compressionType = getCompressionType(metadata);
            //TODO: Rethink about blobType and contentType. Should we keep them just in metadata?
            final BlobType blobType = BlobType.from(metadata.get("blob-type"));

            final InputStream is = s3Object.getObjectContent();

            try (final InputStream uncompressedStream = CompressDecompressService.uncompressData(compressionType, is)) {
                Blob blob = Blob.newBuilder()
                        .setKey(key)
                        .putAllMetadata(metadata)
                        .setContent(ByteString.copyFrom(readInputStream(uncompressedStream)))
                        .setBlobType(blobType.getType() == BlobType.REQUEST.getType() ? com.expedia.www.haystack.agent.blobs.grpc.Blob.BlobType.REQUEST : com.expedia.www.haystack.agent.blobs.grpc.Blob.BlobType.RESPONSE)
                        .setContentType(metadata.get("content-type"))
                        .build();

                return Optional.of(blob);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to read the blob from name={}", getName(), e);
            return Optional.empty();
        }
    }

    String getCompressionType(Map<String, String> metadata) {
        final String compressionType = metadata.get(COMPRESSION_TYPE);
        return StringUtils.isEmpty(compressionType) ? "none" : compressionType;
    }

    protected byte[] readInputStream(InputStream is) throws IOException {
        return IOUtils.toByteArray(is);
    }

    @Override
    public void initialize(final Config s3Config) {
        Validate.isTrue(s3Config.hasPath(BUCKET_NAME_PROPERTY), "s3 bucket name should be present");
        bucketName = s3Config.getString(BUCKET_NAME_PROPERTY);
        Validate.notEmpty(bucketName, "s3 bucket name can't be empty");

        Validate.isTrue(s3Config.hasPath(MAX_OUTSTANDING_REQUESTS), "number of max parallel uploads should be present");
        maxOutstandingRequests = s3Config.getInt(MAX_OUTSTANDING_REQUESTS);
        Validate.isTrue(maxOutstandingRequests > 0, "max parallel uploads has to be greater than 0");

        shouldWaitForUpload = s3Config.hasPath(SHOULD_WAIT_FOR_UPLOAD) && s3Config.getBoolean(SHOULD_WAIT_FOR_UPLOAD);
        this.parallelUploadSemaphore = new Semaphore(maxOutstandingRequests);

        transferManager = createTransferManager(s3Config);

        executor = directExecutor();

        compressDecompressService = new CompressDecompressService(s3Config);

        dispatchFailureMeter = SharedMetricRegistry.newMeter("s3.dispatch.failure");
        dispatchTimer = SharedMetricRegistry.newTimer("s3.dispatch.timer");

        LOGGER.info("Successfully initialized the S3 dispatcher with config={}", s3Config);
    }

    private static TransferManager createTransferManager(final Config config) {
        Validate.isTrue(config.hasPath(REGION_PROPERTY), "s3 bucket region can't be empty");
        final String region = config.getString(REGION_PROPERTY);

        final int maxConnections = config.hasPath(MAX_CONNECTIONS) ? config.getInt(MAX_CONNECTIONS) : 50;
        final boolean keepAlive = config.hasPath(KEEP_ALIVE) && config.getBoolean(KEEP_ALIVE);
        final int retryCount = config.hasPath(RETRY_COUNT) ? config.getInt(RETRY_COUNT) : -1;

        final ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withMaxConnections(maxConnections)
                .withTcpKeepAlive(keepAlive);

        if (retryCount > 0) {
            clientConfiguration.setMaxErrorRetry(retryCount);
        }

        final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withRegion(region)
                .withCredentials(buildCredentialProvider(config))
                .withClientConfiguration(clientConfiguration)
                .build();
        return TransferManagerBuilder.standard().withS3Client(s3)
                .withMultipartUploadThreshold(MULTIPART_UPLOAD_THRESHOLD).build();
    }

    @VisibleForTesting
    static AWSCredentialsProvider buildCredentialProvider(final Config config) {
        if (config.hasPath(AWS_ACCESS_KEY) && config.hasPath(AWS_SECRET_KEY)) {
            LOGGER.info("using static aws credential provider with access and secret key for s3 dispatcher");
            return new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(config.getString(AWS_ACCESS_KEY), config.getString(AWS_SECRET_KEY)));
        } else {
            LOGGER.info("using default credential provider chain for s3 dispatcher");
            return DefaultAWSCredentialsProviderChain.getInstance();
        }
    }

    @Override
    public void close() {
        if (transferManager != null) {
            LOGGER.info("shutting down the s3 dispatcher now..");
            transferManager.shutdownNow();
        }
    }

}
