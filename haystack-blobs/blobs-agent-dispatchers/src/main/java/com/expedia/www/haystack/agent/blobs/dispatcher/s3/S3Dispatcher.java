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
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.expedia.blobs.core.Blob;
import com.expedia.blobs.stores.aws.S3BlobStore;
import com.expedia.www.haystack.agent.blobs.dispatcher.core.BlobDispatcher;
import com.expedia.www.haystack.agent.core.config.ConfigurationHelpers;
import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class S3Dispatcher implements BlobDispatcher, AutoCloseable {
    private final static Logger LOGGER = LoggerFactory.getLogger(S3Dispatcher.class);

    private final static String BUCKET_NAME_PROPERTY = "bucketName";
    private final static String REGION_PROPERTY = "region";
    private final static String RETRY_COUNT = "retryCount";
    private final static String AWS_ACCESS_KEY = "awsAccessKey";
    private final static String AWS_SECRET_KEY = "awsSecretKey";
    private final static String MAX_CONNECTIONS = "maxConnections";
    private final static String KEEP_ALIVE = "keepAlive";
    private final static Long MULTIPART_UPLOAD_THRESHOLD = 5L * 1024 * 1024;
    private final static String DISABLE_AUTO_SHUTDOWN = "disableAutoShutdown";
    private final static String DISPATCHER_THREADPOOL_SIZE = "dispatcherThreadpoolSize";
    private final static String DISPATCHER_SHUTDOWN_WAIT_TIME = "dispatcherShutdownWaitInSeconds";

    private S3BlobStore s3BlobStore;

    @Override
    public String getName() {
        return "s3";
    }

    @Override
    public void dispatch(Blob blob) {
        s3BlobStore.storeInternal(blob);
    }

    @Override
    public void initialize(final Config config) {
        Properties properties = ConfigurationHelpers.generatePropertiesFromMap(ConfigurationHelpers.convertToPropertyMap(config));

        S3BlobStore.Builder builder = new S3BlobStore.Builder(properties.getProperty(BUCKET_NAME_PROPERTY), createTransferManager(config));

        final Boolean disableAutoShutdown = Boolean.parseBoolean(properties.getProperty(DISABLE_AUTO_SHUTDOWN, "false"));
        final String dispatcherThreadpoolSize = properties.getProperty(DISPATCHER_THREADPOOL_SIZE);
        final String dispatcherShutdownWaitInSeconds = properties.getProperty(DISPATCHER_SHUTDOWN_WAIT_TIME);

        if (disableAutoShutdown) {
            builder.disableAutoShutdown();
        }
        try {
            if (dispatcherThreadpoolSize != null) {
                builder.withThreadPoolSize(Integer.parseInt(dispatcherThreadpoolSize));
            }
            if (dispatcherShutdownWaitInSeconds != null) {
                builder.withThreadPoolSize(Integer.parseInt(dispatcherShutdownWaitInSeconds));
            }
        } catch (Exception ex) {
            LOGGER.warn("Failed to parse string config to integer. Using the default value.", ex);
        }

        s3BlobStore = builder.build();

        LOGGER.info("Successfully initialized the S3 dispatcher with config={}", config);
    }

    private static TransferManager createTransferManager(final Config config) {
        Validate.notEmpty(config.getString(REGION_PROPERTY), "s3 bucket region can't be empty");

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
                .withRegion(config.getString(REGION_PROPERTY))
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
        s3BlobStore.close();
    }

}
