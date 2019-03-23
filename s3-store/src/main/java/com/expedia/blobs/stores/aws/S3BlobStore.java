package com.expedia.blobs.stores.aws;

import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.expedia.blobs.core.Blob;
import com.expedia.blobs.core.io.AsyncStore;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3BlobStore extends AsyncStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3BlobStore.class);
    private final String bucketName;
    private final TransferManager transferManager;

    public S3BlobStore(String bucketName, TransferManager transferManager) {
        this(bucketName, transferManager, Runtime.getRuntime().availableProcessors());
    }

    public S3BlobStore(String bucketName,
                       TransferManager transferManager,
                       int threadPoolSize) {
        super(threadPoolSize);

        this.transferManager = transferManager;

        Validate.notEmpty(bucketName);
        this.bucketName = bucketName;
    }

    @Override
    protected void storeInternal(Blob blob) {
        try {
            final ObjectMetadata metadata = new ObjectMetadata();
            blob.getMetadata().forEach(metadata::addUserMetadata);

            final InputStream stream = new ByteArrayInputStream(blob.getData());
            metadata.setContentLength(blob.getSize());

            final PutObjectRequest putRequest =
                    new PutObjectRequest(bucketName, blob.getKey(), stream, metadata)
                            .withCannedAcl(CannedAccessControlList.BucketOwnerFullControl)
                            .withGeneralProgressListener(new UploadProgressListener(LOGGER, blob.getKey()));

            final Upload upload = transferManager.upload(putRequest);
            //upload.waitForUploadResult();
        } catch (Exception e) {
            final String message = String.format("Unable to upload blob to S3 for  key %s : %s",
                                                 blob.getKey(),
                                                 e.getMessage());
            LOGGER.error(message, e);
        }
    }

    @Override
    protected Blob readInternal(String fileKey) {
        return null;
    }
}
