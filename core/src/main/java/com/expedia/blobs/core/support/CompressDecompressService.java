package com.expedia.blobs.core.support;

import com.expedia.blobs.core.io.BlobInputStream;
import org.apache.commons.io.IOUtils;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CompressDecompressService {

    private final CompressionType compressionType;

    public CompressDecompressService(CompressionType compressionType) {
        this.compressionType = compressionType;
    }

    public enum CompressionType {
        GZIP, SNAPPY, NONE
    }

    private interface CompressOutputStream {
        OutputStream apply(final OutputStream inp) throws IOException;
    }

    public BlobInputStream compressData(final byte[] dataBytes) {
        try {
            switch (compressionType) {
                case GZIP:
                    return createCompressBlobStream(dataBytes, GZIPOutputStream::new);
                case SNAPPY:
                    return createCompressBlobStream(dataBytes, SnappyOutputStream::new);
                case NONE:
                default:
                    return new BlobInputStream(new ByteArrayInputStream(dataBytes), dataBytes.length);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private BlobInputStream createCompressBlobStream(final byte[] dataBytes,
                                                     final CompressOutputStream compressor) throws IOException {
        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        final OutputStream compressedStream = compressor.apply(outStream);
        IOUtils.copy(new ByteArrayInputStream(dataBytes), compressedStream);
        compressedStream.close();
        final byte[] compressedBytes = outStream.toByteArray();
        return new BlobInputStream(
                new ByteArrayInputStream(compressedBytes), compressedBytes.length);
    }

    public static InputStream uncompressData(final CompressionType compressionType, final InputStream stream) {
        try {
            switch (compressionType) {
                case GZIP:
                    return new GZIPInputStream(stream);
                case SNAPPY:
                    return new SnappyInputStream(stream);
                case NONE:
                default:
                    return stream;
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public String getCompressionType() {
        return compressionType.toString();
    }
}
