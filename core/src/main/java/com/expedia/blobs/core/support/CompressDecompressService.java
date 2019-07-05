package com.expedia.blobs.core.support;

import com.expedia.blobs.core.io.BlobInputStream;
import com.typesafe.config.Config;
import org.apache.commons.io.IOUtils;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;
import java.util.Locale;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CompressDecompressService {

    private final static String COMPRESSION_TYPE = "compressionType";
    private final CompressionType compressionType;

    public CompressDecompressService(Config config) {
        this.compressionType = findCompressionType(config);
    }

    enum CompressionType {
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

    public static InputStream uncompressData(final String compressionType, final InputStream stream) {
        try {
            switch (compressionType.toLowerCase(Locale.US)) {
                case "gzip":
                    return new GZIPInputStream(stream);
                case "snappy":
                    return new SnappyInputStream(stream);
                case "none":
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

    private CompressionType findCompressionType(Config config) {
        final String compressionType = config.hasPath(COMPRESSION_TYPE) ? config.getString(COMPRESSION_TYPE).toUpperCase() : "NONE";
        return CompressDecompressService.CompressionType.valueOf(compressionType);
    }
}
