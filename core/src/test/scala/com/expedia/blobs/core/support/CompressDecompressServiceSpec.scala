package com.expedia.blobs.core.support

import org.apache.commons.io.IOUtils
import org.scalatest.easymock.EasyMockSugar
import org.scalatest.{FunSpec, GivenWhenThen, Matchers}

class CompressDecompressServiceSpec extends FunSpec with Matchers with GivenWhenThen with EasyMockSugar {
  private val blobStream = getClass.getClassLoader.getResourceAsStream("response.txt")
  private val blobBytes = IOUtils.toByteArray(blobStream)
  private val gzipCompressedStream = getClass.getClassLoader.getResourceAsStream("gzipCompressedData.txt")
  private val snappyCompressedStream = getClass.getClassLoader.getResourceAsStream("snappyCompressedData.txt")


  describe("The compression and decompression service") {

    it("should compress the blob with GZIP compression type") {

      Given("initialized CompressDecompressService with compression type as GZIP")

      val compressDecompressService = new CompressDecompressService(CompressDecompressService.CompressionType.GZIP)

      When("compress call is made")

      val blobInputStream = compressDecompressService.compressData(blobBytes)

      Then("Size of output stream should be less than input stream")
      compressDecompressService.getCompressionType shouldBe "GZIP"
      assert(blobInputStream.getLength < blobBytes.length)
      blobInputStream.getStream should not be (null)
    }

    it("should compress the blob with SNAPPY compression type") {
      Given("initialized CompressDecompressService with compression type as SNAPPY")

      val compressDecompressService = new CompressDecompressService(CompressDecompressService.CompressionType.SNAPPY)

      When("compress call is made")

      val blobInputStream = compressDecompressService.compressData(blobBytes)


      Then("Size of output stream should be less than input stream")
      compressDecompressService.getCompressionType shouldBe "SNAPPY"
      assert(blobInputStream.getLength < blobBytes.length)
      blobInputStream.getStream should not be (null)
    }

    it("should not compress the blob") {
      Given("initialized CompressDecompressService with compression type as NONE")

      val compressDecompressService = new CompressDecompressService(CompressDecompressService.CompressionType.NONE)

      When("compress call is made")

      val blobInputStream = compressDecompressService.compressData(blobBytes)


      Then("Size of output stream should be less than input stream")
      compressDecompressService.getCompressionType shouldBe "NONE"
      blobInputStream.getLength shouldBe blobBytes.length
      blobInputStream.getStream should not be (null)
    }

    it("should uncompress blob with GZIP type") {
      Given("compressed blob using GZIP compression type")

      When("uncompress call is made")
      val uncompressedBlobStream = CompressDecompressService.uncompressData(CompressDecompressService.CompressionType.GZIP, gzipCompressedStream)

      val uncompressedBlobArray = IOUtils.toByteArray(uncompressedBlobStream)
      val gzipCompressedBlobArray = IOUtils.toByteArray(gzipCompressedStream)

      Then("Size of output stream should be greater than input stream")
      uncompressedBlobStream should not be (null)
      assert(uncompressedBlobArray.length > gzipCompressedBlobArray.length)

    }

    it("should uncompress blob with SNAPPY type") {
      Given("compressed blob using SNAPPY compression type")

      When("uncompress call is made")
      val uncompressedBlobStream = CompressDecompressService.uncompressData(CompressDecompressService.CompressionType.SNAPPY, snappyCompressedStream)

      val uncompressedBlobArray = IOUtils.toByteArray(uncompressedBlobStream)
      val snappyCompressedBlobArray = IOUtils.toByteArray(snappyCompressedStream)

      Then("Size of output stream should be greater than input stream")
      uncompressedBlobStream should not be (null)
      assert(uncompressedBlobArray.length > snappyCompressedBlobArray.length)

    }

    it("should return exact blob object when compression type is 'NONE' ") {
      Given("uncompressed blob")

      When("uncompress call is made")
      val uncompressedBlobStream = CompressDecompressService.uncompressData(CompressDecompressService.CompressionType.NONE, blobStream)

      val uncompressedBlobArray = IOUtils.toByteArray(uncompressedBlobStream)
      val compressedBlobArray = IOUtils.toByteArray(blobStream)

      Then("Size of output stream should be greater than input stream")
      uncompressedBlobStream should not be (null)
      uncompressedBlobArray.length shouldEqual compressedBlobArray.length
    }

  }
}
