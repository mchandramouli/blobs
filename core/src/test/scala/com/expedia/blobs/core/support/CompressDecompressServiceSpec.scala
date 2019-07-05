package com.expedia.blobs.core.support

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.commons.io.{FileUtils, IOUtils}
import org.scalatest.{FunSpec, GivenWhenThen, Matchers}
import org.scalatest.easymock.EasyMockSugar

class CompressDecompressServiceSpec extends FunSpec with Matchers with GivenWhenThen with EasyMockSugar {
  private val blobStream = getClass.getClassLoader.getResourceAsStream("response.txt")
  private val blobBytes = IOUtils.toByteArray(blobStream)
  private val gzipCompressedStream = getClass.getClassLoader.getResourceAsStream("gzipCompressedData.txt")
  private val snappyCompressedStream = getClass.getClassLoader.getResourceAsStream("snappyCompressedData.txt")


  describe("The compression and decompression service") {

    it("should compress the blob with GZIP compression type") {

      Given("config with compression type as GZIP")
      val config = ConfigFactory.parseString(
        """
          |compressionType = "GZIP"
        """.stripMargin)

      val compressDecompressService = new CompressDecompressService(config)

      When("compress call is made")

      val blobInputStream = compressDecompressService.compressData(blobBytes)

      Then("Size of output stream should be less than input stream")
      compressDecompressService.getCompressionType shouldBe "GZIP"
      assert(blobInputStream.getLength < blobBytes.length)
      blobInputStream.getStream should not be (null)
    }

    it("should compress the blob with SNAPPY compression type") {
      Given("config with compression type as SNAPPY")
      val config = ConfigFactory.parseString(
        """
          |compressionType = "SNAPPY"
        """.stripMargin)

      val compressDecompressService = new CompressDecompressService(config)

      When("compress call is made")

      val blobInputStream = compressDecompressService.compressData(blobBytes)


      Then("Size of output stream should be less than input stream")
      compressDecompressService.getCompressionType shouldBe "SNAPPY"
      assert(blobInputStream.getLength < blobBytes.length)
      blobInputStream.getStream should not be (null)

      val file = new File("/Users/vsawhney/OpenSource-Blobs/vaibhavsawhney/blobs/core/src/test/resources/snappyCompressedData.txt")

      FileUtils.writeByteArrayToFile(file, IOUtils.toByteArray(blobInputStream.getStream))
    }

    it("should not compress the blob") {
      Given("config with compression type as NONE")
      val config = ConfigFactory.parseString(
        """
          |compressionType = "NONE"
        """.stripMargin)

      val compressDecompressService = new CompressDecompressService(config)

      When("compress call is made")

      val blobInputStream = compressDecompressService.compressData(blobBytes)


      Then("Size of output stream should be less than input stream")
      compressDecompressService.getCompressionType shouldBe "NONE"
      blobInputStream.getLength shouldBe blobBytes.length
      blobInputStream.getStream should not be (null)
    }

    it("should not compress if no compression type is specified") {
      Given("config with no compression type")
      val config = ConfigFactory.parseString(
        """
        """.stripMargin)

      val compressDecompressService = new CompressDecompressService(config)

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
      val uncompressedBlobStream = CompressDecompressService.uncompressData("gzip", gzipCompressedStream)

      val uncompressedBlobArray = IOUtils.toByteArray(uncompressedBlobStream)
      val gzipCompressedBlobArray = IOUtils.toByteArray(gzipCompressedStream)

      Then("Size of output stream should be greater than input stream")
      uncompressedBlobStream should not be (null)
      assert(uncompressedBlobArray.length > gzipCompressedBlobArray.length)

    }

    it("should uncompress blob with SNAPPY type") {
      Given("compressed blob using SNAPPY compression type")

      When("uncompress call is made")
      val uncompressedBlobStream = CompressDecompressService.uncompressData("SNAPPY", snappyCompressedStream)

      val uncompressedBlobArray = IOUtils.toByteArray(uncompressedBlobStream)
      val snappyCompressedBlobArray = IOUtils.toByteArray(snappyCompressedStream)

      Then("Size of output stream should be greater than input stream")
      uncompressedBlobStream should not be (null)
      assert(uncompressedBlobArray.length > snappyCompressedBlobArray.length)

    }

    it("should return exact blob object when compression type is 'NONE' ") {
      Given("uncompressed blob")

      When("uncompress call is made")
      val uncompressedBlobStream = CompressDecompressService.uncompressData("NONE", blobStream)

      val uncompressedBlobArray = IOUtils.toByteArray(uncompressedBlobStream)
      val compressedBlobArray = IOUtils.toByteArray(blobStream)

      Then("Size of output stream should be greater than input stream")
      uncompressedBlobStream should not be (null)
      uncompressedBlobArray.length shouldEqual compressedBlobArray.length
    }

  }
}
