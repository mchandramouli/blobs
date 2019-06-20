package com.expedia.www.haystack.agent.blobs.server.spi

import java.io.{File, IOException}
import java.net.URL
import java.util

import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.easymock.EasyMockSugar

class BlobAgentSpec extends FunSpec with Matchers with EasyMockSugar {

  private val dispatcherLoadFile = "META-INF/services/com.expedia.www.haystack.agent.blobs.dispatcher.core.BlobDispatcher"

  describe("Blob Agent") {
    it("should return the 'blobs' as name") {
      new BlobAgent().getName shouldEqual "blobs"
    }

    it("should load the dispatchers from the config") {
      val agent = new BlobAgent()
      val cfg = ConfigFactory.parseString(
        """
          |    port = 34001
          |
          |    dispatchers {
          |      test-dispatcher {
          |        bucketName = "mybucket"
          |        region = "us-east-1"
          |      }
          |    }
        """.stripMargin)

      val cl = new ReplacingClassLoader(getClass.getClassLoader, dispatcherLoadFile, "META-INF/services/dispatcherProvider.txt")
      val dispatchers = agent.loadAndInitializeDispatchers(cfg, cl)
      dispatchers.size() shouldBe 1
      dispatchers.get(0).close()
    }
  }

  it("initialization should fail if no dispatchers exist") {
    val agent = new BlobAgent()
    val cfg = ConfigFactory.parseString(
      """
        |    port = 34001
        |
        |    dispatchers {
        |      test-dispatcher {
        |        bucketName = "mybucket"
        |        region = "us-east-1"
        |      }
        |    }
      """.stripMargin)

    val caught = intercept[Exception] {
      agent.loadAndInitializeDispatchers(cfg, getClass.getClassLoader)
    }

    caught.getMessage shouldEqual "Blob agent dispatchers can't be an empty set"
  }

  class ReplacingClassLoader(val parent: ClassLoader, val resource: String, val replacement: String) extends ClassLoader(parent) {
    override def getResource(name: String): URL = {
      if (resource == name) {
        return getParent.getResource(replacement)
      }
      super.getResource(name)
    }

    @throws[IOException]
    override def getResources(name: String): util.Enumeration[URL] = {
      if (resource == name) {
        return getParent.getResources(replacement)
      }
      super.getResources(name)
    }
  }

}
