package com.getindata.mleap

import com.getindata.mleap.gcs.{GCSClient, GCSClientBundleDownloadError}
import ml.combust.mleap.runtime.frame.Transformer
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.File
import java.nio.file.Paths
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Success, Try}


class GCSBundleLoaderSpec extends AnyWordSpec with Matchers {

  val bundleURI = "bucket/mlep-bundle"

  val fakeClient = new GCSClient {
    override def downloadBundle(uri: String): Future[Try[File]] = {
      if (uri == bundleURI) Future.successful(Success(Paths.get(getClass.getResource("/mleap-bundle.zip").toURI).toFile))
      else Future.failed(new GCSClientBundleDownloadError(s"Problem with downloading bundle: $uri", new RuntimeException("")))
    }
  }
  "GCSBundleLoader" should {
    "load bundle using GCS client" in {
      val loader = GCSBundleLoader(fakeClient, 5.second)
      loader.loadBundle(bundleURI).get  shouldBe a [Transformer]
    }
  }
}
