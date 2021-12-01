package com.getindata.mleap

import com.getindata.mleap.gcs.GCSClient
import ml.combust.bundle.BundleFile
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.frame.Transformer
import net.lingala.zip4j.ZipFile
import resource.managed

import java.io.File
import java.nio.file.{Files, Path}
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Future, blocking}
import scala.util.{Failure, Success, Try}

sealed trait BundleLoader {
  def loadBundle(bundlePath: String): Try[Transformer]
}

case object FileBundleLoader extends BundleLoader {
  override def loadBundle(bundlePath: String): Try[Transformer] = {
    val bundle = for (bf <- managed(BundleFile(bundlePath))) yield {
      bf.loadMleapBundle().get
    }
    bundle.tried.map(_.root)
  }
}

case class GCSBundleLoader(gcsClient: GCSClient, loadingTimeout: FiniteDuration) extends BundleLoader {
  override def loadBundle(bundlePath: String): Try[Transformer] = {
    val bundleF: Future[Try[Transformer]] = for {
      zippedBundle <- gcsClient.downloadBundle(bundlePath)
      unzippedBundle <- unzip(zippedBundle)
    } yield unzippedBundle.flatMap(
      bundle => FileBundleLoader.loadBundle("file:" + bundle.toString)
    )

    Await.result(bundleF, loadingTimeout)
  }

  private def unzip(file: Try[File]): Future[Try[File]] = {
    file match {
      case Success(source) => Future {
        blocking {
          Try {
            val tmpDir: Path = Files.createTempDirectory("bundle-unzipped")
            val zipFile = new ZipFile(source)
            zipFile.extractAll(tmpDir.toString)
            tmpDir.toFile
          }
        }
      }
      case Failure(exception) => Future.successful(Failure(exception))
    }
  }
}