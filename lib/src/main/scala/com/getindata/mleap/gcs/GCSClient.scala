package com.getindata.mleap.gcs

import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.storage.{BlobId, Storage, StorageOptions}
import com.typesafe.config.Config

import java.io.File
import java.nio.file.{Files, Paths}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, blocking}
import scala.util.{Failure, Try}

case class GCSClientBundleDownloadError(msg: String, e: Throwable) extends RuntimeException(msg, e)

trait GCSClient {
  def downloadBundle(uri: String): Future[Try[File]]
}

class GCSClientImpl(bucket: String, storage: Storage) extends GCSClient {
  override def downloadBundle(uri: String): Future[Try[File]] = {
    Future {
      blocking {
        val tmpDir = Files.createTempDirectory("mleap-bundle")
        val tmpFile = Paths.get(tmpDir.toString, "bundle.zip")
        val fileTry = Try {
          val blob = storage.get(BlobId.of(bucket, uri))
          blob.downloadTo(tmpFile)
          tmpFile.toFile
        } recoverWith {
          case ex =>
            Failure(GCSClientBundleDownloadError(s"Problem with downloading bundle: $uri", ex))
        }
        fileTry
      }
    }
  }
}

object GCSClientImpl {
  def apply(config: Config): GCSClient = {

    val bucket = config.getString("mleap.gcs.bucket")
    val storage = StorageOptions.getDefaultInstance().getService()

    new GCSClientImpl(bucket, storage)
  }
}