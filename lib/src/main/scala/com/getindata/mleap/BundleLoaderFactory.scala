package com.getindata.mleap

import com.getindata.mleap.gcs.GCSClientImpl
import com.typesafe.config.Config

import scala.concurrent.duration._

trait BundleLoaderFactory {
  def creteFileLoader: BundleLoader

  def creteGCSLoader(config: Config): BundleLoader
}

case object BundleLoaderFactory extends BundleLoaderFactory {
  override def creteFileLoader: BundleLoader = FileBundleLoader

  override def creteGCSLoader(config: Config): BundleLoader = GCSBundleLoader(GCSClientImpl(config), 30.seconds)
}
