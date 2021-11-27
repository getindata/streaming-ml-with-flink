package com.getindata.bundle

import ml.combust.bundle.BundleFile
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.frame.Transformer
import resource.managed

import scala.util.Try

sealed trait BundleLoader  {
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

// TODO add loader from gs, redis?