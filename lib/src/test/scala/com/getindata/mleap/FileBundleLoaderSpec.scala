package com.getindata.mleap

import ml.combust.mleap.runtime.frame.Transformer
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class FileBundleLoaderSpec extends AnyWordSpec with Matchers {

  "FileBundleLoader" should {
    "load bundle from file" in {
      FileBundleLoader.loadBundle(getClass.getResource("/mleap-bundle").toString).get  shouldBe a [Transformer]
    }
  }

}
