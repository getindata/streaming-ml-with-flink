package com.getindata.example.mleap

import ml.combust.bundle.BundleFile
import ml.combust.bundle.dsl.Bundle
import ml.combust.mleap.core.types.{BasicType, StructField, StructType, TensorType}
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row, Transformer}
import ml.combust.mleap.tensor.DenseTensor
import resource.{ExtractableManagedResource, managed}

import java.util.Random

object MLeapApp extends App {
  val bundle: ExtractableManagedResource[Bundle[Transformer]] = for (bf <- managed(BundleFile(MLeapApp.getClass.getResource("/mleap-example-1").toString))) yield {
    bf.loadMleapBundle().get
  }

  val schema = StructType(List(StructField("x", TensorType(BasicType.Double)))).get
  val transformer: Transformer = bundle.opt.get.root
  val rand = new Random()

  val dataset = Seq(Row(DenseTensor(Array(rand.nextFloat.toDouble), List(1))))
  val frame = DefaultLeapFrame(schema, dataset)
  println(transformer.transform(frame).get.dataset.head(1))

}
