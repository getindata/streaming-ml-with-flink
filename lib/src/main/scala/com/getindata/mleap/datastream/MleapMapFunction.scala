package com.getindata.mleap.datastream

import com.getindata.mleap.BundleLoader
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row, Transformer}
import ml.combust.mleap.tensor.DenseTensor
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

case class BundleLoadProblem(th: Throwable) extends RuntimeException(th)

case class MleapMapFunction(bundleName: String, bundleLoader: BundleLoader) extends
  RichMapFunction[Double, Double] {

  private val LOG = LoggerFactory.getLogger(classOf[MleapMapFunction])
  @transient var transformer: Transformer = _

  override def open(parameters: Configuration): Unit = {
    transformer = bundleLoader.loadBundle(bundleName) match {
      case Failure(exception) => {
        LOG.error(s"Error while loading bundle: $bundleName", exception)
        throw BundleLoadProblem(exception)
      }
      case Success(value) => value
    }
  }

  override def map(value: Double): Double = {
    val dataset = Seq(Row(DenseTensor(Array(value), List(1))))
    val frame = DefaultLeapFrame(transformer.inputSchema, dataset)
    val res = transformer.transform(frame).get.dataset.head(1).asInstanceOf[Double]
    res
  }
}