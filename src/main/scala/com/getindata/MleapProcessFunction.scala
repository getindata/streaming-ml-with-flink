package com.getindata

import com.getindata.bundle.BundleLoader
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row, Transformer}
import ml.combust.mleap.tensor.DenseTensor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

case class BundleLoadProblem(th: Throwable) extends RuntimeException(th)

case class MleapProcessFunction(bundleName: String, bundleLoader: BundleLoader) extends KeyedProcessFunction[String, Double, Double] {

  private val LOG = LoggerFactory.getLogger(classOf[MleapProcessFunction])
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

  override def processElement(value: Double, ctx: KeyedProcessFunction[String, Double, Double]#Context, out: Collector[Double]): Unit = {
    val dataset = Seq(Row(DenseTensor(Array(value), List(1))))
    val frame = DefaultLeapFrame(transformer.inputSchema, dataset)
    val res = transformer.transform(frame).get.dataset.head(1).asInstanceOf[Double]
    out.collect(res)
  }
}