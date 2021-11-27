package com.getindata

import com.getindata.bundle.FileBundleLoader
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row, Transformer}
import ml.combust.mleap.tensor.DenseTensor
import org.apache.flink.table.functions.{FunctionContext, ScalarFunction}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}


class MLeapUDF(bundlePath: String) extends ScalarFunction {
  private val LOG = LoggerFactory.getLogger(classOf[MLeapUDF])

  @transient var transformer: Transformer = _

  override def open(context: FunctionContext): Unit = {
    transformer = FileBundleLoader.loadBundle(bundlePath) match {
      case Failure(exception) => {
        LOG.error(s"Error while loading bundle: $bundlePath", exception)
        throw BundleLoadProblem(exception)
      }
      case Success(value) => value
    }
  }

  def eval(value: Double): Double = {
    println(value)
    val dataset = Seq(Row(DenseTensor(Array(value), List(1))))
    val frame = DefaultLeapFrame(transformer.inputSchema, dataset)
    transformer.transform(frame).get.dataset.head(1).asInstanceOf[Double]
  }

}
