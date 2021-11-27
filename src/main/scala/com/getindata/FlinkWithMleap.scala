package com.getindata

import com.getindata.bundle.FileBundleLoader
import ml.combust.mleap.core.types.StructType
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import java.util.Random

object FlinkWithMleap {
  def main(args: Array[String]): Unit = {

    implicit val typeInfo = TypeInformation.of(classOf[StructType])
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val rand: Random = new Random()

    val text = env.fromElements(rand.nextDouble(), rand.nextDouble(), rand.nextDouble())
    val bundlePath = getClass.getResource("/mleap-example-1").toString

    text.keyBy(v => "1").process(MleapProcessFunction(bundlePath, FileBundleLoader)).print()

    env.execute()
  }
}
