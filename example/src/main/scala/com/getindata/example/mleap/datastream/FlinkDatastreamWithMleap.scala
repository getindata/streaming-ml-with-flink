package com.getindata.example.mleap.datastream

import com.getindata.mleap.FileBundleLoader
import com.getindata.mleap.datastream.MleapMapFunction
import ml.combust.mleap.core.types.StructType
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import java.util.Random

object FlinkDatastreamWithMleap {
  def main(args: Array[String]): Unit = {

    implicit val typeInfo = TypeInformation.of(classOf[StructType])
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val rand: Random = new Random()

    val text = env.fromElements(rand.nextDouble(), rand.nextDouble(), rand.nextDouble())
    val bundlePath = getClass.getResource("/mleap-example-1").toString

    text.map(MleapMapFunction(bundlePath, FileBundleLoader)).print()

    env.execute()
  }
}
