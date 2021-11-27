package com.getindata

import ml.combust.mleap.core.types.StructType
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object FlinkWithMleapSql {
  def main(args: Array[String]): Unit = {

    implicit val typeInfo = TypeInformation.of(classOf[StructType])
    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    tableEnv.executeSql("" +
      "CREATE TABLE Features (\n    " +
      "feature1 DOUBLE NOT NULL,\n    " +
      "feature_timestamp   TIMESTAMP(3)\n) " +
      "WITH (  'connector' = 'datagen', 'number-of-rows' = '10', 'fields.feature1.min' = '0.0', 'fields.feature1.max' = '1.0')")

    tableEnv.createTemporarySystemFunction("Predict", new MLeapUDF(getClass.getResource("/mleap-example-1").toString))
    tableEnv.createTemporarySystemFunction("Predictv2", new MLeapUDF(getClass.getResource("/mleap-example-2").toString))

    tableEnv.executeSql("SELECT Predict(feature1) as prediction, Predictv2(feature1) as prediction2 FROM Features").print()

    env.execute()
  }
}
