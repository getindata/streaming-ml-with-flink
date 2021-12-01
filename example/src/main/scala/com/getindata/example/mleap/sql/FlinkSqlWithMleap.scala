package com.getindata.example.mleap.sql

import com.getindata.mleap.sql.MLeapUDFRegistry
import com.typesafe.config.ConfigFactory
import ml.combust.mleap.core.types.StructType
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object FlinkSqlWithMleap {
  def main(args: Array[String]): Unit = {

    implicit val typeInfo = TypeInformation.of(classOf[StructType])
    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    // Register UDFs basing on config
    val config = ConfigFactory.load()
    MLeapUDFRegistry.registerFromConfig(config, tableEnv)

    // Create source
    tableEnv.executeSql("" +
      "CREATE TABLE Features (\n    " +
      "feature1 DOUBLE NOT NULL,\n    " +
      "feature_timestamp   TIMESTAMP(3)\n) " +
      "WITH (  'connector' = 'datagen', 'number-of-rows' = '10', 'fields.feature1.min' = '0.0', 'fields.feature1.max' = '1.0')")

    // Execute predictions
    tableEnv.executeSql("SELECT Predict(feature1) as prediction, Predictv2(feature1) as prediction2 FROM Features").print()

    env.execute()
  }
}
