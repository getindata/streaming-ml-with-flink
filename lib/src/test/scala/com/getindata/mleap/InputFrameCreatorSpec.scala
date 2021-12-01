package com.getindata.mleap

import com.google.protobuf.ByteString
import ml.combust.mleap.core.types.{BasicType, StructField, StructType, TensorType}
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import ml.combust.mleap.tensor.DenseTensor
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec


class InputFrameCreatorSpec extends AnyWordSpec with Matchers {

  "InputFrameCreator" should {
    "prepare frame for Boolean tensor" in {
      val inputSchema = StructType(Seq(StructField("x", new TensorType(BasicType.Boolean, false)))).get
      val values = List(true, false)
      InputFrameCreator.prepareFrame(
        values.map(_.asInstanceOf[AnyRef]), inputSchema
      ).shouldBe(DefaultLeapFrame(inputSchema, Seq(Row(DenseTensor(values.toArray[Boolean], Seq(2))))))
    }

    "prepare frame for Byte tensor" in {
      val inputSchema = StructType(Seq(StructField("x", new TensorType(BasicType.Byte, false)))).get
      val values = List(1.toByte, 2.toByte)
      InputFrameCreator.prepareFrame(
        values.map(_.asInstanceOf[AnyRef]), inputSchema
      ).shouldBe(DefaultLeapFrame(inputSchema, Seq(Row(DenseTensor(values.toArray[Byte], Seq(2))))))
    }

    "prepare frame for Short tensor" in {
      val inputSchema = StructType(Seq(StructField("x", new TensorType(BasicType.Short, false)))).get
      val values = List(1.toShort, 2.toShort)
      InputFrameCreator.prepareFrame(
        values.map(_.asInstanceOf[AnyRef]), inputSchema
      ).shouldBe(DefaultLeapFrame(inputSchema, Seq(Row(DenseTensor(values.toArray[Short], Seq(2))))))
    }

    "prepare frame for Int tensor" in {
      val inputSchema = StructType(Seq(StructField("x", new TensorType(BasicType.Int, false)))).get
      val values = List(1, 2)
      InputFrameCreator.prepareFrame(
        values.map(_.asInstanceOf[AnyRef]), inputSchema
      ).shouldBe(DefaultLeapFrame(inputSchema, Seq(Row(DenseTensor(values.toArray[Int], Seq(2))))))
    }

    "prepare frame for Long tensor" in {
      val inputSchema = StructType(Seq(StructField("x", new TensorType(BasicType.Long, false)))).get
      val values = List(1L, 2L)
      InputFrameCreator.prepareFrame(
        values.map(_.asInstanceOf[AnyRef]), inputSchema
      ).shouldBe(DefaultLeapFrame(inputSchema, Seq(Row(DenseTensor(values.toArray[Long], Seq(2))))))
    }

    "prepare frame for Float tensor" in {
      val inputSchema = StructType(Seq(StructField("x", new TensorType(BasicType.Float, false)))).get
      val values = List(1.0F, 2.0F)
      InputFrameCreator.prepareFrame(
        values.map(_.asInstanceOf[AnyRef]), inputSchema
      ).shouldBe(DefaultLeapFrame(inputSchema, Seq(Row(DenseTensor(values.toArray[Float], Seq(2))))))
    }

    "prepare frame for Double tensor" in {
      val inputSchema = StructType(Seq(StructField("x", new TensorType(BasicType.Double, false)))).get
      val values = List(1.0, 2.0)
      InputFrameCreator.prepareFrame(
        values.map(_.asInstanceOf[AnyRef]), inputSchema
      ).shouldBe(DefaultLeapFrame(inputSchema, Seq(Row(DenseTensor(values.toArray[Double], Seq(2))))))
    }

    "prepare frame for String tensor" in {
      val inputSchema = StructType(Seq(StructField("x", new TensorType(BasicType.String, false)))).get
      val values = List("a", "b")
      InputFrameCreator.prepareFrame(
        values.map(_.asInstanceOf[AnyRef]), inputSchema
      ).shouldBe(DefaultLeapFrame(inputSchema, Seq(Row(DenseTensor(values.toArray[String], Seq(2))))))
    }

    "prepare frame for ByteString tensor" in {
      val inputSchema = StructType(Seq(StructField("x", new TensorType(BasicType.ByteString, false)))).get
      val values = List(ByteString.copyFrom(Array(1.toByte)), ByteString.copyFrom(Array(2.toByte)))
      InputFrameCreator.prepareFrame(
        values.map(_.asInstanceOf[AnyRef]), inputSchema
      ).shouldBe(DefaultLeapFrame(inputSchema, Seq(Row(DenseTensor(values.toArray[ByteString], Seq(2))))))
    }
  }
}
