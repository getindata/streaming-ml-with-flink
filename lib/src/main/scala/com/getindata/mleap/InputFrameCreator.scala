package com.getindata.mleap

import com.google.protobuf.ByteString
import ml.combust.mleap.core.types.{BasicType, ListType, ScalarType, StructType, TensorType}
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import ml.combust.mleap.tensor.DenseTensor


trait InputFrameCreator {
  def prepareFrame(values: List[AnyRef], inputSchema: StructType): DefaultLeapFrame
}

case object InputFrameCreator extends InputFrameCreator {
  def prepareFrame(values: List[AnyRef], inputSchema: StructType): DefaultLeapFrame = {
    val tensor = inputSchema.fields.head.dataType match {
      case ScalarType(base, _) => createTensor(values, base)
      case TensorType(base, _, _) => createTensor(values, base)
      case ListType(base, _) => createTensor(values, base)
    }

    DefaultLeapFrame(inputSchema, Seq(Row(tensor)))
  }

  private def createTensor(values: List[AnyRef], base: BasicType) = base match {
    case BasicType.Boolean => DenseTensor(values.map(_.asInstanceOf[Boolean]).toArray, List(values.length))
    case BasicType.Byte => DenseTensor(values.map(_.asInstanceOf[Byte]).toArray, List(values.length))
    case BasicType.Short => DenseTensor(values.map(_.asInstanceOf[Short]).toArray, List(values.length))
    case BasicType.Int => DenseTensor(values.map(_.asInstanceOf[Int]).toArray, List(values.length))
    case BasicType.Long => DenseTensor(values.map(_.asInstanceOf[Long]).toArray, List(values.length))
    case BasicType.Float => DenseTensor(values.map(_.asInstanceOf[Float]).toArray, List(values.length))
    case BasicType.Double => DenseTensor(values.map(_.asInstanceOf[Double]).toArray, List(values.length))
    case BasicType.String => DenseTensor(values.map(_.asInstanceOf[String]).toArray, List(values.length))
    case BasicType.ByteString => DenseTensor(values.map(_.asInstanceOf[ByteString]).toArray, List(values.length))
  }
}
