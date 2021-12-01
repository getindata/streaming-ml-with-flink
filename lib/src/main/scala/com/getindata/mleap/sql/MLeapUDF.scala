package com.getindata.mleap.sql

import com.getindata.mleap.datastream.BundleLoadProblem
import com.getindata.mleap.{BundleLoader, InputFrameCreator}
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.Transformer
import org.apache.flink.table.annotation.{DataTypeHint, InputGroup}
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.functions.{FunctionContext, ScalarFunction}
import org.apache.flink.table.types.inference.{CallContext, TypeInference}
import org.slf4j.LoggerFactory

import java.util.Optional
import scala.annotation.varargs
import scala.util.{Failure, Success}


class MLeapUDF(bundlePath: String, bundleLoader: () => BundleLoader, inputSchema: StructType, outputSchema: StructType,
               inputFrameCreator: InputFrameCreator) extends ScalarFunction {
  private val LOG = LoggerFactory.getLogger(classOf[MLeapUDF])

  @transient var transformer: Transformer = _

  override def open(context: FunctionContext): Unit = {

    transformer = bundleLoader().loadBundle(bundlePath) match {
      case Failure(exception) => {
        LOG.error(s"Error while loading bundle: $bundlePath", exception)
        throw BundleLoadProblem(exception)
      }
      case Success(value) => value
    }
  }

  @varargs
  def eval(@DataTypeHint(inputGroup = InputGroup.ANY) values: AnyRef*): AnyRef = {
    val frame = inputFrameCreator.prepareFrame(values.toList, inputSchema)

    transformer.transform(frame).get.dataset.head(1).asInstanceOf[AnyRef]
  }

  override def getTypeInference(typeFactory: DataTypeFactory): TypeInference = {
    TypeInference.newBuilder()
      // TODO: create output strategy basing on schema
      .outputTypeStrategy((_: CallContext) => {
        outputSchema.fields.head.dataType.base match {
          case BasicType.Boolean => Optional.of(DataTypes.BOOLEAN().notNull())
          case BasicType.Byte => Optional.of(DataTypes.TINYINT().notNull())
          case BasicType.Short => Optional.of(DataTypes.SMALLINT().notNull())
          case BasicType.Int => Optional.of(DataTypes.INT().notNull())
          case BasicType.Long => Optional.of(DataTypes.BIGINT().notNull())
          case BasicType.Float => Optional.of(DataTypes.FLOAT().notNull())
          case BasicType.Double => Optional.of(DataTypes.DOUBLE().notNull())
          case BasicType.String => Optional.of(DataTypes.STRING().notNull())
          case BasicType.ByteString => Optional.of(DataTypes.BYTES().notNull())
        }
      })
      .build()
  }
}
