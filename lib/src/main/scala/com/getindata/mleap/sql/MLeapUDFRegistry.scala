package com.getindata.mleap.sql

import com.getindata.mleap.{BundleLoader, BundleLoaderFactory, InputFrameCreator}
import com.typesafe.config.Config
import ml.combust.mleap.runtime.frame.Transformer
import org.apache.flink.table.api.TableEnvironment
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}


sealed trait BundleSource

case object FileSource extends BundleSource

case object GCSSource extends BundleSource

case class InvalidBundleSourceException(invalidSource: String) extends Throwable(invalidSource)

object BundleSource {
  def apply(source: String): Try[BundleSource] = {
    source match {
      case "file" => Success(FileSource)
      case "gcs" => Success(GCSSource)
      case invalidSource => Failure(InvalidBundleSourceException(invalidSource))
    }
  }
}

case class MLeapUDFConfig(udfName: String, bundlePath: String, bundleSource: BundleSource) {
  def getBundlePath: String =
    bundleSource match {
      case FileSource => getClass.getResource(bundlePath).toString
      case GCSSource => bundlePath
    }
}

trait MLeapUDFRegistry {
  def registerFromConfig(conf: Config, tableEnvironment: TableEnvironment): Boolean

  def register(configs: List[MLeapUDFConfig], tableEnvironment: TableEnvironment, config: Config): Boolean
}

object MLeapUDFRegistry extends MLeapUDFRegistry {
  private val LOG = LoggerFactory.getLogger(classOf[MLeapUDFRegistry])


  def registerFromConfig(conf: Config, tableEnvironment: TableEnvironment): Boolean = {
    val configs = Try {
      conf.getConfigList("mleap.udfRegistry").asScala.map(
        c => MLeapUDFConfig(c.getString("udfName"), c.getString("bundlePath"), BundleSource(c.getString("bundleSource")).get))
    }
    configs match {
      case Failure(exception) =>
        LOG.error("Error with config parsing: ", exception)
        false
      case Success(properConfigs) =>
        register(properConfigs.toList, tableEnvironment, conf)
    }
  }

  def register(configs: List[MLeapUDFConfig], tableEnvironment: TableEnvironment, config: Config): Boolean = {
    val bundleLoaders = prepareNecessaryBundleLoaders(configs, config)
    val bundles: Try[Map[String, Transformer]] = Try {
      configs.map(c => (c.udfName, bundleLoaders(c.bundleSource).loadBundle(c.getBundlePath).get)).toMap
    }
    bundles match {
      case Failure(exception) =>
        LOG.error("Error with loading bundles: ", exception)
        false
      case Success(loadedBundles) =>
        configs.foreach {
          c =>
            val bundle = loadedBundles(c.udfName)
            tableEnvironment.createTemporarySystemFunction(c.udfName, new MLeapUDF(c.getBundlePath, mapBundleSourceToLoader(c.bundleSource, config),
              bundle.inputSchema, bundle.outputSchema, InputFrameCreator))
        }
        true
    }
  }

  private def prepareNecessaryBundleLoaders(configs: List[MLeapUDFConfig], config: Config): Map[BundleSource, BundleLoader] = {
    configs.map(_.bundleSource).toSet.map((source: BundleSource) => (source, mapBundleSourceToLoader(source, config)())).toMap
  }

  private def mapBundleSourceToLoader(source: BundleSource, config: Config): () => BundleLoader = source match {
    case FileSource => () => BundleLoaderFactory.creteFileLoader
    case GCSSource => () => BundleLoaderFactory.creteGCSLoader(config)
  }
}
