package com.tencent.angel.serving.sources

import java.io.File
import java.net.URI
import scala.collection.JavaConverters._
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.TrueFileFilter
import org.apache.hadoop.conf.Configuration
import scala.collection.immutable.Set
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.util.matching.Regex
import com.tencent.angel.serving.core.{FailedPreconditions, NotFoundExceptions}

object SystemFileUtils {
  def fileExist(basePath: String, conf: Configuration = null): Boolean ={
    if (conf == null) {
      throw FailedPreconditions("hadoop configuration has not been set!")
    }
    val fs = FileSystem.get(URI.create(basePath),conf)
    if (fs.exists(new Path(basePath))) {
      fs.isDirectory(new Path(basePath))
    } else {
      throw NotFoundExceptions(s"the HDFS basePath: ${basePath} is not found!")
    }
  }

  def getChildren(basePath: String, conf: Configuration = null):Set[String] = {
    if (conf == null) {
      throw FailedPreconditions("hadoop configuration has not been set!")
    }
    val fs = FileSystem.get(URI.create(basePath),conf)
    val path = new Path(basePath)
    val fileStatus  = fs.listStatus(path)
    val pattern = new Regex("^[0-9]*$")
    val children = fileStatus.filter(fs => fs.isDirectory).filterNot(fs => fs.getPath.equals(path)).map{ fs =>
      pattern.findAllMatchIn(fs.getPath.getName).mkString("")
    }.filterNot(child => child.isEmpty)
    children.toSet
  }
}

