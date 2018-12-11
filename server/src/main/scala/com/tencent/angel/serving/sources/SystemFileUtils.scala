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
    if (basePath.startsWith("hdfs://")){
      if (conf == null) {
        throw FailedPreconditions("hadoop configuration has not been set!")
      }
      val fs = FileSystem.get(URI.create(basePath),conf)
      if (fs.exists(new Path(basePath))) {
        fs.isDirectory(new Path(basePath))
      } else {
        throw NotFoundExceptions(s"the HDFS basePath: ${basePath} is not found!")
      }
    } else {
      val filePath = new File(basePath)
      if (filePath.exists()) {
        filePath.isDirectory
      } else {
        throw NotFoundExceptions(s"the FileSystem basePath: ${basePath} is not found!")
      }

    }
  }

  def getChildren(basePath: String, conf: Configuration = null):Set[String] = {
    if (basePath.startsWith("hdfs://")){
      if (conf == null) {
        throw FailedPreconditions("hadoop configuration has not been set!")
      }
      getChildrenFromHDFS(basePath, conf)
    } else {
      getChildrenFromLocal(basePath)
    }
  }

  def getChildrenFromLocal(basePath: String): Set[String] ={
    val f = new File(basePath)
    val len = f.getPath.length
    val listFiles = FileUtils.listFilesAndDirs(f, TrueFileFilter.INSTANCE, TrueFileFilter.TRUE)
    val realChildren = listFiles.asScala.filter(child => child.getAbsoluteFile.isDirectory).filterNot(
      child => child.getPath.equals(f.getPath)).map{child =>
      val childStr = child.getPath.substring(len + 1)
      val pattern = new Regex("^[0-9]*$")
      if (childStr.contains("\\")){
        pattern.findAllMatchIn(childStr.substring(0, childStr.indexOf("\\"))).mkString("")
      }else if (childStr.contains("/")){
        pattern.findAllMatchIn(childStr.substring(0, childStr.indexOf("/"))).mkString("")
      }else {
        pattern.findAllMatchIn(childStr).mkString("")
      }}
    realChildren.filterNot(child => child.isEmpty).toSet
  }

  def getChildrenFromHDFS(basePath: String, conf: Configuration): Set[String] ={
//    val conf = new Configuration()
//    conf.set("hadoop.job.ugi", "angel, angel")
//    conf.addResource("hdfs-site.xml")
//    conf.addResource("core-site.xml")
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

