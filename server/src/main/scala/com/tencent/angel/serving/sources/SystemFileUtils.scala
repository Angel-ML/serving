package com.tencent.angel.serving.sources

import java.io.File


import scala.collection.JavaConverters._
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.TrueFileFilter
import scala.collection.immutable.Set

object SystemFileUtils {
  def fileExist(base_path: String): Boolean ={
    val filePath = new File(base_path)
    filePath.isDirectory
  }

  def getChildren(base_path: String): Set[String] ={
    val f = new File(base_path)
    val len = f.getPath.length
    val listFiles = FileUtils.listFilesAndDirs(f, TrueFileFilter.INSTANCE, TrueFileFilter.TRUE)
    val realChildren = listFiles.asScala.filter(child => child.getAbsoluteFile.isDirectory).filterNot(
      child => child.getPath.equals(f.getPath)).map{child =>
      val childStr = child.getPath.substring(len + 1)
      if (childStr.contains("\\")){
        childStr.substring(0, childStr.indexOf("\\"))
      }else if (childStr.contains("/")){
        childStr.substring(0, childStr.indexOf("/"))
      }else {
        childStr
      }}
    realChildren.toSet
  }
}
