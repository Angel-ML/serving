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
    val listFiles = FileUtils.listFilesAndDirs(new File(base_path), TrueFileFilter.INSTANCE, TrueFileFilter.TRUE)
    val realChildren = listFiles.asScala.filter(child => child.getAbsoluteFile.isDirectory).map(
      child => child.getPath.substring(base_path.length-1, child.getPath.length)).filter(child => child.length>1).map{child =>
      val childStr = child.substring(1, child.length)
      if (childStr.contains("\\")){
        childStr.substring(0, childStr.indexOf("\\"))
      }else {
        childStr
      }}
    realChildren.toSet
  }
}
