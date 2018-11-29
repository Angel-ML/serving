package com.tencent.angel.serving.sources

import java.io.File
import java.util._

import org.apache.commons.io.FileUtils

object SystemFileUtils {
  def fileExist(base_path: String): Boolean ={
    val filePath = new File(base_path)
    filePath.isDirectory
  }

  def getChildren(base_path: String): ArrayList[String] ={
    val children = new ArrayList[String]()
    val listFiles = FileUtils.listFiles(new File(base_path),null,false).iterator()
    while(listFiles.hasNext){
     children.add(listFiles.next().getName)
    }
    children
  }
}
