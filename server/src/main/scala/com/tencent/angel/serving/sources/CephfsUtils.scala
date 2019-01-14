package com.tencent.angel.serving.sources

import com.ceph.fs.{CephMount, CephStat}
import scala.util.matching.Regex

object CephfsUtils{
  private var mount = null.asInstanceOf[CephMount]

   def getMountCephfsByRoot(basePath: String): CephMount = {
     if (mount == null){
       mount.synchronized {
         if (mount == null){
           try {
             mount = new CephMount("admin")
             mount.conf_read_file(basePath)
             //      mount.conf_set("mon_host", "192.168.0.44;192.168.0.45;192.168.0.46")
             //      mount.conf_set("key", "AQCxGlBbNBRBGBAAAYW6tv/Z8x2Dz1mnCxwW9w==")
             mount.mount("/")
           } catch {
             case e: Exception =>
               e.printStackTrace()
           }
         }
       }
     }
    mount
  }




  def dirExist(basePath: String): Boolean ={
    val mount = getMountCephfsByRoot(basePath)
    val stat = new CephStat()
    mount.stat(basePath, stat)
    stat.isDir
  }

  def fileExist(basePath: String): Boolean ={
    val mount = getMountCephfsByRoot(basePath)
    val stat = new CephStat()
    mount.stat(basePath, stat)
    stat.isFile
  }

  def getTotalSpace(basePath: String): Long ={
    val mount = getMountCephfsByRoot(basePath)
    val stat = new CephStat()
    mount.stat(basePath, stat)
    stat.size
  }

  def getChildren(basePath: String): Set[String] ={
    val mount = getMountCephfsByRoot(basePath)
    val pattern = new Regex("^[0-9]*$")
    val children: Set[String] = mount.listdir(basePath).filter(fs =>
      dirExist(fs)).filterNot(fs => fs.equals(basePath)).map{ fs =>
      pattern.findAllMatchIn(fs).mkString("")
    }.filterNot(child => child.isEmpty).toSet
    children
  }


   def createDirByPath(path: String): Array[String] = {
    var dirList: Array[String] = null
    try {
      if (mount == null) return null
      mount.mkdirs(path, 7777)
      dirList = mount.listdir("/")
      return dirList
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    null
  }
}
