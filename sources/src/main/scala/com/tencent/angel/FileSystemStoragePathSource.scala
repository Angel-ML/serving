package com.tencent.angel


import java.util
import java.util.{ArrayList, List}

import com.tencent.angel.ServableId
import com.tencent.angel.StoragePath.StoragePath
import com.tencent.angel.config.FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig
import com.tencent.angel.config.FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig._
import org.apache.commons.io._
import org.apache.commons.logging.LogFactory


object FileSystemStoragePathSource extends Source[StoragePath]{

}

class FileSystemStoragePathSource{
  val LOG = LogFactory.getLog(classOf[FileSystemStoragePathSource])


  def PollFileSystemForConfig (config: FileSystemStoragePathSourceConfig,
                               versions_by_servable_name: Map[String, ArrayList[ServableData[StoragePath]]]): Boolean ={
    val servables = config.getServablesList
    (0 until servables.size()).foreach{ i =>
      val servable: ServableToMonitor = servables.get(i)
      val versions: ArrayList[ServableData[StoragePath]] = new ArrayList[ServableData[StoragePath]]()
      pollFileSystemForServable(servable, versions)
//      versions_by_servable_name += (servable.getServableName->versions)
    }



    true
  }


  def pollFileSystemForServable (servable:ServableToMonitor, versions: ArrayList[ServableData[StoragePath]]): Boolean ={
    if (!SystemFileUtils.fileExist(servable.getBasePath)){
      throw new Exception(s"Could not find base path ${servable.getBasePath} for servable ${servable.getServableName()}")
    }

    //Retrieve a list of base-path children from the file system.
    val children: ArrayList[String] = new  ArrayList[String]()
    if (!SystemFileUtils.getChildren(servable.getBasePath, children)){
      throw new Exception("The base path "+ servable.getBasePath + " for servable " +
        servable.getServableName() + "has not children")
    }

    //real children
    val real_children: ArrayList[String] = new  ArrayList[String]()
    (0 until children.size()).foreach{ i =>
      real_children.add(children.get(i).substring(0, children.get(i).indexOf("/")))
    }
    val children_by_version: Map[Long, String] = indexChildrenByVersion(real_children)

    var at_last_one_version_found: Boolean = true
    servable.getServableVersionPolicy.getPolicyChoiceCase.getNumber match {
      case x if (x==100) =>at_last_one_version_found = AspireLastestVersions(servable, children_by_version, versions)
      case x if (x==101) =>at_last_one_version_found = AspireAllVersions(servable, children, versions)
      case x if (x==102) =>at_last_one_version_found = AspireSpecificVersions(servable, children_by_version, versions)
      case x if (x==0) => null
    }
    if (!at_last_one_version_found){
      LOG.warn(s"No versions of servable  ${servable.getServableName} found under base path ${servable.getBasePath}")
    }
    true
  }


  def AspireLastestVersions(servable:ServableToMonitor, children_by_version: Map[Long, String],
                            versions: ArrayList[ServableData[StoragePath]]): Boolean ={
    val num_servable_versions_to_serve:Long = math.max(servable.getServableVersionPolicy.getLatest.getNumVersions,0)
    var num_versions_emitted = 0
    children_by_version.foreach{ case (version_number:Long, child:String)=>
        if (num_versions_emitted < num_servable_versions_to_serve){
          AspireVersions(servable, child, version_number, versions)
          num_versions_emitted +=1
        }
    }
    children_by_version.isEmpty
  }

  def AspireAllVersions(servable:ServableToMonitor, children: ArrayList[String],
                        versions: ArrayList[ServableData[StoragePath]]): Boolean ={
    var at_least_one_version_found: Boolean = false
    (0 until children.size()).foreach{ i =>
      val child =children.get(i)
      val version_number: Long = parseVersionNumber(child)
      if (version_number > 0){
        AspireVersions(servable, child, version_number, versions)
        at_least_one_version_found = true
      }
    }
    at_least_one_version_found
  }

  def AspireSpecificVersions(servable:ServableToMonitor, children_by_version: Map[Long, String],
                             versions: ArrayList[ServableData[StoragePath]]): Boolean ={
    val versions_to_serve = servable.getServableVersionPolicy.getSpecific.getVersionsList
    val aspired_versions: util.HashSet[Long] =  new util.HashSet[Long]()
    children_by_version.foreach{ case (version_number:Long, child:String)=>
        if (versions_to_serve.contains(version_number)){
          AspireVersions(servable, child, version_number, versions)
          aspired_versions.add(version_number)
        }
    }
    (0 until versions_to_serve.size()).foreach{ i =>
      val version =versions_to_serve.get(i)
      if (!aspired_versions.contains(version)){
        LOG.warn(s"version ${version} of servable ${servable.getServableName} , which was requested to be served as " +
          s"a specific version in the servable's version policy, was not found in the system.")
      }
    }
    !aspired_versions.isEmpty
  }

  def AspireVersions(servable:ServableToMonitor, version_relative_path: String, version_number: Long,
                     versions: ArrayList[ServableData[StoragePath]]): Unit ={
    val servable_id = new ServableId(servable.getServableName, version_number)
    val full_path: String = FilenameUtils.concat(servable.getBasePath, version_relative_path)
    versions.add(new ServableData[StoragePath](servable_id, full_path))
  }

  def indexChildrenByVersion(children: ArrayList[String]): Map[Long, String] ={
    var children_by_version: Map[Long, String] = null
    (0 until children.size()).foreach{ i =>
      val child =children.get(i)
      val version_number = parseVersionNumber(child)
      if (version_number >= 0){
        children_by_version += (version_number -> child)
      }
    }
    children_by_version
  }

  def parseVersionNumber(version: String): Long ={
    try{
      version.toLong
    }catch {
      case e:Exception => return -1
    }
    version.toLong
  }
}
