package com.tencent.angel.serving.sources

import java.util
import java.util.{ArrayList, Timer, TimerTask}
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.tencent.angel.config.FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableToMonitor
import com.tencent.angel.config.FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableVersionPolicy.PolicyChoiceCase
import com.tencent.angel.serving.core._
import com.tencent.angel.serving.serving.FileSystemStoragePathSourceConfig
import org.apache.commons.io.FilenameUtils
import org.apache.commons.logging.LogFactory

import scala.collection.mutable

object FileSystemStoragePathSource {
  var config_ : FileSystemStoragePathSourceConfig = new FileSystemStoragePathSourceConfig()
  var aspired_versions_callback_ : AspiredVersionsCallback[StoragePath] = null
  var timer: Timer = null
}

class FileSystemStoragePathSource extends Source[StoragePath] {

  import FileSystemStoragePathSource._

  val LOG = LogFactory.getLog(classOf[FileSystemStoragePathSource])

  private val callbackLock = new ReentrantReadWriteLock()
  private val callbackReadLock: ReentrantReadWriteLock.ReadLock = callbackLock.readLock()
  private val callbackWriteLock: ReentrantReadWriteLock.WriteLock = callbackLock.writeLock()

  private val configLock = new ReentrantReadWriteLock()
  private val configReadLock: ReentrantReadWriteLock.ReadLock = configLock.readLock()
  private val configWriteLock: ReentrantReadWriteLock.WriteLock = configLock.writeLock()


  def create(config: FileSystemStoragePathSourceConfig): FileSystemStoragePathSource = {
    val source: FileSystemStoragePathSource = new FileSystemStoragePathSource()
    source.updateConfig(config)
    source
  }

  def updateConfig(config: FileSystemStoragePathSourceConfig): Unit = {
    configWriteLock.lock()
    try {
      if (timer != null && config.getFileSystemPollWaitSeconds != config_.getFileSystemPollWaitSeconds) {
        throw new Exception("Changing file_system_poll_wait_seconds is not supported")
      }
      //normalize

      if (config.getFailIfZeroVersionsAtStartup) {
        failIfZeroVersions(config)
      }

      if (aspired_versions_callback_ != null) {
        unAspireServables(getDeletedServables(config_, config))
      }
      config_ = config
    } finally {
      configWriteLock.unlock()
    }
  }

  // Determines if, for any servables in 'config', the file system doesn't
  // currently contain at least one version under its base path.
  def failIfZeroVersions(config: FileSystemStoragePathSourceConfig): Unit = {
    val versions_by_servable_name: Map[String, List[ServableData[StoragePath]]] =
      pollFileSystemForConfig(config)
    versions_by_servable_name.foreach { case (servable_name, versions) =>
      if (versions.isEmpty) {
        throw new Exception(s"Unable to find a numerical version path for servable: ${servable_name}, " +
          s"at:${config.getBasePath}")
      }
    }
  }

  def unAspireServables(servable_names: Set[String]): Unit = {
    servable_names.foreach { servable_name =>
      aspired_versions_callback_(servable_name, null)
    }
  }

  // Returns the names of servables that appear in 'old_config' but not in
  // 'new_config'. Assumes both configs are normalized.
  def getDeletedServables(old_config: FileSystemStoragePathSourceConfig, new_config: FileSystemStoragePathSourceConfig
                         ): Set[String] = {
    val new_servables: Set[String] = Set[String]()
    for ((servable: ServableToMonitor) <- new_config.getServablesList) {
      new_servables.+(servable.getServableName)
    }

    val deleted_servables: Set[String] = Set[String]()
    for ((old_servable: ServableToMonitor) <- old_config.getServablesList) {
      if (!new_servables.contains(old_servable.getServableName)) {
        deleted_servables.+(old_servable.getServableName)
      }
    }
    deleted_servables
  }

  override def setAspiredVersionsCallback(callback: AspiredVersionsCallback[StoragePath]): Unit = {
    callbackWriteLock.lock() //todo: lock
    try {
      if (timer != null) {
        LOG.error("SetAspiredVersionsCallback() called multiple times; ignoring this call") //TODO; error log
        return
      }
      aspired_versions_callback_ = callback
      timer = new Timer("FileSystemStoragePathSource_filesystem_polling_thread")
      timer.schedule(new TimerTask {
        override def run(): Unit = {
          configWriteLock.unlock()
          pollFileSystemAndInvokeCallback()
        }
      }, 0, config_.getFileSystemPollWaitSeconds * 1000)

    } finally {
      callbackWriteLock.unlock()
    }
  }

  def pollFileSystemAndInvokeCallback() = {
    configWriteLock.lock()
    try {
      val versions_by_servable_name: Map[String, List[ServableData[StoragePath]]] =
        pollFileSystemForConfig(config_)
      versions_by_servable_name.foreach { case (servable_name, versions) =>
        aspired_versions_callback_(servable_name, versions)
      }
    } finally {
      configWriteLock.unlock()
    }
  }

  def pollFileSystemForConfig(config: FileSystemStoragePathSourceConfig):
                              Map[String, List[ServableData[StoragePath]]] = {
    var versions_by_servable_name: Map[String, List[ServableData[StoragePath]]] =
      Map[String, List[ServableData[StoragePath]]]()
    val servables = config.getServablesList
    (0 until servables.size()).foreach { i =>
      val servable: ServableToMonitor = servables.get(i)
      val versions = pollFileSystemForServable(servable)
      versions_by_servable_name += (servable.getServableName -> versions)
    }
    versions_by_servable_name
  }

  def pollFileSystemForServable(servable: ServableToMonitor): List[ServableData[StoragePath]] = {
    if (!SystemFileUtils.fileExist(servable.getBasePath)) {
      throw new Exception(s"Could not find base path ${servable.getBasePath} for servable ${servable.getServableName()}")
    }

    //Retrieve a list of base-path children from the file system.
    val children: ArrayList[String] = new ArrayList[String]()
    if (!SystemFileUtils.getChildren(servable.getBasePath, children)) {
      throw new Exception("The base path " + servable.getBasePath + " for servable " +
        servable.getServableName() + "has not children")
    }

    //real children
    val real_children: ArrayList[String] = new ArrayList[String]()
    (0 until children.size()).foreach { i =>
      real_children.add(children.get(i).substring(0, children.get(i).indexOf("/")))
    }
    children.clear()
    val children_by_version: Map[Long, String] = indexChildrenByVersion(real_children)

    var versions: List[ServableData[StoragePath]] = null
    servable.getServableVersionPolicy.getPolicyChoiceCase match {
      //todo
      case x if (x.asInstanceOf[PolicyChoiceCase.LATEST.type]) =>
        versions = AspireLastestVersions(servable, children_by_version)
      case x if (x.asInstanceOf[PolicyChoiceCase.ALL.type]) =>
        versions = AspireAllVersions(servable, children)
      case x if (x.asInstanceOf[PolicyChoiceCase.SPECIFIC.type]) =>
        versions = AspireSpecificVersions(servable, children_by_version)
      case x if (x.asInstanceOf[PolicyChoiceCase.POLICYCHOICE_NOT_SET.type]) =>
        versions = AspireLastestVersions(servable, children_by_version)
    }
    versions
  }

  def AspireLastestVersions(servable: ServableToMonitor, children_by_version: Map[Long, String]
                            ): List[ServableData[StoragePath]]= {
    val num_servable_versions_to_serve: Long = math.max(servable.getServableVersionPolicy.getLatest.getNumVersions, 0)
    val versions: List[ServableData[StoragePath]] = List[ServableData[StoragePath]]()
    var num_versions_emitted = 0
    if (children_by_version.isEmpty){
      LOG.warn(s"No versions of servable  ${servable.getServableName} found under base path ${servable.getBasePath}")
    } else {
      //todo:
      children_by_version.foreach { case (version_number: Long, child: String) =>
        if (num_versions_emitted < num_servable_versions_to_serve) {
          versions.:+(AspireVersions(servable, child, version_number))
          num_versions_emitted += 1
        }
      }
    }
    versions
  }

  def AspireVersions(servable: ServableToMonitor, version_relative_path: String, version_number: Long
                    ): ServableData[StoragePath] = {
    val servable_id = ServableId(servable.getServableName, version_number)
    val full_path: StoragePath = FilenameUtils.concat(servable.getBasePath, version_relative_path)
    ServableData[StoragePath](servable_id, Status.OK, full_path)
  }

  def AspireAllVersions(servable: ServableToMonitor, children: ArrayList[String]
                        ): List[ServableData[StoragePath]] = {
    val versions: List[ServableData[StoragePath]] = List[ServableData[StoragePath]]()
    var at_least_one_version_found: Boolean = false
    (0 until children.size()).foreach { i =>
      val child = children.get(i)
      val version_number: Long = parseVersionNumber(child)
      if (version_number > 0) {
        versions.:+(AspireVersions(servable, child, version_number))
        at_least_one_version_found = true
      }
    }
    if (!at_least_one_version_found){
      LOG.warn(s"No versions of servable  ${servable.getServableName} found under base path ${servable.getBasePath}")
    }
    versions
  }


  def AspireSpecificVersions(servable: ServableToMonitor, children_by_version: Map[Long, String]
                             ): List[ServableData[StoragePath]] = {
    val versions: List[ServableData[StoragePath]] = List[ServableData[StoragePath]]()
    val versions_to_serve = servable.getServableVersionPolicy.getSpecific.getVersionsList
    val aspired_versions: util.HashSet[Long] = new util.HashSet[Long]()
    children_by_version.foreach { case (version_number: Long, child: String) =>
      if (versions_to_serve.contains(version_number)) {
        versions.:+(AspireVersions(servable, child, version_number))
        aspired_versions.add(version_number)
      }
    }
    (0 until versions_to_serve.size()).foreach { i =>
      val version = versions_to_serve.get(i)
      if (!aspired_versions.contains(version)) {
        LOG.warn(s"version ${version} of servable ${servable.getServableName} , which was requested to be served as " +
          s"a specific version in the servable's version policy, was not found in the system.")
      }
    }
    if (aspired_versions.isEmpty){
      LOG.warn(s"No versions of servable  ${servable.getServableName} found under base path ${servable.getBasePath}")
    }
    versions
  }

  def parseVersionNumber(version: String): Long = {
    try {
      version.toLong
    } catch {
      case e: Exception => return -1
    }
    version.toLong
  }

  def indexChildrenByVersion(children: ArrayList[String]): Map[Long, String] = {
    var children_by_version: Map[Long, String] = Map[Long, String]()
    (0 until children.size()).foreach { i =>
      val child = children.get(i)
      val version_number = parseVersionNumber(child)
      if (version_number >= 0) {
        children_by_version += (version_number -> child)
      }
    }
    children_by_version.toList.sortBy(_._1).toMap
  }
}
