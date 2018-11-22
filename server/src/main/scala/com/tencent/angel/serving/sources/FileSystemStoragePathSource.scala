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


object FileSystemStoragePathSource {

  var AspiredVersionsCallback_ : AspiredVersionsCallback[StoragePath] = null
  var timer: Timer = null


  def create(config: FileSystemStoragePathSourceConfig): FileSystemStoragePathSource = {
    val source: FileSystemStoragePathSource = new FileSystemStoragePathSource(config)
    source.updateConfig(config)
    source
  }
}

class FileSystemStoragePathSource(config: FileSystemStoragePathSourceConfig) extends Source[StoragePath] {

  import FileSystemStoragePathSource._
  var config_ : FileSystemStoragePathSourceConfig = config
  val LOG = LogFactory.getLog(classOf[FileSystemStoragePathSource])

  private val callbackLock = new ReentrantReadWriteLock()
  private val callbackReadLock: ReentrantReadWriteLock.ReadLock = callbackLock.readLock()
  private val callbackWriteLock: ReentrantReadWriteLock.WriteLock = callbackLock.writeLock()

  private val configLock = new ReentrantReadWriteLock()
  private val configReadLock: ReentrantReadWriteLock.ReadLock = configLock.readLock()
  private val configWriteLock: ReentrantReadWriteLock.WriteLock = configLock.writeLock()


  def updateConfig(config: FileSystemStoragePathSourceConfig): Unit = {
    configWriteLock.lock()
    try {
      if (timer != null && config.getFileSystemPollWaitSeconds != config_.getFileSystemPollWaitSeconds) {
        throw InvalidArguments("Changing file_system_poll_wait_seconds is not supported")
      }
      //normalize

      if (config.getFailIfZeroVersionsAtStartup) {
        try {
          failIfZeroVersions(config)
        } catch {
          case e: NotFoundExceptions => LOG(e)
        }
      }

      if (AspiredVersionsCallback_ != null) {
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
    val versionsByServableName: Map[String, List[ServableData[StoragePath]]] =
      pollFileSystemForConfig(config)
    versionsByServableName.foreach { case (servableName, versions) =>
      if (versions.isEmpty) {
        throw NotFoundExceptions(s"Unable to find a numerical version path for servable: ${servableName}, " +
          s"at:${config.getBasePath}")
      }
    }
  }

  def unAspireServables(servableNames: Set[String]): Unit = {
    servableNames.foreach { servableName =>
      AspiredVersionsCallback_(servableName, null)
    }
  }

  // Returns the names of servables that appear in 'old_config' but not in
  // 'new_config'. Assumes both configs are normalized.
  def getDeletedServables(oldConfig: FileSystemStoragePathSourceConfig, newConfig: FileSystemStoragePathSourceConfig
                         ): Set[String] = {
    val newServables: Set[String] = Set[String]()
    for ((servable: ServableToMonitor) <- newConfig.getServablesList) {
      newServables.+(servable.getServableName)
    }

    val deletedServables: Set[String] = Set[String]()
    for ((oldServable: ServableToMonitor) <- oldConfig.getServablesList) {
      if (!newServables.contains(oldServable.getServableName)) {
        deletedServables.+(oldServable.getServableName)
      }
    }
    deletedServables
  }

  override def setAspiredVersionsCallback(callback: AspiredVersionsCallback[StoragePath]): Unit = {
    callbackWriteLock.lock()
    try {
      if (timer != null) {
        LOG.info("SetAspiredVersionsCallback() called multiple times; ignoring this call")
        return
      }
      AspiredVersionsCallback_ = callback
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
      val versionsByServableName: Map[String, List[ServableData[StoragePath]]] =
        pollFileSystemForConfig(config_)
      versionsByServableName.foreach { case (servableName, versions) =>
        AspiredVersionsCallback_(servableName, versions)
      }
    } finally {
      configWriteLock.unlock()
    }
  }

  def pollFileSystemForConfig(config: FileSystemStoragePathSourceConfig):
                              Map[String, List[ServableData[StoragePath]]] = {
    var versionsByServableName: Map[String, List[ServableData[StoragePath]]] = null
    try {
      versionsByServableName = Map[String, List[ServableData[StoragePath]]]()
    } catch {
      case e: InvalidArguments => {LOG(e)}
    }

    val servables = config.getServablesList
    (0 until servables.size()).foreach { i =>
      val servable: ServableToMonitor = servables.get(i)
      val versions = pollFileSystemForServable(servable)
      versionsByServableName += (servable.getServableName -> versions)
    }
    versionsByServableName
  }

  def pollFileSystemForServable(servable: ServableToMonitor): List[ServableData[StoragePath]] = {
    if (!SystemFileUtils.fileExist(servable.getBasePath)) {
      throw new Exception(s"Could not find base path ${servable.getBasePath} for servable ${servable.getServableName()}")
    }

    //Retrieve a list of base-path children from the file system.
    val children: ArrayList[String] = new ArrayList[String]()
    if (!SystemFileUtils.getChildren(servable.getBasePath, children)) {
      throw InvalidArguments("The base path " + servable.getBasePath + " for servable " +
        servable.getServableName() + "has not children")
    }

    //real children
    val realChildren: ArrayList[String] = new ArrayList[String]()
    (0 until children.size()).foreach { i =>
      realChildren.add(children.get(i).substring(0, children.get(i).indexOf("/")))
    }
    children.clear()
    val childrenByVersion: Map[Long, String] = indexChildrenByVersion(realChildren)

    var versions: List[ServableData[StoragePath]] = null
    servable.getServableVersionPolicy.getPolicyChoiceCase match {
      //todo
      case x if (x.asInstanceOf[PolicyChoiceCase.LATEST.type]) =>
        versions = AspireLastestVersions(servable, childrenByVersion)
      case x if (x.asInstanceOf[PolicyChoiceCase.ALL.type]) =>
        versions = AspireAllVersions(servable, children)
      case x if (x.asInstanceOf[PolicyChoiceCase.SPECIFIC.type]) =>
        versions = AspireSpecificVersions(servable, childrenByVersion)
      case x if (x.asInstanceOf[PolicyChoiceCase.POLICYCHOICE_NOT_SET.type]) =>
        versions = AspireLastestVersions(servable, childrenByVersion)
    }
    versions
  }

  def AspireLastestVersions(servable: ServableToMonitor, childrenByVersion: Map[Long, String]
                            ): List[ServableData[StoragePath]]= {
    val numServableVersionsToServe: Long = math.max(servable.getServableVersionPolicy.getLatest.getNumVersions, 0)
    val versions: List[ServableData[StoragePath]] = List[ServableData[StoragePath]]()
    var numVersionsEmitted = 0
    if (childrenByVersion.isEmpty){
      LOG.warn(s"No versions of servable  ${servable.getServableName} found under base path ${servable.getBasePath}")
    } else {
      childrenByVersion.foreach { case (versionNumber: Long, child: String) =>
        if (numVersionsEmitted < numServableVersionsToServe) {
          versions.:+(AspireVersions(servable, child, versionNumber))
          numVersionsEmitted += 1
        }
      }
    }
    versions
  }

  def AspireVersions(servable: ServableToMonitor, versionRelativePath: String, versionNumber: Long
                    ): ServableData[StoragePath] = {
    val servableId = ServableId(servable.getServableName, versionNumber)
    val fullPath: StoragePath = FilenameUtils.concat(servable.getBasePath, versionRelativePath)
    ServableData[StoragePath](servableId, fullPath)
  }

  def AspireAllVersions(servable: ServableToMonitor, children: ArrayList[String]
                        ): List[ServableData[StoragePath]] = {
    val versions: List[ServableData[StoragePath]] = List[ServableData[StoragePath]]()
    var atLeastOneVersionFound: Boolean = false
    (0 until children.size()).foreach { i =>
      val child = children.get(i)
      val versionNumber: Long = parseVersionNumber(child)
      if (versionNumber > 0) {
        versions.:+(AspireVersions(servable, child, versionNumber))
        atLeastOneVersionFound = true
      }
    }
    if (!atLeastOneVersionFound){
      LOG.warn(s"No versions of servable  ${servable.getServableName} found under base path ${servable.getBasePath}")
    }
    versions
  }


  def AspireSpecificVersions(servable: ServableToMonitor, childrenByVersion: Map[Long, String]
                             ): List[ServableData[StoragePath]] = {
    val versions: List[ServableData[StoragePath]] = List[ServableData[StoragePath]]()
    val versionsToServe = servable.getServableVersionPolicy.getSpecific.getVersionsList
    val aspiredVersions: util.HashSet[Long] = new util.HashSet[Long]()
    childrenByVersion.foreach { case (version_number: Long, child: String) =>
      if (versionsToServe.contains(version_number)) {
        versions.:+(AspireVersions(servable, child, version_number))
        aspiredVersions.add(version_number)
      }
    }
    (0 until versionsToServe.size()).foreach { i =>
      val version = versionsToServe.get(i)
      if (!aspiredVersions.contains(version)) {
        LOG.warn(s"version ${version} of servable ${servable.getServableName} , which was requested to be served as " +
          s"a specific version in the servable's version policy, was not found in the system.")
      }
    }
    if (aspiredVersions.isEmpty){
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
    var childrenByVersion: Map[Long, String] = Map[Long, String]()
    (0 until children.size()).foreach { i =>
      val child = children.get(i)
      val versionNumber = parseVersionNumber(child)
      if (versionNumber >= 0) {
        childrenByVersion += (versionNumber -> child)
      }
    }
    childrenByVersion.toList.sortBy(_._1).toMap
  }
}
