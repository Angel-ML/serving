package com.tencent.angel.config


import com.tencent.angel.config.FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableVersionPolicy.PolicyChoiceCase

import scala.collection.mutable.ListBuffer

case class Latest(numVersions: Int) {
  def toProto : FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableVersionPolicy.Latest ={
    val builder = FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableVersionPolicy.Latest.newBuilder()
    builder.setNumVersions(numVersions)
    builder.build()
  }

  override def toString: String = toProto.toString

  override def equals(obj: scala.Any): Boolean = this.equals(obj)

  def ==(other: Latest): Boolean = this.numVersions == other.numVersions

  def !=(other: Latest): Boolean = !(this == other)
}

object Latest{
  def apply(latest: FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableVersionPolicy.Latest): Latest = {
    Latest(latest.getNumVersions)
  }

  def apply(latest: String): Latest = {
    val latestProto = FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableVersionPolicy
      .Latest.parseFrom(latest.getBytes)
    Latest(latestProto.getNumVersions)
  }

}

class All{
  def toProto : FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableVersionPolicy.All ={
    val builder = FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableVersionPolicy.All.newBuilder()
    builder.build()
  }

  override def toString: String = toProto.toString

  override def equals(obj: scala.Any): Boolean = this.equals(obj)

  def ==(other: All): Boolean = this.equals(other)

  def !=(other: All): Boolean = !(this == other)
}

object All{
  def apply(): All = new All()
}

case class Specific(versions: List[Long]){
  def toProto : FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableVersionPolicy.Specific ={
    val builder = FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableVersionPolicy.Specific.newBuilder()
    (0 until versions.size).foreach( i =>
      builder.setVersions(i, versions(i))
    )
    builder.build()
  }

  override def toString: String = toProto.toString

  override def equals(obj: scala.Any): Boolean = this.equals(obj)

  def ==(other: Specific): Boolean = this.equals(other)

  def !=(other: Specific): Boolean = !(this == other)
}

object Specific{
  def apply(specific: FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableVersionPolicy.Specific): Specific = {
    val versions = List[Long]()
    val it = specific.getVersionsList.iterator()
    while (it.hasNext){
      versions :+ it.next()
    }
   Specific(versions)
  }

  def apply(specific: String): Specific = {
    val specificProto = FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableVersionPolicy
      .Specific.parseFrom(specific.getBytes)
    val versions = List[Long]()
    val it = specificProto.getVersionsList.iterator()
    while (it.hasNext){
      versions :+ it.next()
    }
    Specific(versions)
  }
}



sealed trait ServableVersionPolicy {
  def toProto: FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableVersionPolicy
}

case class LatestPolicy(latest: Latest) extends ServableVersionPolicy {
  override def toProto: FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableVersionPolicy = {
    val builder = FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableVersionPolicy.newBuilder()
    builder.setLatest(latest.toProto)
    builder.build()
  }

  override def toString: String = toProto.toString

  override def equals(obj: scala.Any): Boolean = this.equals(obj)

  def ==(other: LatestPolicy): Boolean = this.latest == other.latest

  def !=(other: LatestPolicy): Boolean = !(this == other)
}


case class AllPolicy(all: All) extends ServableVersionPolicy {
  override def toProto: FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableVersionPolicy = {
    val builder = FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableVersionPolicy.newBuilder()
    builder.setAll(all.toProto)
    builder.build()
  }

  override def toString: String = toProto.toString

  override def equals(obj: scala.Any): Boolean = this.equals(obj)

  def ==(other: AllPolicy): Boolean = this.all == other.all

  def !=(other: AllPolicy): Boolean = !(this == other)
}

case class SpecificPolicy(specific: Specific) extends ServableVersionPolicy {
  override def toProto: FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableVersionPolicy = {
    val builder = FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableVersionPolicy.newBuilder()
    builder.setSpecific(specific.toProto)
    builder.build()
  }

  override def toString: String = toProto.toString

  override def equals(obj: scala.Any): Boolean = this.equals(obj)

  def ==(other: SpecificPolicy): Boolean = this.specific == other.specific

  def !=(other: SpecificPolicy): Boolean = !(this == other)
}

object ServableVersionPolicy {
  def apply(latest: Latest): ServableVersionPolicy = new LatestPolicy(latest)
  def apply(all: All): ServableVersionPolicy = new AllPolicy(all)
  def apply(specific: Specific): ServableVersionPolicy = new SpecificPolicy(specific)

  def apply(servableVersionPolicy: FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableVersionPolicy
           ): ServableVersionPolicy = {
    servableVersionPolicy.getPolicyChoiceCase match {
      case PolicyChoiceCase.LATEST => LatestPolicy(Latest(servableVersionPolicy.getLatest))
      case PolicyChoiceCase.ALL => AllPolicy(All())
      case PolicyChoiceCase.SPECIFIC => SpecificPolicy(Specific(servableVersionPolicy.getSpecific))
      case PolicyChoiceCase.POLICYCHOICE_NOT_SET => LatestPolicy(Latest(servableVersionPolicy.getLatest))
    }
  }

  def apply(servableVersionPolicy: String): ServableVersionPolicy = {
    val servableVersionPolicyProto = FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig
      .ServableVersionPolicy.parseFrom(servableVersionPolicy.getBytes)
    servableVersionPolicyProto.getPolicyChoiceCase match {
      case PolicyChoiceCase.LATEST => LatestPolicy(Latest(servableVersionPolicyProto.getLatest))
      case PolicyChoiceCase.ALL => AllPolicy(All())
      case PolicyChoiceCase.SPECIFIC => SpecificPolicy(Specific(servableVersionPolicyProto.getSpecific))
      case PolicyChoiceCase.POLICYCHOICE_NOT_SET => LatestPolicy(Latest(servableVersionPolicyProto.getLatest))
    }
  }

}

case class ServableToMonitor(servableName: String, basePath: String, servableVersionPolicy: ServableVersionPolicy) {
  override def toString: String = toProto.toString

  def toProto: FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableToMonitor = {
    val builder = FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableToMonitor.newBuilder()
    builder.setServableName(servableName)
    builder.setBasePath(basePath)
    builder.setServableVersionPolicy(servableVersionPolicy.toProto)
    builder.build()
  }

  override def equals(obj: scala.Any): Boolean = this.equals(obj)

  def ==(other: ServableToMonitor): Boolean = this.servableName == other.servableName &&
    this.basePath == other.basePath && this.servableVersionPolicy == other.servableVersionPolicy

  def !=(other: ServableToMonitor): Boolean = !(this == other)
}

object ServableToMonitor {
  def apply(servableToMonitor: FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.ServableToMonitor
           ): ServableToMonitor = {
    ServableToMonitor(servableToMonitor.getServableName, servableToMonitor.getBasePath,
      ServableVersionPolicy(servableToMonitor.getServableVersionPolicy))
  }

  def apply(servableToMonitor: String): ServableToMonitor = {
    val servableToMonitorProto = FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.
      ServableToMonitor.parseFrom(servableToMonitor.getBytes)
    ServableToMonitor(servableToMonitorProto.getServableName, servableToMonitorProto.getBasePath,
      ServableVersionPolicy(servableToMonitorProto.getServableVersionPolicy))
  }
}


case class FileSystemStoragePathSourceConfig(servables: List[ServableToMonitor], fileSystemPollWaitSeconds: Long,
                                        failIfZeroVersionsAtStartup: Boolean) {

  override def toString: String = toProto.toString

  def toProto: FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig = {
    val builder = FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.newBuilder()
    servables.foreach(servable =>
      builder.addServables(servable.toProto)
    )
    builder.setFileSystemPollWaitSeconds(fileSystemPollWaitSeconds)
    builder.setFailIfZeroVersionsAtStartup(failIfZeroVersionsAtStartup)
    builder.build()
  }

  override def equals(obj: scala.Any): Boolean = this.equals(obj)

  def ==(other: FileSystemStoragePathSourceConfig): Boolean = this.servables == other.servables &&
    this.fileSystemPollWaitSeconds == other.fileSystemPollWaitSeconds &&
    this.failIfZeroVersionsAtStartup == other.failIfZeroVersionsAtStartup

  def !=(other: FileSystemStoragePathSourceConfig): Boolean = !(this == other)
}

object FileSystemStoragePathSourceConfig {
  def apply(fileSystemStoragePathSourceConfig: FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig)
  : FileSystemStoragePathSourceConfig = {
    val servablesList = ListBuffer[ServableToMonitor]()
    val it = fileSystemStoragePathSourceConfig.getServablesList.iterator()
    while (it.hasNext) {
      servablesList :+ ServableToMonitor(it.next())
    }
    FileSystemStoragePathSourceConfig(servablesList.toList, fileSystemStoragePathSourceConfig.getFileSystemPollWaitSeconds,
      fileSystemStoragePathSourceConfig.getFailIfZeroVersionsAtStartup)
  }

  def apply(fileSystemStoragePathSourceConfig: String): FileSystemStoragePathSourceConfig = {
    val pathSourceConfigProtos = FileSystemStoragePathSourceConfigProtos.FileSystemStoragePathSourceConfig.parseFrom(
      fileSystemStoragePathSourceConfig.getBytes)
    val servablesList = ListBuffer[ServableToMonitor]()
    val it = pathSourceConfigProtos.getServablesList.iterator()
    while (it.hasNext) {
      servablesList :+ ServableToMonitor(it.next())
    }
    FileSystemStoragePathSourceConfig(servablesList.toList, pathSourceConfigProtos.getFileSystemPollWaitSeconds,
      pathSourceConfigProtos.getFailIfZeroVersionsAtStartup)
  }
}
