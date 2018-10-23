package com.tencent.angel.framework.contexts

import java.util

import com.tencent.angel.framework.Operation

import scala.util.matching.Regex

class DeviceDesc(val job: Option[String] = None, val replica: Option[Int] = None, val task: Option[Int] = None,
                 val deviceType: Option[String] = None, val deviceIndex: Option[Int] = None) {

  override def toString: String = {
    val sb = new StringBuilder
    job.map(x => sb.append(s"/job:$x"))
    replica.map(x => sb.append(s"/replica:$x"))
    task.map(x => sb.append(s"/task:$x"))
    deviceType.map(x => sb.append(s"/device:${x.toUpperCase}"))
    deviceIndex.map(x => sb.append(s":$x"))
    sb.toString()
  }

  def mergeFrom(dev: DeviceDesc): DeviceDesc = {
    new DeviceDesc(
      job = if (job.nonEmpty) job else dev.job,
      replica = if (replica.nonEmpty) replica else dev.replica,
      task = if (task.nonEmpty) task else dev.task,
      deviceType = if (deviceType.nonEmpty) deviceType else dev.deviceType,
      deviceIndex = if (deviceIndex.nonEmpty) deviceIndex else dev.deviceIndex
    )
  }

  def isEmpty: Boolean = {
    job.isEmpty && replica.isEmpty && task.isEmpty && deviceType.isEmpty && deviceIndex.isEmpty
  }

  def nonEmpty: Boolean = !isEmpty

}

object DeviceDesc {
  private val JOB: Regex = "job:(\\w+)".r
  private val REPLICA: Regex = "replica:(\\d+)".r
  private val TASK: Regex = "task:(\\d+)".r
  private val DEVICE1: Regex = "device:(\\w+)".r
  private val DEVICE2: Regex = "device:(\\w+):(\\d+)".r

  def parseFromString(devDesc: String): DeviceDesc = {
    if (devDesc.nonEmpty) {
      val map = new util.HashMap[String, Any]()
      devDesc.substring(1).split("/").foreach {
        case JOB(name) => map.put("job", Some(name))
        case REPLICA(idx) => map.put("replica", Some(idx.toInt))
        case TASK(idx) => map.put("task", Some(idx.toInt))
        case DEVICE1(name) => map.put("deviceType", Some(name))
        case DEVICE2(name, idx) =>
          map.put("deviceType", Some(name))
          map.put("deviceIndex", Some(idx.toInt))
        case _ => throw new Exception("Parse Error, pls. check the device format!")
      }

      new DeviceDesc(
        if (map.containsKey("job")) map.get("job").asInstanceOf[Option[String]] else None,
        if (map.containsKey("replica")) map.get("replica").asInstanceOf[Option[Int]] else None,
        if (map.containsKey("task")) map.get("task").asInstanceOf[Option[Int]] else None,
        if (map.containsKey("deviceType")) map.get("deviceType").asInstanceOf[Option[String]] else None,
        if (map.containsKey("deviceIndex")) map.get("deviceIndex").asInstanceOf[Option[Int]] else None
      )
    } else {
      new DeviceDesc()
    }
  }
}


class DeviceContext {
  private var devStack: util.Stack[DeviceDesc] = new util.Stack[DeviceDesc]()
  private var colocationList: util.ArrayList[Operation] = new util.ArrayList[Operation]()
  devStack.push(new DeviceDesc)

  def applyDevice(op: Operation): Unit = {
    val peek = devStack.peek()
    val device = if (peek == null || peek.isEmpty) {
      ""
    } else {
      peek.toString
    }

    op.device = device
  }

  def withDevice(devDesc: DeviceDesc)(func: () => Unit): Unit = {
    if (devDesc != null && devDesc.nonEmpty) {
      val peek = devStack.peek()
      if (peek != null && peek.nonEmpty) {
        devStack.push(devDesc.mergeFrom(peek))
      } else {
        devStack.push(devDesc)
      }
    } else {
      devStack.push(devDesc)
    }

    try {
      func()
    } finally {
      devStack.pop()
    }
  }

  def withDevice(devDescStr: String)(func: () => Unit): Unit = {
    if (devDescStr != null && devDescStr.nonEmpty) {
      val peek = devStack.peek()
      val devDesc = DeviceDesc.parseFromString(devDescStr)
      if (peek != null && peek.nonEmpty) {
        devStack.push(devDesc.mergeFrom(peek))
      } else {
        devStack.push(devDesc)
      }
    } else {
      if (devDescStr == null) {
        devStack.push(null.asInstanceOf[DeviceDesc])
      } else {
        devStack.push(new DeviceDesc)
      }
    }

    try {
      func()
    } finally {
      devStack.pop()
    }
  }

  def hasColocation: Boolean = !colocationList.isEmpty

  def withColocation(op: Operation, ignoreExisting: Boolean = false)(func: () => Unit): Unit = {
    val oldColocationList = colocationList
    if (ignoreExisting) {
      colocationList = new util.ArrayList[Operation]()
    }
    if (op != null) {
      colocationList.add(op)
    }

    val oldDevStack: util.Stack[DeviceDesc] = devStack
    devStack = new util.Stack[DeviceDesc]()
    devStack.push(new DeviceDesc)

    try {
      func()
    } finally {
      devStack = oldDevStack

      if (op != null) {
        colocationList.remove(colocationList.size() - 1)
      }

      if (ignoreExisting) {
        colocationList = oldColocationList
      }
    }
  }

  def applyColocation(op: Operation): Unit = {
    val allColocationGroups = new util.HashSet[String]()
    (0 until colocationList.size()).foreach { i =>
      val iop = colocationList.get(i)
      iop.colocationGroups.foreach {
        loc => allColocationGroups.add(loc)
      }

      if (iop.device != null && iop.device.nonEmpty) {
        op.device = iop.device
      }
    }

    val iter = allColocationGroups.iterator()
    val colocationGroups = (0 until allColocationGroups.size()).toList.map { _ => iter.next() }.sorted
    op.setColocation(colocationGroups)
  }

}

