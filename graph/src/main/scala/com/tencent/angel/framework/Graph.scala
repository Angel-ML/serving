package com.tencent.angel.framework

import java.util

import com.tencent.angel.framework.contexts._
import com.tencent.angel.utils.ProtoUtils

import scala.collection.JavaConverters._

class Graph private() {
  // data (nodes)  -------------------------------------------------------------------------------------------
  val opsMap: util.HashMap[String, Operation] = new util.HashMap[String, Operation]()
  val collections: util.HashMap[String, util.ArrayList[Operation]] =
    new util.HashMap[String, util.ArrayList[Operation]]()


  // four contexts -------------------------------------------------------------------------------------------

  // (1/4) nameCtx
  private val nameCtx = new NameContext()

  def getName(name: String, reuse: Boolean = false): String = nameCtx.getName(name, reuse)

  def withNameScope(name: String)(func: () => Unit): Unit = nameCtx.withNameScope(name)(func)

  // (2/4) devCtx
  private val devCtx = new DeviceContext()

  def withDevice(devDesc: DeviceDesc)(func: () => Unit): Unit = devCtx.withDevice(devDesc)(func)

  def withDevice(devDescStr: String)(func: () => Unit): Unit = devCtx.withDevice(devDescStr)(func)

  def withColocation(op: Operation, ignoreExisting: Boolean = false)(func: () => Unit): Unit = {
    devCtx.withColocation(op, ignoreExisting)(func)
  }

  // (3/4) ctrlCtx
  private val ctrlCtx = new ControlContext(this)

  def withControlDependencies(ctrlOps: Operation*)(func: () => Unit): Unit = ctrlCtx.withControlDependencies(ctrlOps)(func)

  def inferCtrlDependencies(inputOps: Operation*): List[Operation] = ctrlCtx.inferCtrlDependencies(inputOps)

  // (4/4) cntrCtx
  private val cntrCtx = new ContainerContext()

  def withContainer(name:String)(func: () => Unit): Unit = cntrCtx.withContainer(name)(func)

  //-------------------------------------------------------------------------------------------------------
  private var finalized: Boolean = false
  private var version: Int = 0

  def nextOpId(): Int = {
    if (!finalized) {
      version += 1
      version
    } else {
      throw new Exception("the graph has finalized!")
    }
  }

  def graphDef: GraphDef = {
    val nodes: List[NodeDef] = opsMap.values().asScala.toList.map { op => op.nodeDef }
    val versionDef: VersionDef = ProtoUtils.getVersionDef(version, version)

    ProtoUtils.getGraphDef(nodes, versionDef, null)
  }

  def createOp(nodeDef: NodeDef, opDef: OpDef, inputs: List[Tensor], controlInputs: List[Operation],
               outputTypes: List[DataType]): Operation = {
    if (!finalized) {
      val opName = nodeDef.getName
      val inputTypes: List[DataType] = inputs.map(ipts => ipts.dtype)
      val op = new Operation(opName, nodeDef, opDef, inputs, inputTypes, controlInputs, outputTypes, this)

      // post process: shape, device, control_dependencies, container

      // 1. shape

      // 2. device
      devCtx.applyDevice(op)
      if (devCtx.hasColocation) {
        devCtx.applyColocation(op)
      }

      // 3. control dependencies
      ctrlCtx.markAsSeen(op)

      // 4. container
      cntrCtx.applyContainer(op)

      op
    } else {
      throw new Exception("the graph has finalized!")
    }
  }

  def isFinalized: Boolean = finalized

  def setFinalized(finalize: Boolean): Unit = {
    finalized = finalize
  }

  def addOp(op: Operation): Unit = {
    opsMap.put(op.name, op)
  }

  def getOp(name: String): Operation = {
    opsMap.getOrDefault(name, null)
  }

  def getAllOps: util.Collection[Operation] = {
    opsMap.values()
  }

  def isElement(op: Operation): Boolean = opsMap.containsKey(op.name)

  def getTensor(name: String): Tensor = {
    val temp = name.split(":")
    val opName = temp(0)
    val index = temp(1).toInt

    opsMap.get(opName).outputs(index)
  }

  def isElement(ts: Tensor): Boolean = opsMap.containsKey(ts.op.name)

}


object Graph {
  private val default: Graph = new Graph()
  private val gStack: util.Stack[Graph] = new util.Stack[Graph]()

  gStack.push(default)

  def getGraph: Graph = gStack.peek()

  def withGraph(func: () => Unit): Unit = {
    val graph = new Graph()
    gStack.push(graph)
    try {
      func()
    } finally {
      gStack.pop()
    }
  }

  def withDefaultGraph(func: () => Unit): Unit = {
    if (getGraph == default) {
      func()
    } else {
      gStack.push(default)
      try {
        func()
      } finally {
        gStack.pop()
      }
    }
  }

}


