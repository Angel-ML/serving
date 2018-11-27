package com.tencent.angel.serving.service

import com.tencent.angel.serving.apis.common.ModelSpecProtos.ModelSpec
import com.tencent.angel.serving.apis.modelmgr.GetModelStatusProtos.{GetModelStatusRequest, GetModelStatusResponse, ModelVersionStatus}
import com.tencent.angel.serving.core.ManagerState.ManagerState
import com.tencent.angel.serving.core.{ManagerState, ServableId, ServableState, ServerCore}

object GetModelStatusImpl {

  def managerStateToStateProtoEnum(managerState: ManagerState): ModelVersionStatus.State = {
    managerState match {
      case ManagerState.kStart => ModelVersionStatus.State.START
      case ManagerState.kLoading => ModelVersionStatus.State.LOADING
      case ManagerState.kAvailable => ModelVersionStatus.State.AVAILABLE
      case ManagerState.kUnloading => ModelVersionStatus.State.UNLOADING
      case ManagerState.kEnd => ModelVersionStatus.State.END
    }
  }

  def addModelVersionStatusToResponse(version: Long, servableState: ServableState,
                                      builder: GetModelStatusResponse.Builder): Unit = {
    builder
      .addModelVersionStatus(ModelVersionStatus.newBuilder().setVersion(version).setState(managerStateToStateProtoEnum(servableState.managerState)))
  }

  def getModelStatus(core: ServerCore, request: GetModelStatusRequest,
                     builder: GetModelStatusResponse.Builder): Unit = {
    if(!request.hasModelSpec) {
      System.err.print("Missing ModelSpec")
      return
    }
    getModelStatusWithModelSpec(core, request.getModelSpec, request, builder)
  }

  def getModelStatusWithModelSpec(core: ServerCore, modelSpec: ModelSpec, request: GetModelStatusRequest,
                                  builder: GetModelStatusResponse.Builder): Unit ={
    val modelName = modelSpec.getName
    val monitor = core.getServableStateMonitor
    if(modelSpec.getVersion.getValue.isInstanceOf[Long]) {
      val version = modelSpec.getVersion.getValue
      val id = ServableId(modelName, version)
      val servableState = monitor.getState(id)
      if(servableState.isEmpty) {
        System.out.print("Could not find version " + version + " of model " + modelName)
        return
      }
      addModelVersionStatusToResponse(version, servableState.get, builder)
    } else {
      val versionsAndStates = monitor.getVersionStates(modelName)
      if(versionsAndStates.isEmpty) {
        System.out.print("Could not find any versions of model " + modelName)
        return
      }
      for((version, servableState) <- versionsAndStates) {
        addModelVersionStatusToResponse(version, servableState.state, builder)
      }
    }
  }
}
