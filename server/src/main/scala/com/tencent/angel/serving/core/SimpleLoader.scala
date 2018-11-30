package com.tencent.angel.serving.core
import com.tencent.angel.config.{Resource, ResourceAllocation}
import scala.collection.mutable

class SimpleLoader[ServableType](creator: Unit => ServableType, resourceEstimate: Unit => ResourceAllocation
                                ) extends Loader {

  private var postLoadResourceEstimate: Unit => ResourceAllocation = null
  private val creator_ : Unit => ServableType = creator
  private val resourceEstimate_ : Unit => ResourceAllocation = resourceEstimate
  private var memorizedResourceEstimator: ResourceAllocation = null
  private var resourceUtil: ResourceUtil = null
  private var ramResource: Resource = null
  private var servable_ : ServableType = null.asInstanceOf[ServableType]


  override def estimateResources(): ResourceAllocation = ???

  override def load(): Unit = {
    servable_ = creator_()
    if (postLoadResourceEstimate != null){
      var duringLoadResourceEstimate: ResourceAllocation = estimateResources()
      memorizedResourceEstimator = postLoadResourceEstimate()
      //todo: Release any transient memory used only during load to the OS
    }
  }

  override def unload(): Unit = {
    val resourceEstimate = estimateResources()
    servable_ = null.asInstanceOf[ServableType]
    //todo: release resource
  }

  override def servable(): Any = {servable_}
}

object SimpleLoader{
  def apply[ServableType](creator: Unit => ServableType, resourceEstimate: Unit => ResourceAllocation
                         ): SimpleLoader[ServableType] ={
    val loader = new SimpleLoader[ServableType](creator, resourceEstimate)
    val resourceOptions = ResourceOptions(mutable.Map(DeviceType.kMain -> 1))
    loader.resourceUtil = new ResourceUtil(resourceOptions)
    //todo: ramResource
    loader
  }

  def apply[ServableType](creator: Unit => ServableType, resourceEstimate: Unit => ResourceAllocation,
            postLoadResourceEstimate: Unit => ResourceAllocation): SimpleLoader[ServableType] ={
    val loader = SimpleLoader(creator, resourceEstimate)
    loader.postLoadResourceEstimate = postLoadResourceEstimate
    loader
  }
}
