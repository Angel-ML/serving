package com.tencent.angel.serving.servables.angel

import com.tencent.angel.ml.core.graphsubmit.GraphModel
import com.tencent.angel.ml.core.network.graph.LocalEvnContext
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable

class LocalAngel {

  private val modeMap: mutable.HashMap[String, GraphModel] = mutable.HashMap[String, GraphModel]()
  val modelName = "com.tencent.angel.ml.core.graphsubmit.GraphModel"
  val model = GraphModel(modelName, new Configuration)
  model.buildNetwork()
  model.loadModel(LocalEvnContext(), "")

  model.predict()
}
