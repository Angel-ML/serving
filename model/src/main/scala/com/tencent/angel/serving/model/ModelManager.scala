package com.tencent.angel.serving.model

import scala.collection.mutable

class ModelManager {
  val models: mutable.HashMap[String, Model] = new mutable.HashMap[String, Model]()

  def addModel(model: Model): Unit = {
    models.put(model.name, model)
  }

  def removeModel(model: Model): Unit = {
    if (models.contains(model.name)) {
      models.remove(model.name)
    }
  }
}
