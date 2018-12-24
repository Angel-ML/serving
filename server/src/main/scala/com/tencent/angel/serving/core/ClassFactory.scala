package com.tencent.angel.serving.core


import com.google.protobuf.Message
import com.tencent.angel.servable.SavedModelBundleSourceAdapterConfigProtos.SavedModelBundleSourceAdapterConfig
import com.tencent.angel.servable.SessionBundleSourceAdapterConfigProtos.SessionBundleSourceAdapterConfig

object ClassFactory {

  //create an instance of BaseClass based on a config proto
  def create[BaseClass](config: Message, classCreator: String, methodName: String="apply"): BaseClass = {
    val cls = Class.forName(classCreator)
    val result = config match {
      case savedConf: SavedModelBundleSourceAdapterConfig =>
        val method = cls.getDeclaredMethod(methodName, classOf[SavedModelBundleSourceAdapterConfig])
        method.invoke(null, savedConf)
      case sessionConf: SessionBundleSourceAdapterConfig =>
        val method = cls.getDeclaredMethod(methodName, classOf[SessionBundleSourceAdapterConfig])
        method.invoke(null, sessionConf)
    }

    result.asInstanceOf[BaseClass]
  }

  def createFromAny[BaseClass](platform: String, anyConfig: com.google.protobuf.Any): BaseClass = {
    val (config, fullTypeName) = if (anyConfig.is(classOf[SavedModelBundleSourceAdapterConfig])) {
      val congObj = anyConfig.unpack(classOf[SavedModelBundleSourceAdapterConfig])
      congObj -> congObj.getAdapterClassName
    } else {
      val congObj = anyConfig.unpack(classOf[SessionBundleSourceAdapterConfig])
      congObj -> congObj.getAdapterClassName
    }

    create[BaseClass](config, fullTypeName)
  }
}