package com.tencent.angel.serving.core

object LoadServablesFast {
  def connectSourcesWithFastInitialLoad(aspiredVersionsManager: AspiredVersionsManager, sources:List[Source[Loader]],
                                        servableStateMonitor: ServableStateMonitor, initialServables: List[ServableRequest],
                                        numThreads:Int): Unit = ???


}
