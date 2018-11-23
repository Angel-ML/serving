package com.tencent.angel.serving.servables.angel

import com.tencent.angel.core.saver.MetaGraphProtos.MetaGraphDef

class SavedModelBundle {
  var session: Session = _
  var metaGraphDef: MetaGraphDef = _

}
