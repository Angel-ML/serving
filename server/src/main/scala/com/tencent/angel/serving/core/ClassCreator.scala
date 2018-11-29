package com.tencent.angel.serving.core

import com.google.protobuf.Message

abstract class ClassCreator[BaseClass] {
  def create(config: Message):BaseClass = ???
}
