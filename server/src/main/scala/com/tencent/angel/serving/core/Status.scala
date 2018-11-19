package com.tencent.angel.serving.core

class Status private()

object Status{
  private val ok: Status = new Status()
  private val error: Status = new Status()

  def OK: Status = ok
  def Error: Status = error
}