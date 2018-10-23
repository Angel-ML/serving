package com.tencent.angel.framework.contexts

import java.util

class NameContext {
  private val prefixStack: util.Stack[String] = new util.Stack[String]()
  private val usedNames: util.HashSet[String] = new util.HashSet[String]()

  prefixStack.push("")
  usedNames.add("")
  usedNames.add("_")

  def getName(name: String, reuse: Boolean = false): String = {
    val prefix = prefixStack.peek()
    var tempName = if (prefix == "") {
      name
    } else if (prefix.endsWith("/")) {
      s"$prefix$name"
    } else {
      s"$prefix/$name"
    }

    if (!reuse) {
      var i: Int = 1
      while (usedNames.contains(tempName)) {
        tempName = s"${tempName}_$i"
        i += 1
      }
    }

    usedNames.add(tempName)
    tempName
  }

  def withNameScope(name: String)(func: () => Unit): Unit = {
    val nameScope = getName(name)
    prefixStack.push(nameScope)
    try {
      func()
    } finally {
      prefixStack.pop()
    }
  }
}