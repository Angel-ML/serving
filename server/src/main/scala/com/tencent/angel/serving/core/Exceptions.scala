/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.tencent.angel.serving.core

case class CoreExceptions(message: String) extends Exception(message)


case class RouteExceptions(message: String) extends RuntimeException(message)


case class AdaptExceptions(message: String) extends RuntimeException(message)


case class MonitorExceptions(message: String) extends RuntimeException(message)


case class LoadExceptions(message: String) extends Exception(message)


case class ResourceExceptions(message: String) extends RuntimeException(message)


case class ConfigExceptions(message: String) extends RuntimeException(message)


case class ManagerExceptions(message: String) extends RuntimeException(message)

case class FailedPreconditions(message: String) extends Exception(message)

case class InvalidArguments(message: String) extends Exception(message)

case class NotFoundExceptions(message: String) extends Exception(message)




