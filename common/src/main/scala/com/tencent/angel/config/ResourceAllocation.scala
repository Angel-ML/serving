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
package com.tencent.angel.config

import com.tencent.angel.config.ResourceProtos
import com.google.protobuf.UInt32Value

class Resource(val device: String, val deviceInstance: Int, val kind: String) {
  def toProto: ResourceProtos.Resource = {
    val builder = ResourceProtos.Resource.newBuilder()
    val UInt32DeviceInstance = UInt32Value.newBuilder().setValue(deviceInstance).build()
    builder.setDevice(device).setDeviceInstance(UInt32DeviceInstance)
    builder.setKind(kind)

    builder.build()
  }

  override def toString: String = toProto.toString

  override def equals(obj: Any): Boolean = {
    val other = obj.asInstanceOf[Resource]
    this.device == other.device && this.deviceInstance == other.deviceInstance && this.kind == other.kind
  }

  override def hashCode(): Int = {
    device.hashCode + deviceInstance.hashCode() + kind.hashCode
  }

  def ==(other: Resource): Boolean = this.equals(other)

  def !=(other: Resource): Boolean = !this.equals(other)
}

object Resource {
  def apply(device: String, deviceInstance: Int, kind: String): Resource = new Resource(device, deviceInstance, kind)

  def apply(resource: ResourceProtos.Resource): Resource = {
    new Resource(resource.getDevice, resource.getDeviceInstance.getValue, resource.getKind)
  }

  def apply(resource: String): Resource = {
    val resourceProtos = ResourceProtos.Resource.parseFrom(resource.getBytes)
    new Resource(resourceProtos.getDevice, resourceProtos.getDeviceInstance.getValue, resourceProtos.getKind)
  }
}


class Entry(val resource: Resource, var quantity: Long) {
  def clear(): Unit = {
    quantity = 0
  }

  def toProto: ResourceProtos.ResourceAllocation.Entry = {
    val builder = ResourceProtos.ResourceAllocation.Entry.newBuilder()
    builder.setResource(resource.toProto)
    builder.setQuantity(quantity)
    builder.build()
  }

  override def toString: String = toProto.toString

  def ==(other: Entry): Boolean = this.resource == other.resource && this.quantity == other.quantity

  def !=(other: Entry): Boolean = !(this == other)

  def >(other: Entry): Boolean = this.resource == other.resource && this.quantity > other.quantity

  def <(other: Entry): Boolean = this.resource == other.resource && this.quantity < other.quantity

  def >=(other: Entry): Boolean = this.resource == other.resource && this.quantity >= other.quantity

  def <=(other: Entry): Boolean = this.resource == other.resource && this.quantity <= other.quantity

  def +(other: Entry): Entry = {
    assert(this.resource == other.resource)
    Entry(this.resource, this.quantity + other.quantity)
  }

  def -(other: Entry): Entry = {
    assert(this.resource == other.resource)
    Entry(this.resource, this.quantity - other.quantity)
  }

  def *(other: Entry): Entry = {
    assert(this.resource == other.resource)
    Entry(this.resource, this.quantity * other.quantity)
  }

  def /(other: Entry): Entry = {
    assert(this.resource == other.resource)
    Entry(this.resource, this.quantity / other.quantity)
  }

  def +=(other: Entry): this.type = {
    assert(this.resource == other.resource)
    this.quantity += other.quantity
    this
  }

  def -=(other: Entry): this.type = {
    assert(this.resource == other.resource)
    this.quantity -= other.quantity
    this
  }

  def *=(other: Entry): this.type = {
    assert(this.resource == other.resource)
    this.quantity *= other.quantity
    this
  }

  def /=(other: Entry): this.type = {
    assert(this.resource == other.resource)
    this.quantity /= other.quantity
    this
  }

  def +(other: Double): Entry = {
    Entry(this.resource, (this.quantity + other).toLong)
  }

  def -(other: Double): Entry = {
    Entry(this.resource, (this.quantity - other).toLong)
  }

  def *(other: Double): Entry = {
    Entry(this.resource, (this.quantity * other).toLong)
  }

  def /(other: Double): Entry = {
    Entry(this.resource, (this.quantity / other).toLong)
  }

  def +=(other: Double): this.type = {
    this.quantity = (this.quantity + other).toLong
    this
  }

  def -=(other: Double): this.type = {
    this.quantity = (this.quantity - other).toLong
    this
  }

  def *=(other: Double): this.type = {
    this.quantity = (this.quantity * other).toLong
    this
  }

  def /=(other: Double): this.type = {
    this.quantity = (this.quantity / other).toLong
    this
  }
}

object Entry {
  def apply(resource: Resource, quantity: Long): Entry = new Entry(resource, quantity)

  def apply(entry: ResourceProtos.ResourceAllocation.Entry): Entry = {
    new Entry(Resource(entry.getResource), entry.getQuantity)
  }

  def apply(entry: String): Entry = {
    val entryProtos = ResourceProtos.ResourceAllocation.Entry.parseFrom(entry.getBytes)
    new Entry(Resource(entryProtos.getResource), entryProtos.getQuantity)
  }
}


class ResourceAllocation(val resourceQuantities: List[Entry]) {
  def clear(): Unit = {
    resourceQuantities.foreach(entry => entry.clear())
  }

  def verify(): Boolean = resourceQuantities.forall(entry => entry.quantity >= 0)

  def toProto: ResourceProtos.ResourceAllocation = {
    val builder = ResourceProtos.ResourceAllocation.newBuilder()
    resourceQuantities.foreach(entry =>
      builder.addResourceQuantities(entry.toProto)
    )
    builder.build()
  }

  override def toString: String = toProto.toString

  def ==(other: ResourceAllocation): Boolean = {
    this.resourceQuantities.zip(other.resourceQuantities).forall { case (thisResource, otherResource) =>
      thisResource == otherResource
    }
  }

  def !=(other: ResourceAllocation): Boolean = !(this == other)

  def >(other: ResourceAllocation): Boolean = {
    this.resourceQuantities.zip(other.resourceQuantities).forall { case (thisResource, otherResource) =>
      thisResource > otherResource
    }
  }

  def <(other: ResourceAllocation): Boolean = {
    this.resourceQuantities.zip(other.resourceQuantities).forall { case (thisResource, otherResource) =>
      thisResource < otherResource
    }
  }

  def >=(other: ResourceAllocation): Boolean = {
    this.resourceQuantities.zip(other.resourceQuantities).forall { case (thisResource, otherResource) =>
      thisResource >= otherResource
    }
  }

  def <=(other: ResourceAllocation): Boolean = {
    this.resourceQuantities.zip(other.resourceQuantities).forall { case (thisResource, otherResource) =>
      thisResource <= otherResource
    }
  }

  def +(other: ResourceAllocation): ResourceAllocation = {
    val entries = this.resourceQuantities.zip(other.resourceQuantities).map { case (thisResource, otherResource) =>
      thisResource + otherResource
    }
    ResourceAllocation(entries)
  }

  def -(other: ResourceAllocation): ResourceAllocation = {
    val entries = this.resourceQuantities.zip(other.resourceQuantities).map { case (thisResource, otherResource) =>
      thisResource - otherResource
    }
    ResourceAllocation(entries)
  }

  def *(other: ResourceAllocation): ResourceAllocation = {
    val entries = this.resourceQuantities.zip(other.resourceQuantities).map { case (thisResource, otherResource) =>
      thisResource * otherResource
    }
    ResourceAllocation(entries)
  }

  def /(other: ResourceAllocation): ResourceAllocation = {
    val entries = this.resourceQuantities.zip(other.resourceQuantities).map { case (thisResource, otherResource) =>
      thisResource / otherResource
    }
    ResourceAllocation(entries)
  }

  def +=(other: ResourceAllocation): this.type = {
    this.resourceQuantities.zip(other.resourceQuantities).map { case (thisResource, otherResource) =>
      thisResource += otherResource
    }
    this
  }

  def -=(other: ResourceAllocation): this.type = {
    this.resourceQuantities.zip(other.resourceQuantities).map { case (thisResource, otherResource) =>
      thisResource -= otherResource
    }
    this
  }

  def *=(other: ResourceAllocation): this.type = {
    this.resourceQuantities.zip(other.resourceQuantities).map { case (thisResource, otherResource) =>
      thisResource *= otherResource
    }
    this
  }

  def /=(other: ResourceAllocation): this.type = {
    this.resourceQuantities.zip(other.resourceQuantities).map { case (thisResource, otherResource) =>
      thisResource /= otherResource
    }
    this
  }

  def +(other: Double): ResourceAllocation = {
    val entries = this.resourceQuantities.map { thisResource =>
      thisResource + other
    }
    ResourceAllocation(entries)
  }

  def -(other: Double): ResourceAllocation = {
    val entries = this.resourceQuantities.map { thisResource =>
      thisResource - other
    }
    ResourceAllocation(entries)
  }

  def *(other: Double): ResourceAllocation = {
    val entries = this.resourceQuantities.map { thisResource =>
      thisResource * other
    }
    ResourceAllocation(entries)
  }

  def /(other: Double): ResourceAllocation = {
    val entries = this.resourceQuantities.map { thisResource =>
      thisResource / other
    }
    ResourceAllocation(entries)
  }

  def +=(other: Double): this.type = {
    this.resourceQuantities.map { thisResource =>
      thisResource += other
    }
    this
  }

  def -=(other: Double): this.type = {
    this.resourceQuantities.map { thisResource =>
      thisResource -= other
    }
    this
  }

  def *=(other: Double): this.type = {
    this.resourceQuantities.map { thisResource =>
      thisResource *= other
    }
    this
  }

  def /=(other: Double): this.type = {
    this.resourceQuantities.map { thisResource =>
      thisResource /= other
    }
    this
  }
}

object ResourceAllocation {
  def apply(resourceQuantities: List[Entry]): ResourceAllocation = new ResourceAllocation(resourceQuantities)

  def apply(resourceAllocation: ResourceProtos.ResourceAllocation): ResourceAllocation = {
    val resourceQuantities = (0 until resourceAllocation.getResourceQuantitiesCount).toList.map { idx =>
      Entry(resourceAllocation.getResourceQuantities(idx))
    }
    new ResourceAllocation(resourceQuantities)
  }

  def apply(resourceAllocation: String): ResourceAllocation = {
    val resourceAllocationProto = ResourceProtos.ResourceAllocation.parseFrom(resourceAllocation.getBytes)
    val numEntries = resourceAllocationProto.getResourceQuantitiesCount
    val resourceQuantities = (0 until numEntries).toList.map { idx =>
      Entry(resourceAllocationProto.getResourceQuantities(idx))
    }
    new ResourceAllocation(resourceQuantities)
  }
}
