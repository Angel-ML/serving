package com.tencent.angel

import java.util

import com.tencent.angel.core.gen._

package object framework {
  type DataType = TypesProtos.DataType

  type VersionDef = VersionsProtos.VersionDef

  type OpDef = OpDefProtos.OpDef
  type OpDeprecation = OpDefProtos.OpDeprecation
  type OpList = OpDefProtos.OpList
  type ArgDef = OpDefProtos.OpDef.ArgDef
  type AttrDef = OpDefProtos.OpDef.AttrDef

  type AttrValue = AttrValueProtos.AttrValue
  type ListValue = AttrValueProtos.AttrValue.ListValue
  type NameAttrList = AttrValueProtos.NameAttrList

  type CostGraphDef = CostGraphProtos.CostGraphDef
  type Node = CostGraphProtos.CostGraphDef.Node
  type InputInfo = CostGraphProtos.CostGraphDef.Node.InputInfo
  type OutputInfo = CostGraphProtos.CostGraphDef.Node.OutputInfo

  type FunctionDefLibrary = FunctionProtos.FunctionDefLibrary
  type FunctionDef = FunctionProtos.FunctionDef
  type GradientDef = FunctionProtos.GradientDef

  type GraphDef = GraphProtos.GraphDef

  type NodeDef = NodeProto.NodeDef

  type ResourceHandleProto = ResourceHandleProtos.ResourceHandleProto

  type TensorProto = TensorProtos.TensorProto
  type VariantTensorDataProto = TensorProtos.VariantTensorDataProto

  type TensorShapeProto = TensorShapeProtos.TensorShapeProto
  type Dim = TensorShapeProtos.TensorShapeProto.Dim

}
