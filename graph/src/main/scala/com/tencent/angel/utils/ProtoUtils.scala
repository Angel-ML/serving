package com.tencent.angel.utils

import com.google.protobuf.ByteString
import com.tencent.angel.core.gen._
import com.tencent.angel.framework._

object ProtoUtils {
  def getVersionDef(producer: Int, minConsumer: Int, badConsumers: List[Int] = null): VersionDef = {
    val builder = VersionsProtos.VersionDef.newBuilder()
    builder.setProducer(producer).setMinConsumer(minConsumer)
    if (badConsumers!= null && badConsumers.nonEmpty) badConsumers.foreach { badConsumer =>
      builder.addBadConsumers(badConsumer)
    }

    builder.build()
  }

  def getOpDef(name:String, inputArgs: List[ArgDef], outputArgs: List[ArgDef], attrs:List[AttrDef],
               summary: Option[String] = None,
               description: Option[String] = None,
               isCommutative: Option[Boolean] = None,
               isAggregate: Option[Boolean] = None,
               isStateful: Option[Boolean] = None,
               allowsUninitializedInput: Option[Boolean] = None
              ): OpDef = {
    val builder = OpDefProtos.OpDef.newBuilder()

    builder.setName(name)
    if (inputArgs != null && inputArgs.nonEmpty) {
      inputArgs.foreach( ipa => builder.addInputArg(ipa))
    }

    if (outputArgs != null && outputArgs.nonEmpty) {
      outputArgs.foreach( opa => builder.addOutputArg(opa))
    }

    if (attrs != null && attrs.nonEmpty) {
      attrs.foreach( att => builder.addAttr(att))
    }

    summary.foreach( ele => builder.setSummary(ele))
    description.foreach( ele => builder.setDescription(ele))
    isCommutative.foreach( ele => builder.setIsCommutative(ele))
    isAggregate.foreach( ele => builder.setIsAggregate(ele))
    isStateful.foreach( ele => builder.setIsStateful(ele))
    allowsUninitializedInput.foreach( ele => builder.setAllowsUninitializedInput(ele))

    builder.build()
  }

  def getOpDeprecation(version: Int, explanation: String = null): OpDeprecation = {
    val builder = OpDefProtos.OpDeprecation.newBuilder()
    if (version > 0) builder.setVersion(version)
    if (explanation != null && explanation.nonEmpty) builder.setExplanation(explanation)

    builder.build()
  }

  def getOpList(ops: OpDef*): OpList = {
    val builder = OpDefProtos.OpList.newBuilder()
    if (ops != null && ops.nonEmpty) ops.foreach(op => builder.addOp(op))

    builder.build()
  }

  def getOpList(ops: List[OpDef]): OpList = {
    val builder = OpDefProtos.OpList.newBuilder()
    if (ops != null && ops.nonEmpty) ops.foreach(op => builder.addOp(op))

    builder.build()
  }

  def getArgDef(name: String, dtype: DataType, typeAttr: String,
                numberAttr: String = null, typeListAttr: String = null,
                description: Option[String] = None, isRef: Option[Boolean] = None): ArgDef = {
    val builder = OpDefProtos.OpDef.ArgDef.newBuilder()
    if (name != null && name.nonEmpty) builder.setName(name)
    if (dtype != null) builder.setType(dtype)
    if (typeAttr != null && typeAttr.nonEmpty) builder.setTypeAttr(typeAttr)
    if (numberAttr != null && numberAttr.nonEmpty) builder.setNumberAttr(numberAttr)
    if (typeListAttr != null && typeListAttr.nonEmpty) builder.setTypeListAttr(typeListAttr)

    description.foreach( ele => builder.setDescription(ele))
    isRef.foreach( ele => builder.setIsRef(ele))

    builder.build()
  }

  def getAttrDef(name: String, dtype: String, default: AttrValue,
                 description: Option[String] = None, hasMinimum: Option[Boolean] = None,
                 minimum: Option[Long] = None, allowedValues: Option[AttrValue] = None): AttrDef = {
    val builder = OpDefProtos.OpDef.AttrDef.newBuilder()
    if (name != null && name.nonEmpty) builder.setName(name)
    if (dtype != null && dtype.nonEmpty) builder.setType(dtype)
    if (default != null) builder.setDefaultValue(default)

    description.foreach( ele => builder.setDescription(ele))
    hasMinimum.foreach( ele => builder.setHasMinimum(ele))
    minimum.foreach( ele => builder.setMinimum(ele))
    allowedValues.foreach( ele => builder.setAllowedValues(ele))


    builder.build()
  }

  def getAttrValue(s: Option[String] = None, i: Option[Long] = None, f: Option[Float] = None, b: Option[Boolean] = None,
                   dtype: Option[DataType] = None, shape: Option[TensorShapeProto] = None, placeholder: Option[String] = None,
                   tensor: Option[TensorProto] = None, list: Option[ListValue] = None, func: Option[NameAttrList] = None
                  ): AttrValue = {
    val paramsIsDefined = List(
      "s" -> s.isDefined,
      "i" -> i.isDefined,
      "f" -> f.isDefined,
      "b" -> b.isDefined,
      "dtype" -> dtype.isDefined,
      "shape" -> shape.isDefined,
      "placeholder" -> placeholder.isDefined,
      "tensor" -> tensor.isDefined,
      "list" -> list.isDefined,
      "func" -> func.isDefined)
    val setted = paramsIsDefined.filter{ case (_, flag) => flag }
    assert(setted.size <= 1)

    val builder = AttrValueProtos.AttrValue.newBuilder()
    if (setted.size == 1) {
      setted.head._1 match {
        case "s" => builder.setS(ByteString.copyFromUtf8(s.get))
        case "i" => builder.setI(i.get)
        case "f" => builder.setF(f.get)
        case "b" => builder.setB(b.get)
        case "dtype" => builder.setType(dtype.get)
        case "shape" => builder.setShape(shape.get)
        case "placeholder" => builder.setPlaceholder(placeholder.get)
        case "tensor" => builder.setTensor(tensor.get)
        case "list" => builder.setList(list.get)
        case "func" => builder.setFunc(func.get)
      }
    }

    builder.build()
  }

//  def getListValue(ss: String*): ListValue = {
//    val builder = AttrValueProtos.AttrValue.ListValue.newBuilder()
//    if (ss != null && ss.nonEmpty) {
//      ss.zipWithIndex.foreach { case (str, idx) =>
//        builder.setS(idx, ByteString.copyFromUtf8(str))
//      }
//    }
//
//    builder.build()
//  }
//
//  def getListValue(is: Long*): ListValue = {
//    val builder = AttrValueProtos.AttrValue.ListValue.newBuilder()
//    if (is != null && is.nonEmpty) {
//      is.zipWithIndex.foreach { case (i, idx) =>
//        builder.setI(idx, i)
//      }
//    }
//
//    builder.build()
//  }
//
//  def getListValue(fs: Float*): ListValue = {
//    val builder = AttrValueProtos.AttrValue.ListValue.newBuilder()
//    if (fs != null && fs.nonEmpty) {
//      fs.zipWithIndex.foreach { case (f, idx) =>
//        builder.setF(idx, f)
//      }
//    }
//
//    builder.build()
//  }
//
//  def getListValue(bs: Boolean*): ListValue = {
//    val builder = AttrValueProtos.AttrValue.ListValue.newBuilder()
//    if (bs != null && bs.nonEmpty) {
//      bs.zipWithIndex.foreach { case (b, idx) =>
//        builder.setB(idx, b)
//      }
//    }
//
//    builder.build()
//  }
//
//  def getListValue(types: DataType*): ListValue = {
//    val builder = AttrValueProtos.AttrValue.ListValue.newBuilder()
//    if (types != null && types.nonEmpty) {
//      types.zipWithIndex.foreach { case (type_, idx) =>
//        builder.setType(idx, type_)
//      }
//    }
//
//    builder.build()
//  }
//
//  def getListValue(shapes: TensorShapeProto*): ListValue = {
//    val builder = AttrValueProtos.AttrValue.ListValue.newBuilder()
//    if (shapes != null && shapes.nonEmpty) {
//      shapes.zipWithIndex.foreach { case (shape, idx) =>
//        builder.setShape(idx, shape)
//      }
//    }
//
//    builder.build()
//  }
//
//  def getListValue(tensors: TensorProto*): ListValue = {
//    val builder = AttrValueProtos.AttrValue.ListValue.newBuilder()
//    if (tensors != null && tensors.nonEmpty) {
//      tensors.zipWithIndex.foreach { case (tensor, idx) =>
//        builder.setTensor(idx, tensor)
//      }
//    }
//
//    builder.build()
//  }
//
//  def getListValue(funcs: NameAttrList*): ListValue = {
//    val builder = AttrValueProtos.AttrValue.ListValue.newBuilder()
//    if (funcs != null && funcs.nonEmpty) {
//      funcs.zipWithIndex.foreach { case (func, idx) =>
//        builder.setFunc(idx, func)
//      }
//    }
//
//    builder.build()
//  }

  def getListValue(ss: List[String] = null,
                   is: List[Long] = null,
                   fs: List[Float] = null,
                   bs: List[Boolean] = null,
                   shapes: List[TensorShapeProto] = null,
                   types: List[DataType] = null,
                   tensors: List[TensorProto] = null,
                   funcs: List[NameAttrList] = null): ListValue = {
    val builder = AttrValueProtos.AttrValue.ListValue.newBuilder()
    if (ss != null && ss.nonEmpty) {
      ss.zipWithIndex.foreach { case (str, idx) =>
        builder.setS(idx, ByteString.copyFromUtf8(str))
      }
    }

    if (is != null && is.nonEmpty) {
      is.zipWithIndex.foreach { case (i, idx) =>
        builder.setI(idx, i)
      }
    }

    if (fs != null && fs.nonEmpty) {
      fs.zipWithIndex.foreach { case (f, idx) =>
        builder.setF(idx, f)
      }
    }

    if (bs != null && bs.nonEmpty) {
      bs.zipWithIndex.foreach { case (b, idx) =>
        builder.setB(idx, b)
      }
    }

    if (shapes != null && shapes.nonEmpty) {
      shapes.zipWithIndex.foreach { case (shape, idx) =>
        builder.setShape(idx, shape)
      }
    }

    if (types != null && types.nonEmpty) {
      types.zipWithIndex.foreach { case (type_, idx) =>
        builder.setType(idx, type_)
      }
    }

    if (tensors != null && tensors.nonEmpty) {
      tensors.zipWithIndex.foreach { case (tensor, idx) =>
        builder.setTensor(idx, tensor)
      }
    }

    if (funcs != null && funcs.nonEmpty) {
      funcs.zipWithIndex.foreach { case (func, idx) =>
        builder.setFunc(idx, func)
      }
    }

    builder.build()
  }


  def getNameAttrList(name: String, attr: Map[String, AttrValue]): NameAttrList = {
    val builder = AttrValueProtos.NameAttrList.newBuilder()
    if (name != null && name.nonEmpty) builder.setName(name)
    if (attr != null) {
      attr.foreach{ case (key, value) =>
        builder.putAttr(key, value)
      }
    }

    builder.build()
  }

  def getCostGraphDef(nodes: Node*): CostGraphDef = {
    val builder = CostGraphProtos.CostGraphDef.newBuilder()
    if (nodes != null && nodes.nonEmpty) {
      nodes.foreach(node => builder.addNode(node))
    }

    builder.build()
  }

  def getCostGraphDef(nodes: List[Node]): CostGraphDef = {
    val builder = CostGraphProtos.CostGraphDef.newBuilder()
    if (nodes != null && nodes.nonEmpty) {
      nodes.foreach(node => builder.addNode(node))
    }

    builder.build()
  }

  def getNode(name: String, device: String, id: Int, inputInfos: List[InputInfo], outputInfos: List[OutputInfo],
              temporaryMemorySize: Option[Long] = None,
              persistentMemorySize: Option[Long] = None,
              computeCost: Option[Long] = None,
              computeTime: Option[Long] = None,
              memoryTime: Option[Long] = None,
              isFinal: Option[Boolean] = None,
              controlInputs: Option[Array[Int]] = None,
              inAccurate: Option[Boolean] = None
             ): Node = {
    val builder = CostGraphProtos.CostGraphDef.Node.newBuilder()
    builder.setName(name).setDevice(device).setId(id)

    if (inputInfos != null && inputInfos.nonEmpty) inputInfos.foreach(ipi => builder.addInputInfo(ipi))
    if (outputInfos != null && outputInfos.nonEmpty) outputInfos.foreach(opi => builder.addOutputInfo(opi))

    temporaryMemorySize.foreach( ele => builder.setTemporaryMemorySize(ele))
    persistentMemorySize.foreach( ele => builder.setPersistentMemorySize(ele))
    computeCost.foreach( ele => builder.setComputeCost(ele))
    computeTime.foreach( ele => builder.setComputeTime(ele))
    memoryTime.foreach( ele => builder.setMemoryTime(ele))
    isFinal.foreach( ele => builder.setIsFinal(ele))
    controlInputs.foreach{ eles => eles.foreach(ele => builder.addControlInput(ele)) }
    inAccurate.foreach( ele => builder.setInaccurate(ele))

    builder.build()
  }

  def getInputInfo(precedingNode: Int, precedingPort: Int): InputInfo = {
    val builder = CostGraphProtos.CostGraphDef.Node.InputInfo.newBuilder()
    builder.setPrecedingNode(precedingNode).setPrecedingPort(precedingPort)
    builder.build()
  }

  def getOutputInfo(shape: TensorShapeProto, dtype: DataType,
                    size: Option[Long] = None, aliasInputPort:Option[Long]=None): OutputInfo = {
    val builder = CostGraphProtos.CostGraphDef.Node.OutputInfo.newBuilder()
    if (shape != null) builder.setShape(shape)
    if (dtype != null) builder.setDtype(dtype)
    size.foreach(ele =>  builder.setSize(ele))
    aliasInputPort.foreach(ele => builder.setAliasInputPort(ele))

    builder.build()
  }

  def getFunctionDefLibrary(functions: List[FunctionDef], gradients: List[GradientDef]): FunctionDefLibrary = {
    val builder = FunctionProtos.FunctionDefLibrary.newBuilder()
    assert(functions.size == gradients.size)
    if (functions != null && functions.nonEmpty) functions.foreach(func => builder.addFunction(func))
    if (gradients != null && gradients.nonEmpty) gradients.foreach(grad => builder.addGradient(grad))

    builder.build()
  }

  def getFunctionDef(signature: OpDef, nodeDefs: List[NodeDef],
                     attr: Map[String, AttrValue] = null, ret: Map[String, String] = null): FunctionDef = {
    val builder = FunctionProtos.FunctionDef.newBuilder()
    if (signature != null) builder.setSignature(signature)

    if (nodeDefs != null && nodeDefs.nonEmpty) {
      nodeDefs.foreach(nodeDef => builder.addNodeDef(nodeDef))
    }

    if (attr != null && attr.nonEmpty) {
      attr.foreach{ case (key, value) => builder.putAttr(key, value)}
    }

    if (ret != null && ret.nonEmpty) {
      ret.foreach{ case (key, value) => builder.putRet(key, value)}
    }

    builder.build()
  }

  def getGradientDef(name:String, gradFunc: String): GradientDef = {
    val builder = FunctionProtos.GradientDef.newBuilder()
    builder.setFunctionName(name).setGradientFunc(gradFunc).build()
  }

  def getGraphDef(nodes: List[NodeDef], versionDef: VersionDef,
                  library: FunctionDefLibrary): GraphDef = {
    val builder = GraphProtos.GraphDef.newBuilder()
    if (nodes != null && nodes.nonEmpty) nodes.foreach(node => builder.addNode(node))
    if (versionDef != null) builder.setVersions(versionDef)
    if (library != null) builder.setLibrary(library)

    builder.build()
  }

  def getNodeDef(name: String, opName: String, input: List[String],
                 device: String = "cup", attr: Map[String, AttrValue] = null): NodeDef = {
    val builder = NodeProto.NodeDef.newBuilder()
    if (name != null && name.nonEmpty) builder.setName(name)
    if (opName != null && opName.nonEmpty) builder.setOp(opName)
    if (device != null && device.nonEmpty) builder.setDevice(device)
    if (input != null && input.nonEmpty) input.foreach(ips => builder.addInput(ips))
    if (attr != null && attr.nonEmpty) attr.foreach { case (key, value) => builder.putAttr(key, value) }

    builder.build()
  }

  def getResourceHandleProto(name: String, device: String = null, container: String = null,
                             hashCode: Long = -1L, maybeTypeName: String = null): ResourceHandleProto = {
    val builder = ResourceHandleProtos.ResourceHandleProto.newBuilder()
    if (device != null && device.nonEmpty) builder.setDevice(device)
    if (container != null && container.nonEmpty) builder.setContainer(container)
    if (name != null && name.nonEmpty) builder.setName(name)
    if (maybeTypeName != null && maybeTypeName.nonEmpty) builder.setMaybeTypeName(maybeTypeName)
    if (hashCode > 0) builder.setHashCode(hashCode)

    builder.build()
  }

  def getTensorProto(dtype: DataType, shape: TensorShapeProto, version: Option[Int]=None,
                     content:Option[Array[Byte]] = None,
                     stringVals:Option[Array[String]] = None,
                     floatVals:Option[Array[Float]] = None,
                     doubleVal:Option[Array[Double]] = None,
                     intVals:Option[Array[Int]] = None,
                     longVals:Option[Array[Long]] = None,
                     boolVals:Option[Array[Boolean]] = None,
                     resourceHandle: Option[Array[ResourceHandleProto]] = None): TensorProto = {
    val builder = TensorProtos.TensorProto.newBuilder()
    builder.setDtype(dtype).setTensorShape(shape)
    if (version.isDefined) builder.setVersionNumber(version.get)

    if (content.isDefined) builder.setTensorContent(ByteString.copyFrom(content.get))

    if (stringVals.isDefined) stringVals.get.foreach{ str => builder.addStringVal(ByteString.copyFromUtf8(str)) }

    if (floatVals.isDefined) floatVals.get.foreach{ v => builder.addFloatVal(v) }

    if (doubleVal.isDefined) doubleVal.get.foreach{ v => builder.addDoubleVal(v) }

    if (intVals.isDefined) intVals.get.foreach{ v => builder.addIntVal(v) }

    if (longVals.isDefined) longVals.get.foreach{ v => builder.addInt64Val(v) }

    if (boolVals.isDefined) boolVals.get.foreach{ v => builder.addBoolVal(v) }

    if (resourceHandle.isDefined) resourceHandle.get.foreach{ v => builder.addResourceHandleVal(v) }

    builder.build()
  }

  def getVariantTensorDataProto(typeName: String, metadata: Array[Byte], tensors: TensorProto*): VariantTensorDataProto = {
    val builder = TensorProtos.VariantTensorDataProto.newBuilder()
    if (typeName != null && typeName != "") builder.setTypeName(typeName)
    if (metadata != null && metadata.nonEmpty) builder.setMetadata(ByteString.copyFrom(metadata))
    if (tensors != null && tensors.nonEmpty) {
      tensors.zipWithIndex.foreach { case (ts, idx) => builder.setTensors(idx, ts) }
    }

    builder.build()
  }

  def getTensorShapeProto(unknownRank: Option[Boolean], dims: Long*): TensorShapeProto = {
    val builder = TensorShapeProtos.TensorShapeProto.newBuilder()
    if (unknownRank.isDefined) builder.setUnknownRank(unknownRank.get)
    dims.foreach(dim => builder.addDim(getDim(dim)))

    builder.build()
  }

  def getTensorShapeProto(dims: Long*): TensorShapeProto = {
    val builder = TensorShapeProtos.TensorShapeProto.newBuilder()
    builder.setUnknownRank(false)
    dims.foreach { dim => builder.addDim(getDim(dim)) }

    builder.build()
  }

  def getTensorShapeProto(dims: List[Long]): TensorShapeProto = {
    val builder = TensorShapeProtos.TensorShapeProto.newBuilder()
    builder.setUnknownRank(false)
    dims.foreach { dim => builder.addDim(getDim(dim)) }

    builder.build()
  }

  def getDim(size: Long, name: String = null): Dim = {
    val builder = TensorShapeProtos.TensorShapeProto.Dim.newBuilder()
    builder.setSize(size)
    if (name != null && name.nonEmpty) builder.setName(name)

    builder.build()
  }
}
