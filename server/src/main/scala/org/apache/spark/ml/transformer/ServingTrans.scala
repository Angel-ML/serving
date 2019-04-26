package org.apache.spark.ml.transformer

import java.util
import org.apache.spark.ml.data.{SDFrame, SRow}
import org.apache.spark.ml.param.{ParamMap, ParamPair}


abstract class ServingTrans extends ServingStage {
  def transform(dataset: SDFrame): SDFrame

  def transform(dataset: SDFrame, paramMap: ParamMap): SDFrame = {
    this.copy(paramMap).transform(dataset)
  }

  def transform(dataset: SDFrame,
                firstParamPair: ParamPair[_],
                otherParamPairs: ParamPair[_]*): SDFrame = {
    val map = new ParamMap()
      .put(firstParamPair)
      .put(otherParamPairs: _*)

    transform(dataset, map)
  }

  override def copy(extra: ParamMap): ServingTrans

  def prepareData(rows: Array[SRow]): SDFrame

  def prepareData(feature: util.Map[String, _]): SDFrame
}
