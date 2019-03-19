package org.apache.spark.ml.utils

import org.apache.spark.ml.classification._
import org.apache.spark.ml.clustering._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.fpm.FPGrowthModel
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.ml.regression._
import org.apache.spark.ml.transformer.{ServingModel, ServingPipelineModel, ServingStage, ServingTrans}
import org.apache.spark.ml.tuning.{CrossValidatorModel, TrainValidationSplitModel}
import org.apache.spark.ml.util.DefaultParamsReader
import org.apache.spark.ml.{MetaSnapshot, Model, PipelineModel, Transformer}
import org.apache.spark.sql.SparkSession


object ModelUtils {

  def loadMetadata(path: String, spark: SparkSession): MetaSnapshot = {
    val metadata = DefaultParamsReader.loadMetadata(path, spark.sparkContext)

    MetaSnapshot(metadata.uid, metadata.className, metadata.sparkVersion)
  }

  def loadModel(name: String, path: String): Model[_] = {
    name match {
      case modelName if classOf[AFTSurvivalRegressionModel].getSimpleName.equalsIgnoreCase(modelName) =>
        AFTSurvivalRegressionModel.load(path)
      case modelName if classOf[Bucketizer].getSimpleName.equalsIgnoreCase(modelName) =>
        Bucketizer.load(path)
      case modelName if classOf[IsotonicRegressionModel].getSimpleName.equalsIgnoreCase(modelName) =>
        IsotonicRegressionModel.load(path)
      case modelName if classOf[MinMaxScalerModel].getSimpleName.equalsIgnoreCase(modelName) =>
        MinMaxScalerModel.load(path)
      case modelName if classOf[RFormulaModel].getSimpleName.equalsIgnoreCase(modelName) =>
        RFormulaModel.load(path)
      case modelName if classOf[Word2VecModel].getSimpleName.equalsIgnoreCase(modelName) =>
        Word2VecModel.load(path)
      case modelName if classOf[KMeansModel].getSimpleName.equalsIgnoreCase(modelName) =>
        KMeansModel.load(path)
      case modelName if classOf[StringIndexerModel].getSimpleName.equalsIgnoreCase(modelName) =>
        StringIndexerModel.load(path)
      case modelName if classOf[MinHashLSHModel].getSimpleName.equalsIgnoreCase(modelName) =>
        MinHashLSHModel.load(path)
      case modelName if classOf[BucketedRandomProjectionLSHModel].getSimpleName.equalsIgnoreCase(modelName) =>
        BucketedRandomProjectionLSHModel.load(path)
      case modelName if classOf[CrossValidatorModel].getSimpleName.equalsIgnoreCase(modelName) =>
        CrossValidatorModel.load(path)
      case modelName if classOf[DistributedLDAModel].getSimpleName.equalsIgnoreCase(modelName) =>
        DistributedLDAModel.load(path)
      case modelName if classOf[LocalLDAModel].getSimpleName.equalsIgnoreCase(modelName) =>
        LocalLDAModel.load(path)
      case modelName if classOf[MaxAbsScalerModel].getSimpleName.equalsIgnoreCase(modelName) =>
        MaxAbsScalerModel.load(path)
      case modelName if classOf[ImputerModel].getSimpleName.equalsIgnoreCase(modelName) =>
        ImputerModel.load(path)
      case modelName if classOf[TrainValidationSplitModel].getSimpleName.equalsIgnoreCase(modelName) =>
        TrainValidationSplitModel.load(path)
      case modelName if classOf[BisectingKMeansModel].getSimpleName.equalsIgnoreCase(modelName) =>
        BisectingKMeansModel.load(path)
      case modelName if classOf[RandomForestRegressionModel].getSimpleName.equalsIgnoreCase(modelName) =>
        RandomForestRegressionModel.load(path)
      case modelName if classOf[GBTRegressionModel].getSimpleName.equalsIgnoreCase(modelName) =>
        GBTRegressionModel.load(path)
      case modelName if classOf[GeneralizedLinearRegressionModel].getSimpleName.equalsIgnoreCase(modelName) =>
        GeneralizedLinearRegressionModel.load(path)
      case modelName if classOf[LinearRegressionModel].getSimpleName.equalsIgnoreCase(modelName) =>
        LinearRegressionModel.load(path)
      case modelName if classOf[DecisionTreeRegressionModel].getSimpleName.equalsIgnoreCase(modelName) =>
        DecisionTreeRegressionModel.load(path)
      case modelName if classOf[LinearSVCModel].getSimpleName.equalsIgnoreCase(modelName) =>
        LinearSVCModel.load(path)
      case modelName if classOf[NaiveBayesModel].getSimpleName.equalsIgnoreCase(modelName) =>
        NaiveBayesModel.load(path)
      case modelName if classOf[GBTClassificationModel].getSimpleName.equalsIgnoreCase(modelName) =>
        GBTClassificationModel.load(path)
      case modelName if classOf[DecisionTreeClassificationModel].getSimpleName.equalsIgnoreCase(modelName) =>
        DecisionTreeClassificationModel.load(path)
      case modelName if classOf[MultilayerPerceptronClassificationModel].getSimpleName.equalsIgnoreCase(modelName) =>
        MultilayerPerceptronClassificationModel.load(path)
      case modelName if classOf[LogisticRegressionModel].getSimpleName.equalsIgnoreCase(modelName) =>
        LogisticRegressionModel.load(path)
      case modelName if classOf[RandomForestClassificationModel].getSimpleName.equalsIgnoreCase(modelName) =>
        RandomForestClassificationModel.load(path)
      case modelName if classOf[GaussianMixtureModel].getSimpleName.equalsIgnoreCase(modelName) =>
        GaussianMixtureModel.load(path)
      case modelName if classOf[PCAModel].getSimpleName.equalsIgnoreCase(modelName) =>
        PCAModel.load(path)
      case modelName if classOf[PipelineModel].getSimpleName.equalsIgnoreCase(modelName) =>
        PipelineModel.load(path)
      case modelName if classOf[ChiSqSelectorModel].getSimpleName.equalsIgnoreCase(modelName) =>
        ChiSqSelectorModel.load(path)
      case modelName if classOf[VectorIndexerModel].getSimpleName.equalsIgnoreCase(modelName) =>
        VectorIndexerModel.load(path)
      case modelName if classOf[ALSModel].getSimpleName.equalsIgnoreCase(modelName) =>
        ALSModel.load(path)
      case modelName if classOf[OneHotEncoderModel].getSimpleName.equalsIgnoreCase(modelName) =>
        OneHotEncoderModel.load(path)
      case modelName if classOf[OneVsRestModel].getSimpleName.equalsIgnoreCase(modelName) =>
        OneVsRestModel.load(path)
      case modelName if classOf[FPGrowthModel].getSimpleName.equalsIgnoreCase(modelName) =>
        FPGrowthModel.load(path)
      case modelName if classOf[StandardScalerModel].getSimpleName.equalsIgnoreCase(modelName) =>
        StandardScalerModel.load(path)
      case modelName if classOf[CountVectorizerModel].getSimpleName.equalsIgnoreCase(modelName) =>
        CountVectorizerModel.load(path)
      case modelName if classOf[IDFModel].getSimpleName.equalsIgnoreCase(modelName) =>
        IDFModel.load(path)
    }
  }


  def transModel(sparkModel: Model[_]): ServingModel[_] = {
    sparkModel match {
      case model: PipelineModel =>
        val serStages = model.stages.map(trans)
        new ServingPipelineModel(model.uid, serStages)
      case model => trans(model).asInstanceOf[ServingModel[_]]
    }
  }

  private def trans(stage: Transformer): ServingTrans = ???


}
