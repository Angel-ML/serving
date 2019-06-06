package org.apache.spark.ml.feature.utils

import org.apache.spark.ml.classification._
import org.apache.spark.ml.clustering._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.fpm.FPGrowthModel
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.ml.regression._
import org.apache.spark.ml.transformer.{ServingModel, ServingPipelineModel, ServingStage, ServingTrans}
import org.apache.spark.ml.tuning.{CrossValidatorModel, TrainValidationSplitModel}
import org.apache.spark.ml.tunning.{CrossValidatorServingModel, TrainValidationSplitServingModel}
import org.apache.spark.ml.util.DefaultParamsReader
import org.apache.spark.ml._
import org.apache.spark.sql.SparkSession


object ModelUtils {

  def loadMetadata(path: String, spark: SparkSession): MetaSnapshot = {
    val metadata = DefaultParamsReader.loadMetadata(path, spark.sparkContext)

    MetaSnapshot(metadata.uid, metadata.className, metadata.sparkVersion)
  }

  def loadModel(name: String, path: String): Model[_] = {
    name match {
      case modelName if classOf[AFTSurvivalRegressionModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        AFTSurvivalRegressionModel.load(path)
      case modelName if classOf[Bucketizer].getCanonicalName.equalsIgnoreCase(modelName) =>
        Bucketizer.load(path)
      case modelName if classOf[IsotonicRegressionModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        IsotonicRegressionModel.load(path)
      case modelName if classOf[MinMaxScalerModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        MinMaxScalerModel.load(path)
      case modelName if classOf[RFormulaModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        RFormulaModel.load(path)
      case modelName if classOf[Word2VecModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        Word2VecModel.load(path)
      case modelName if classOf[KMeansModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        KMeansModel.load(path)
      case modelName if classOf[StringIndexerModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        StringIndexerModel.load(path)
      case modelName if classOf[MinHashLSHModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        MinHashLSHModel.load(path)
      case modelName if classOf[BucketedRandomProjectionLSHModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        BucketedRandomProjectionLSHModel.load(path)
      case modelName if classOf[CrossValidatorModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        CrossValidatorModel.load(path)
      case modelName if classOf[DistributedLDAModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        DistributedLDAModel.load(path)
      case modelName if classOf[LocalLDAModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        LocalLDAModel.load(path)
      case modelName if classOf[MaxAbsScalerModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        MaxAbsScalerModel.load(path)
      case modelName if classOf[ImputerModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        ImputerModel.load(path)
      case modelName if classOf[TrainValidationSplitModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        TrainValidationSplitModel.load(path)
      case modelName if classOf[BisectingKMeansModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        BisectingKMeansModel.load(path)
      case modelName if classOf[RandomForestRegressionModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        RandomForestRegressionModel.load(path)
      case modelName if classOf[GBTRegressionModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        GBTRegressionModel.load(path)
      case modelName if classOf[GeneralizedLinearRegressionModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        GeneralizedLinearRegressionModel.load(path)
      case modelName if classOf[LinearRegressionModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        LinearRegressionModel.load(path)
      case modelName if classOf[DecisionTreeRegressionModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        DecisionTreeRegressionModel.load(path)
      case modelName if classOf[LinearSVCModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        LinearSVCModel.load(path)
      case modelName if classOf[NaiveBayesModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        NaiveBayesModel.load(path)
      case modelName if classOf[GBTClassificationModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        GBTClassificationModel.load(path)
      case modelName if classOf[DecisionTreeClassificationModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        DecisionTreeClassificationModel.load(path)
      case modelName if classOf[MultilayerPerceptronClassificationModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        MultilayerPerceptronClassificationModel.load(path)
      case modelName if classOf[LogisticRegressionModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        LogisticRegressionModel.load(path)
      case modelName if classOf[RandomForestClassificationModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        RandomForestClassificationModel.load(path)
      case modelName if classOf[GaussianMixtureModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        GaussianMixtureModel.load(path)
      case modelName if classOf[PCAModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        PCAModel.load(path)
      case modelName if classOf[PipelineModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        PipelineModel.load(path)
      case modelName if classOf[ChiSqSelectorModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        ChiSqSelectorModel.load(path)
      case modelName if classOf[VectorIndexerModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        VectorIndexerModel.load(path)
      case modelName if classOf[ALSModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        ALSModel.load(path)
      case modelName if classOf[OneHotEncoderModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        OneHotEncoderModel.load(path)
      case modelName if classOf[OneVsRestModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        OneVsRestModel.load(path)
      case modelName if classOf[FPGrowthModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        FPGrowthModel.load(path)
      case modelName if classOf[StandardScalerModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        StandardScalerModel.load(path)
      case modelName if classOf[CountVectorizerModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        CountVectorizerModel.load(path)
      case modelName if classOf[IDFModel].getCanonicalName.equalsIgnoreCase(modelName) =>
        IDFModel.load(path)
    }
  }


  def transModel(sparkModel: Model[_]): ServingModel[_] = {
    sparkModel match {
      case model: PipelineModel =>
        val serStages = model.stages.map(trans)
        new ServingPipelineModel(model.uid, serStages)
      case model => {
        trans(model).asInstanceOf[ServingModel[_]]
      }
    }
  }

  def transTransformer(sparkModel: Transformer): ServingTrans = {
    sparkModel match {
      case model: PipelineModel =>
        val serStages = model.stages.map(trans)
        new ServingPipelineModel(model.uid, serStages)
      case model => trans(model)
    }
  }

  private def trans(stage: PipelineStage): ServingTrans = {
    stage match {
      case transformer: AFTSurvivalRegressionModel => AFTSurvivalRegressionServingModel(transformer)
      case transformer: Bucketizer => BucketizerServing(transformer)
      case transformer: IsotonicRegressionModel => IsotonicRegressionServingModel(transformer)
      case transformer: MinMaxScalerModel => MinMaxScalerServingModel(transformer)
      case transformer: RFormulaModel => RFormulaServingModel(transformer)
      case transformer: Word2VecModel => ???
      case transformer: KMeansModel => KMeansServingModel(transformer)
      case transformer: StringIndexerModel => StringIndexerServingModel(transformer)
      case transformer: MinHashLSHModel => MinHashLSHServingModel(transformer)
      case transformer: BucketedRandomProjectionLSHModel => BucketedRandomProjectionLSHServingModel(transformer)
      case transformer: CrossValidatorModel => CrossValidatorServingModel(transformer)
      case transformer: DistributedLDAModel => ???
      case transformer: LocalLDAModel => LocalLDAServingModel(transformer)
      case transformer: MaxAbsScalerModel => MaxAbsScalerServingModel(transformer)
      case transformer: ImputerModel => ???
      case transformer: TrainValidationSplitModel => TrainValidationSplitServingModel(transformer)
      case transformer: BisectingKMeansModel => BisectingKMeansServingModel(transformer)
      case transformer: RandomForestRegressionModel => RandomForestRegressionServingModel(transformer)
      case transformer: GBTRegressionModel => GBTRegressionServingModel(transformer)
      case transformer: GeneralizedLinearRegressionModel => GeneralizedLinearRegressionServingModel(transformer)
      case transformer: LinearRegressionModel => LinearRegressionServingModel(transformer)
      case transformer: DecisionTreeRegressionModel => DecisionTreeRegressionServingModel(transformer)
      case transformer: LinearSVCModel => LinearSVCServingModel(transformer)
      case transformer: NaiveBayesModel => NaiveBayesServingModel(transformer)
      case transformer: GBTClassificationModel => GBTClassificationServingModel(transformer)
      case transformer: DecisionTreeClassificationModel => DecisionTreeClassificationServingModel(transformer)
      case transformer: MultilayerPerceptronClassificationModel => MultilayerPerceptronClassificationServingModel(transformer)
      case transformer: LogisticRegressionModel => LogisticRegressionServingModel(transformer)
      case transformer: RandomForestClassificationModel => RandomForestClassificationServingModel(transformer)
      case transformer: GaussianMixtureModel => GaussianMixtureServingModel(transformer)
      case transformer: PCAModel => PCAServingModel(transformer)
      case transformer: ChiSqSelectorModel => ChiSqSelectorServingModel(transformer)
      case transformer: VectorIndexerModel => VectorIndexerServingModel(transformer)
      case transformer: ALSModel => ???
      case transformer: OneHotEncoderModel => OneHotEncoderServingModel(transformer)//use value to create SCol
      case transformer: OneVsRestModel => OneVsRestServingModel(transformer)
      case transformer: FPGrowthModel => ???
      case transformer: StandardScalerModel => StandardScalerServingModel(transformer)
      case transformer: CountVectorizerModel => CountVectorizerServingModel(transformer)
      case transformer: IDFModel => IDFServingModel(transformer)
      case transformer: VectorSizeHint => VectorSizeHintServing(transformer)
      case transformer: FeatureHasher => FeatureHasherServing(transformer)
      case transformer: SQLTransformer => ???
      case transformer: ElementwiseProduct => ElementwiseProductServing(transformer)
      case transformer: Tokenizer => TokenizerServing(transformer)
      case transformer: RegexTokenizer => RegexTokenizerServing(transformer)
      case transformer: Normalizer => NormalizerServing(transformer)
      case transformer: NGram => NGramServing(transformer)
      case transformer: DCT => DCTServing(transformer)
      case transformer: PolynomialExpansion => PolynomialExpansionServing(transformer)
      case transformer: VectorSlicer => VectorSlicerServing(transformer)
      case transformer: Binarizer => BinarizerServing(transformer)
      case transformer: HashingTF => HashingTFServing(transformer)
      case transformer: StopWordsRemover => StopWordsRemoverServing(transformer)
      case transformer: IndexToString => IndexToStringServing(transformer)
      case transformer: VectorAssembler => VectorAssemblerServing(transformer)
      case transformer: Interaction => InteractionServing(transformer)
      case transformer: VectorAttributeRewriter => VectorAttributeRewriterServing(transformer)
      case transformer: ColumnPruner => ColumnPrunerServing(transformer)
    }
  }


  def loadTransformer(name: String, path: String): Transformer = {
    name match {
      case modelName if classOf[VectorSizeHint].getCanonicalName.equalsIgnoreCase(modelName) =>
        VectorSizeHint.load(path)
      case modelName if classOf[FeatureHasher].getCanonicalName.equalsIgnoreCase(modelName) =>
        FeatureHasher.load(path)
      case modelName if classOf[SQLTransformer].getCanonicalName.equalsIgnoreCase(modelName) =>
        SQLTransformer.load(path)
      case modelName if classOf[ElementwiseProduct].getCanonicalName.equalsIgnoreCase(modelName) =>
        ElementwiseProduct.load(path)
      case modelName if classOf[Tokenizer].getCanonicalName.equalsIgnoreCase(modelName) =>
        Tokenizer.load(path)
      case modelName if classOf[RegexTokenizer].getCanonicalName.equalsIgnoreCase(modelName) =>
        RegexTokenizer.load(path)
      case modelName if classOf[Normalizer].getCanonicalName.equalsIgnoreCase(modelName) =>
        Normalizer.load(path)
      case modelName if classOf[NGram].getCanonicalName.equalsIgnoreCase(modelName) =>
        NGram.load(path)
      case modelName if classOf[DCT].getCanonicalName.equalsIgnoreCase(modelName) =>
        DCT.load(path)
      case modelName if classOf[PolynomialExpansion].getCanonicalName.equalsIgnoreCase(modelName) =>
        PolynomialExpansion.load(path)
      case modelName if classOf[VectorSlicer].getCanonicalName.equalsIgnoreCase(modelName) =>
        VectorSlicer.load(path)
      case modelName if classOf[Binarizer].getCanonicalName.equalsIgnoreCase(modelName) =>
        Binarizer.load(path)
      case modelName if classOf[HashingTF].getCanonicalName.equalsIgnoreCase(modelName) =>
        HashingTF.load(path)
      case modelName if classOf[StopWordsRemover].getCanonicalName.equalsIgnoreCase(modelName) =>
        StopWordsRemover.load(path)
      case modelName if classOf[IndexToString].getCanonicalName.equalsIgnoreCase(modelName) =>
        IndexToString.load(path)
      case modelName if classOf[VectorAssembler].getCanonicalName.equalsIgnoreCase(modelName) =>
        VectorAssembler.load(path)
      case modelName if classOf[Interaction].getCanonicalName.equalsIgnoreCase(modelName) =>
        Interaction.load(path)
      case modelName if classOf[VectorAttributeRewriter].getCanonicalName.equalsIgnoreCase(modelName) =>
        VectorAttributeRewriter.load(path)
      case modelName if classOf[ColumnPruner].getCanonicalName.equalsIgnoreCase(modelName) =>
        ColumnPruner.load(path)
    }
  }
}
