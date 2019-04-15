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
      case modelName if classOf[VectorSizeHint].getSimpleName.equalsIgnoreCase(modelName) =>
        VectorSizeHint.load(path)
      case modelName if classOf[FeatureHasher].getSimpleName.equalsIgnoreCase(modelName) =>
        FeatureHasher.load(path)
      case modelName if classOf[SQLTransformer].getSimpleName.equalsIgnoreCase(modelName) =>
        SQLTransformer.load(path)
      case modelName if classOf[ElementwiseProduct].getSimpleName.equalsIgnoreCase(modelName) =>
        ElementwiseProduct.load(path)
      case modelName if classOf[Tokenizer].getSimpleName.equalsIgnoreCase(modelName) =>
        Tokenizer.load(path)
      case modelName if classOf[RegexTokenizer].getSimpleName.equalsIgnoreCase(modelName) =>
        RegexTokenizer.load(path)
      case modelName if classOf[Normalizer].getSimpleName.equalsIgnoreCase(modelName) =>
        Normalizer.load(path)
      case modelName if classOf[NGram].getSimpleName.equalsIgnoreCase(modelName) =>
        NGram.load(path)
      case modelName if classOf[DCT].getSimpleName.equalsIgnoreCase(modelName) =>
        DCT.load(path)
      case modelName if classOf[PolynomialExpansion].getSimpleName.equalsIgnoreCase(modelName) =>
        PolynomialExpansion.load(path)
      case modelName if classOf[VectorSlicer].getSimpleName.equalsIgnoreCase(modelName) =>
        VectorSlicer.load(path)
      case modelName if classOf[Binarizer].getSimpleName.equalsIgnoreCase(modelName) =>
        Binarizer.load(path)
      case modelName if classOf[HashingTF].getSimpleName.equalsIgnoreCase(modelName) =>
        HashingTF.load(path)
      case modelName if classOf[StopWordsRemover].getSimpleName.equalsIgnoreCase(modelName) =>
        StopWordsRemover.load(path)
      case modelName if classOf[IndexToString].getSimpleName.equalsIgnoreCase(modelName) =>
        IndexToString.load(path)
      case modelName if classOf[VectorAssembler].getSimpleName.equalsIgnoreCase(modelName) =>
        VectorAssembler.load(path)
      case modelName if classOf[Interaction].getSimpleName.equalsIgnoreCase(modelName) =>
        Interaction.load(path)
      case modelName if classOf[VectorAttributeRewriter].getSimpleName.equalsIgnoreCase(modelName) =>
        VectorAttributeRewriter.load(path)
      case modelName if classOf[ColumnPruner].getSimpleName.equalsIgnoreCase(modelName) =>
        ColumnPruner.load(path)
    }
  }
}
