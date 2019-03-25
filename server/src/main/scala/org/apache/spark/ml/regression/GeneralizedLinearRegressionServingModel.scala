package org.apache.spark.ml.regression

import java.util.Locale

import breeze.stats.{distributions => dist}
import org.apache.spark.ml.data.{SCol, SDFrame, UDF}
import org.apache.spark.ml.feature.OffsetInstance
import org.apache.spark.ml.linalg.{BLAS, Vector, Vectors}
import org.apache.spark.ml.optim.WeightedLeastSquaresModel
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.feature.PredictionServingModel
import org.apache.spark.rdd.RDD

class GeneralizedLinearRegressionServingModel(stage: GeneralizedLinearRegressionModel)
  extends PredictionServingModel[Vector, GeneralizedLinearRegressionServingModel] {
  import GeneralizedLinearRegressionServingModel._

  override def copy(extra: ParamMap): GeneralizedLinearRegressionServingModel = {
    new GeneralizedLinearRegressionServingModel(stage.copy(extra))
  }

  override def predict(features: Vector): Double = {
    predict(features, 0.0)
  }

  lazy val familyAndLink = FamilyAndLink(stage)

  def predict(features: Vector, offset: Double): Double = {
    val eta = predictLink(features, offset)
    familyAndLink.fitted(eta)
  }

  def predictLink(features: Vector, offset: Double): Double = {
    BLAS.dot(features, stage.coefficients) + stage.intercept + offset
  }

  override def transform(dataset: SDFrame): SDFrame = {
    transformSchema(dataset.schema)
    transformImpl(dataset)
  }

  override def transformImpl(dataset: SDFrame): SDFrame = {
    val predictUDF = UDF.make[Double, Vector, Double](
      (feature, offset) => predict(feature, offset))
    val predictLinkUDF = UDF.make[Double, Vector, Double](
      (feature, offset) => predictLink(feature, offset))

    var output = dataset
    val offSetUDF = UDF.make[Double]( () => 0.0)//todo:create a new Col `offset`
    val offset = if (!hasOffsetCol) offSetUDF.apply($(stage.offsetCol)) else SCol($(stage.offsetCol))

    if ($(stage.predictionCol).nonEmpty) {
      output = output.withColum(predictUDF.apply(${stage.predictionCol}, SCol(${stage.featuresCol}), offset))
    }
    if (hasLinkPredictionCol) {
      output = output.withColum(predictLinkUDF.apply(${stage.linkPredictionCol}, SCol(${stage.featuresCol}), offset))
    }
    output
  }

  override val uid: String = stage.uid

  private def hasLinkPredictionCol: Boolean = {
    isDefined(stage.linkPredictionCol) && $(stage.linkPredictionCol).nonEmpty
  }

  private def hasOffsetCol: Boolean =
    isSet(stage.offsetCol) && $(stage.offsetCol).nonEmpty

}

object GeneralizedLinearRegressionServingModel{

  def apply(stage: GeneralizedLinearRegressionModel): GeneralizedLinearRegressionServingModel =
    new GeneralizedLinearRegressionServingModel(stage)

  private val epsilon: Double = 1E-16

  private class FamilyAndLink(val family: Family, val link: Link) extends Serializable {

    /** Linear predictor based on given mu. */
    def predict(mu: Double): Double = link.link(family.project(mu))

    /** Fitted value based on linear predictor eta. */
    def fitted(eta: Double): Double = family.project(link.unlink(eta))


    /**
      * The reweight function used to update working labels and weights
      * at each iteration of [[org.apache.spark.ml.optim.IterativelyReweightedLeastSquares]].
      */
    val reweightFunc: (OffsetInstance, WeightedLeastSquaresModel) => (Double, Double) = {
      (instance: OffsetInstance, model: WeightedLeastSquaresModel) => {
        val eta = model.predict(instance.features) + instance.offset
        val mu = fitted(eta)
        val newLabel = eta - instance.offset + (instance.label - mu) * link.deriv(mu)
        val newWeight = instance.weight / (math.pow(this.link.deriv(mu), 2.0) * family.variance(mu))
        (newLabel, newWeight)
      }
    }
  }

  private object FamilyAndLink {

    /**
      * Constructs the FamilyAndLink object from a parameter map
      */
    def apply(stage: GeneralizedLinearRegressionModel): FamilyAndLink = {
      val familyObj = Family.fromParams(stage)
      val linkObj =
        if ((stage.getFamily.toLowerCase(Locale.ROOT) != "tweedie" &&
          stage.isSet(stage.link)) ||
          (stage.getFamily.toLowerCase(Locale.ROOT) == "tweedie" &&
            stage.isSet(stage.linkPower))) {
          Link.fromParams(stage)
        } else {
          familyObj.defaultLink
        }
      new FamilyAndLink(familyObj, linkObj)
    }
  }

  /**
    * A description of the error distribution to be used in the model.
    *
    * @param name the name of the family.
    */
  private abstract class Family(val name: String) extends Serializable {

    /** The default link instance of this family. */
    val defaultLink: Link

    /** Initialize the starting value for mu. */
    def initialize(y: Double, weight: Double): Double

    /** The variance of the endogenous variable's mean, given the value mu. */
    def variance(mu: Double): Double

    /** Deviance of (y, mu) pair. */
    def deviance(y: Double, mu: Double, weight: Double): Double

    /**
      * Akaike Information Criterion (AIC) value of the family for a given dataset.
      *
      * @param predictions an RDD of (y, mu, weight) of instances in evaluation dataset
      * @param deviance the deviance for the fitted model in evaluation dataset
      * @param numInstances number of instances in evaluation dataset
      * @param weightSum weights sum of instances in evaluation dataset
      */
    def aic(
             predictions: RDD[(Double, Double, Double)],
             deviance: Double,
             numInstances: Double,
             weightSum: Double): Double

    /** Trim the fitted value so that it will be in valid range. */
    def project(mu: Double): Double = mu
  }

  private object Family {

    /**
      * Gets the [[Family]] object based on param family and variancePower.
      * If param family is set with "gaussian", "binomial", "poisson" or "gamma",
      * return the corresponding object directly; otherwise, construct a Tweedie object
      * according to variancePower.
      *
      * @param stage the parameter map containing family name and variance power
      */
    def fromParams(stage: GeneralizedLinearRegressionModel): Family = {
      stage.getFamily.toLowerCase(Locale.ROOT) match {
        case Gaussian.name => Gaussian
        case Binomial.name => Binomial
        case Poisson.name => Poisson
        case Gamma.name => Gamma
        case "tweedie" =>
          stage.getVariancePower match {
            case 0.0 => Gaussian
            case 1.0 => Poisson
            case 2.0 => Gamma
            case others => new Tweedie(others)
          }
      }
    }
  }

  private abstract class Link(val name: String) extends Serializable {

    /** The link function. */
    def link(mu: Double): Double

    /** Derivative of the link function. */
    def deriv(mu: Double): Double

    /** The inverse link function. */
    def unlink(eta: Double): Double
  }

  private object Link {

    /**
      * Gets the [[Link]] object based on param family, link and linkPower.
      * If param family is set with "tweedie", return or construct link function object
      * according to linkPower; otherwise, return link function object according to link.
      *
      * @param params the parameter map containing family, link and linkPower
      */
    def fromParams(params: GeneralizedLinearRegressionModel): Link = {
      if (params.getFamily.toLowerCase(Locale.ROOT) == "tweedie") {
        params.getLinkPower match {
          case 0.0 => Log
          case 1.0 => Identity
          case -1.0 => Inverse
          case 0.5 => Sqrt
          case others => new Power(others)
        }
      } else {
        params.getLink.toLowerCase(Locale.ROOT) match {
          case Identity.name => Identity
          case Logit.name => Logit
          case Log.name => Log
          case Inverse.name => Inverse
          case Probit.name => Probit
          case CLogLog.name => CLogLog
          case Sqrt.name => Sqrt
        }
      }
    }
  }

  /**
    * Tweedie exponential family distribution.
    * This includes the special cases of Gaussian, Poisson and Gamma.
    */
  private class Tweedie(val variancePower: Double)
    extends Family("tweedie") {

    override val defaultLink: Link = new Power(1.0 - variancePower)

    override def initialize(y: Double, weight: Double): Double = {
      if (variancePower >= 1.0 && variancePower < 2.0) {
        require(y >= 0.0, s"The response variable of $name($variancePower) family " +
          s"should be non-negative, but got $y")
      } else if (variancePower >= 2.0) {
        require(y > 0.0, s"The response variable of $name($variancePower) family " +
          s"should be positive, but got $y")
      }
      if (y == 0) Tweedie.delta else y
    }

    override def variance(mu: Double): Double = math.pow(mu, variancePower)

    private def yp(y: Double, mu: Double, p: Double): Double = {
      if (p == 0) {
        math.log(y / mu)
      } else {
        (math.pow(y, p) - math.pow(mu, p)) / p
      }
    }

    override def deviance(y: Double, mu: Double, weight: Double): Double = {
      // Force y >= delta for Poisson or compound Poisson
      val y1 = if (variancePower >= 1.0 && variancePower < 2.0) {
        math.max(y, Tweedie.delta)
      } else {
        y
      }
      2.0 * weight *
        (y * yp(y1, mu, 1.0 - variancePower) - yp(y, mu, 2.0 - variancePower))
    }

    override def aic(
                      predictions: RDD[(Double, Double, Double)],
                      deviance: Double,
                      numInstances: Double,
                      weightSum: Double): Double = {
      /*
       This depends on the density of the Tweedie distribution.
       Only implemented for Gaussian, Poisson and Gamma at this point.
      */
      throw new UnsupportedOperationException("No AIC available for the tweedie family")
    }

    override def project(mu: Double): Double = {
      if (mu < epsilon) {
        epsilon
      } else if (mu.isInfinity) {
        Double.MaxValue
      } else {
        mu
      }
    }
  }

  private object Tweedie{

    /** Constant used in initialization and deviance to avoid numerical issues. */
    val delta: Double = 0.1
  }

  /**
    * Gaussian exponential family distribution.
    * The default link for the Gaussian family is the identity link.
    */
  private object Gaussian extends Tweedie(0.0) {

    override val name: String = "gaussian"

    override val defaultLink: Link = Identity

    override def initialize(y: Double, weight: Double): Double = y

    override def variance(mu: Double): Double = 1.0

    override def deviance(y: Double, mu: Double, weight: Double): Double = {
      weight * (y - mu) * (y - mu)
    }

    override def aic(
                      predictions: RDD[(Double, Double, Double)],
                      deviance: Double,
                      numInstances: Double,
                      weightSum: Double): Double = {
      val wt = predictions.map(x => math.log(x._3)).sum()
      numInstances * (math.log(deviance / numInstances * 2.0 * math.Pi) + 1.0) + 2.0 - wt
    }

    override def project(mu: Double): Double = {
      if (mu.isNegInfinity) {
        Double.MinValue
      } else if (mu.isPosInfinity) {
        Double.MaxValue
      } else {
        mu
      }
    }
  }

  /**
    * Binomial exponential family distribution.
    * The default link for the Binomial family is the logit link.
    */
  private object Binomial extends Family("binomial") {

    val defaultLink: Link = Logit

    override def initialize(y: Double, weight: Double): Double = {
      val mu = (weight * y + 0.5) / (weight + 1.0)
      require(mu > 0.0 && mu < 1.0, "The response variable of Binomial family" +
        s"should be in range (0, 1), but got $mu")
      mu
    }

    override def variance(mu: Double): Double = mu * (1.0 - mu)

    private def ylogy(y: Double, mu: Double): Double = {
      if (y == 0) 0.0 else y * math.log(y / mu)
    }

    override def deviance(y: Double, mu: Double, weight: Double): Double = {
      2.0 * weight * (ylogy(y, mu) + ylogy(1.0 - y, 1.0 - mu))
    }

    override def aic(
                      predictions: RDD[(Double, Double, Double)],
                      deviance: Double,
                      numInstances: Double,
                      weightSum: Double): Double = {
      -2.0 * predictions.map { case (y: Double, mu: Double, weight: Double) =>
        // weights for Binomial distribution correspond to number of trials
        val wt = math.round(weight).toInt
        if (wt == 0) {
          0.0
        } else {
          dist.Binomial(wt, mu).logProbabilityOf(math.round(y * weight).toInt)
        }
      }.sum()
    }

    override def project(mu: Double): Double = {
      if (mu < epsilon) {
        epsilon
      } else if (mu > 1.0 - epsilon) {
        1.0 - epsilon
      } else {
        mu
      }
    }
  }

  /**
    * Poisson exponential family distribution.
    * The default link for the Poisson family is the log link.
    */
  private object Poisson extends Tweedie(1.0) {

    override val name: String = "poisson"

    override val defaultLink: Link = Log

    override def initialize(y: Double, weight: Double): Double = {
      require(y >= 0.0, "The response variable of Poisson family " +
        s"should be non-negative, but got $y")
      /*
        Force Poisson mean > 0 to avoid numerical instability in IRLS.
        R uses y + delta for initialization. See poisson()$initialize.
       */
      math.max(y, Tweedie.delta)
    }

    override def variance(mu: Double): Double = mu

    override def deviance(y: Double, mu: Double, weight: Double): Double = {
      2.0 * weight * (y * math.log(y / mu) - (y - mu))
    }

    override def aic(
                      predictions: RDD[(Double, Double, Double)],
                      deviance: Double,
                      numInstances: Double,
                      weightSum: Double): Double = {
      -2.0 * predictions.map { case (y: Double, mu: Double, weight: Double) =>
        weight * dist.Poisson(mu).logProbabilityOf(y.toInt)
      }.sum()
    }
  }

  /**
    * Gamma exponential family distribution.
    * The default link for the Gamma family is the inverse link.
    */
  private object Gamma extends Tweedie(2.0) {

    override val name: String = "gamma"

    override val defaultLink: Link = Inverse

    override def initialize(y: Double, weight: Double): Double = {
      require(y > 0.0, "The response variable of Gamma family " +
        s"should be positive, but got $y")
      y
    }

    override def variance(mu: Double): Double = mu * mu

    override def deviance(y: Double, mu: Double, weight: Double): Double = {
      -2.0 * weight * (math.log(y / mu) - (y - mu)/mu)
    }

    override def aic(
                      predictions: RDD[(Double, Double, Double)],
                      deviance: Double,
                      numInstances: Double,
                      weightSum: Double): Double = {
      val disp = deviance / weightSum
      -2.0 * predictions.map { case (y: Double, mu: Double, weight: Double) =>
        weight * dist.Gamma(1.0 / disp, mu * disp).logPdf(y)
      }.sum() + 2.0
    }
  }

  /** Power link function class */
  private class Power(val linkPower: Double)
    extends Link("power") {

    override def link(mu: Double): Double = {
      if (linkPower == 0.0) {
        math.log(mu)
      } else {
        math.pow(mu, linkPower)
      }
    }

    override def deriv(mu: Double): Double = {
      if (linkPower == 0.0) {
        1.0 / mu
      } else {
        linkPower * math.pow(mu, linkPower - 1.0)
      }
    }

    override def unlink(eta: Double): Double = {
      if (linkPower == 0.0) {
        math.exp(eta)
      } else {
        math.pow(eta, 1.0 / linkPower)
      }
    }
  }

  private object Identity extends Power(1.0) {

    override val name: String = "identity"

    override def link(mu: Double): Double = mu

    override def deriv(mu: Double): Double = 1.0

    override def unlink(eta: Double): Double = eta
  }

  private object Logit extends Link("logit") {

    override def link(mu: Double): Double = math.log(mu / (1.0 - mu))

    override def deriv(mu: Double): Double = 1.0 / (mu * (1.0 - mu))

    override def unlink(eta: Double): Double = 1.0 / (1.0 + math.exp(-1.0 * eta))
  }

  private object Log extends Power(0.0) {

    override val name: String = "log"

    override def link(mu: Double): Double = math.log(mu)

    override def deriv(mu: Double): Double = 1.0 / mu

    override def unlink(eta: Double): Double = math.exp(eta)
  }

  private object Inverse extends Power(-1.0) {

    override val name: String = "inverse"

    override def link(mu: Double): Double = 1.0 / mu

    override def deriv(mu: Double): Double = -1.0 * math.pow(mu, -2.0)

    override def unlink(eta: Double): Double = 1.0 / eta
  }

  private object Probit extends Link("probit") {

    override def link(mu: Double): Double = dist.Gaussian(0.0, 1.0).inverseCdf(mu)

    override def deriv(mu: Double): Double = {
      1.0 / dist.Gaussian(0.0, 1.0).pdf(dist.Gaussian(0.0, 1.0).inverseCdf(mu))
    }

    override def unlink(eta: Double): Double = dist.Gaussian(0.0, 1.0).cdf(eta)
  }

  private object CLogLog extends Link("cloglog") {

    override def link(mu: Double): Double = math.log(-1.0 * math.log(1 - mu))

    override def deriv(mu: Double): Double = 1.0 / ((mu - 1.0) * math.log(1.0 - mu))

    override def unlink(eta: Double): Double = 1.0 - math.exp(-1.0 * math.exp(eta))
  }

  private object Sqrt extends Power(0.5) {

    override val name: String = "sqrt"

    override def link(mu: Double): Double = math.sqrt(mu)

    override def deriv(mu: Double): Double = 1.0 / (2.0 * math.sqrt(mu))

    override def unlink(eta: Double): Double = eta * eta
  }
}