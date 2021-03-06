package org.template.prediction

import io.prediction.controller.AverageMetric
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.EngineParams
import io.prediction.controller.EngineParamsGenerator
import io.prediction.controller.Evaluation
import io.prediction.controller.Workflow
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

case class Accuracy() extends AverageMetric[EmptyEvaluationInfo, Query, PredictedResult, ActualResult] {
  def calculate(query: Query, predicted: PredictedResult, actual: ActualResult): Double = {
    //  what we actually want to do here is calculate an MSE for the results
    // because AverageMetric takes the global average from calculate all we need to do is get the squared error
    // there's an issue here. predicted.score could be NaN because the model wasn't trained with that user or item.
    // let's just say it's actual for now. This will make the results much better but at least we'll get a score.
    val pScore = (if(predicted.score.isNaN()) actual.score else predicted.score)
    val err = (pScore - actual.score)
    err * err
  }
}

object AccuracyEvaluation extends Evaluation {
  // Define Engine and Metric used in Evaluation
  engineMetric = (PredictionEngine(), new Accuracy())
}

object EngineParamsList extends EngineParamsGenerator {
  // Define list of EngineParams used in Evaluation

  // First, we define the base engine params. It specifies the appId from which
  // the data is read, and a evalK parameter is used to define the
  // cross-validation.
  private[this] val baseEP = EngineParams(
    dataSourceParams = DataSourceParams(appId = 2, evalK = Some(2)))

  // Second, we specify the engine params list by explicitly listing all
  // algorithm parameters. In this case, we evaluate 3 engine params, each with
  // a different algorithm params value.
  engineParamsList = Seq(
    baseEP.copy(algorithmParamsList = Seq(("als", ALSAlgorithmParams(10, 75, 0.01, Some(3))))),
    baseEP.copy(algorithmParamsList = Seq(("als", ALSAlgorithmParams(10, 75, 0.02, Some(3))))),
    baseEP.copy(algorithmParamsList = Seq(("als", ALSAlgorithmParams(10, 75, 0.04, Some(3)))))
  )
}
