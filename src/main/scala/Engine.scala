package org.template.prediction

import io.prediction.controller.IEngineFactory
import io.prediction.controller.Engine

case class Query(
  user: String,
  item: String) extends Serializable

case class PredictedResult(
  val score: Double) extends Serializable

case class ActualResult(
  val score: Double) extends Serializable

object PredictionEngine extends IEngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("als" -> classOf[ALSAlgorithm]),
      classOf[Serving])
  }
}