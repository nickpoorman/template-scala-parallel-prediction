package org.template.prediction

import io.prediction.controller._
import io.prediction.data.storage.Event
import io.prediction.data.storage.Storage

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import grizzled.slf4j.Logger

case class DataSourceParams(
  appId: Int,
  evalK: Option[Int] // define the k-fold parameter.
  ) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData, EmptyEvaluationInfo, Query, ActualResult] {

  @transient lazy val logger = Logger[this.type]

  override def readTraining(sc: SparkContext): TrainingData = {
    val eventsDb = Storage.getPEvents()
    val eventsRDD: RDD[Event] = eventsDb.find(
      appId = dsp.appId,
      entityType = Some("user"),
      eventNames = Some(List("rate")), // read "rate" event
      // targetEntityType is optional field of an event.
      targetEntityType = Some(Some("item")))(sc)

    val ratingsRDD: RDD[Rating] = eventsRDD.map { event =>
      val rating = try {
        val ratingValue: Double = event.event match {
          case "rate" => event.properties.get[Double]("rating")
          case _      => throw new Exception(s"Unexpected event ${event} is read.")
        }
        // entityId and targetEntityId is String
        Rating(event.entityId,
          event.targetEntityId.get,
          ratingValue)
      } catch {
        case e: Exception => {
          logger.error(s"Cannot convert ${event} to Rating. Exception: ${e}.")
          throw e
        }
      }
      rating
    }.cache()

    new TrainingData(ratingsRDD)
  }

  override def readEval(sc: SparkContext): Seq[(TrainingData, EmptyEvaluationInfo, RDD[(Query, ActualResult)])] = {
    require(!dsp.evalK.isEmpty, "DataSourceParams.evalK must not be None")

    // The following code reads the data from data store. It is equivalent to
    // the readTraining method. We copy-and-paste the exact code here for
    // illustration purpose, a recommended approach is to factor out this logic
    // into a helper function and have both readTraining and readEval call the
    // helper.
    val eventsDb = Storage.getPEvents()
    val eventsRDD: RDD[Event] = eventsDb.find(
      appId = dsp.appId,
      entityType = Some("user"),
      eventNames = Some(List("rate")), // read "rate" event
      // targetEntityType is optional field of an event.
      targetEntityType = Some(Some("item")))(sc)

    val ratingsRDD: RDD[Rating] = eventsRDD.map { event => // ratingsRDD contains the complete dataset with which we use to generate the (training, validation) sequence
      val rating = try {
        val ratingValue: Double = event.event match {
          case "rate" => event.properties.get[Double]("rating")
          case _      => throw new Exception(s"Unexpected event ${event} is read.")
        }
        // entityId and targetEntityId is String
        Rating(event.entityId,
          event.targetEntityId.get,
          ratingValue)
      } catch {
        case e: Exception => {
          logger.error(s"Cannot convert ${event} to Rating. Exception: ${e}.")
          throw e
        }
      }
      rating
    }.cache()
    // End of reading from data store

    // K-fold splitting
    val evalK = dsp.evalK.get
    //    val indexedRatings: RDD[(Rating, Long)] = ratingsRDD.zipWithIndex
    //        (0 until evalK).map { idx =>
    //      val trainingRatings = indexedRatings.filter(_._2 % evalK != idx).map(_._1)
    //      val testingRatings = indexedRatings.filter(_._2 % evalK == idx).map(_._1)
    //
    //      (
    //        new TrainingData(trainingRatings),
    //        new EmptyEvaluationInfo(),
    //        testingRatings.map {
    //          r => (new Query(r.user, r.item), new ActualResult(r.rating))
    //        }
    //      )
    //    }

    //// Need to split by user and by items

    //    val indexedItems = ratingsRDD.map(_.item).distinct().zipWithIndex()
    //    (0 until evalK).map { idx =>
    //      val trainingItems = indexedItems.filter(_._2 % evalK != idx).map(_._1).distinct().collect()
    //      val testingItems = indexedItems.filter(_._2 % evalK == idx).map(_._1).distinct().collect()
    //
    //      // group the ratings by which fold they are in
    //      val trainingRatings = ratingsRDD.filter { rating => trainingItems.contains(rating.item) }
    //      val trainingUsers = trainingRatings.map(_.user).distinct().collect()
    //      val testingRatings = ratingsRDD.filter { rating => testingItems.contains(rating.item) }.filter { r => trainingUsers.contains(r.user) && trainingItems.contains(r.item) }
    //
    //      (
    //        new TrainingData(trainingRatings),
    //        new EmptyEvaluationInfo(),
    //        testingRatings.map {
    //          r => (new Query(r.user, r.item), new ActualResult(r.rating))
    //        }
    //      )
    //    }

    ////// can't figure out why this does not work...
    // we need to first map all the trainingRatings
    val userIds = ratingsRDD.map(_.user).distinct().collect()
    // TODO: ? get rid of any users that have less than the number of evalK

    (0 until evalK).map { idx =>
      // then for each of the users we want to split their ratings into two groups
      val trainingRatings = userIds.map { user =>
        // get all the ratings for this user
        val userRatings = ratingsRDD.filter(_.user == user).zipWithIndex()
        // we want to get only the training ratings
        userRatings.filter(_._2 % evalK != idx).map(_._1)
      }.reduce((l, r) => l.union(r))

      // get rid of any items in the test set that were not in the training set
      // get rid of any users in the test set that were not in the training set
      val trainingUsers = trainingRatings.map(_.user).distinct().collect()
      val trainingItems = trainingRatings.map(_.item).distinct().collect()

      val testingRatings = userIds.map { user =>
        // get all the ratings for this user
        val userRatings = ratingsRDD.filter(_.user == user).zipWithIndex()
        // we want to get only the testingRatings ratings
        userRatings.filter(_._2 % evalK == idx).map(_._1)
      }.reduce((l, r) => l.union(r)).filter { r =>
        trainingUsers.contains(r.user) && trainingItems.contains(r.item)
      }

      (
        new TrainingData(trainingRatings),
        new EmptyEvaluationInfo(),
        testingRatings.map {
          r => (new Query(r.user, r.item), new ActualResult(r.rating))
        }
      )
    }

  }

}

//case class FoldData(
//  index: Int,
//  training: RDD[Rating],
//  testing: RDD[Rating]) extends Serializable

case class Rating(
  user: String,
  item: String,
  rating: Double)

class TrainingData(
  val ratings: RDD[Rating]) extends Serializable with SanityCheck {
  override def toString = {
    s"ratings: [${ratings.count()}] (${ratings.take(2).toList}...)"
  }

  override def sanityCheck(): Unit = {
    require(!ratings.take(1).isEmpty, s"ratings cannot be empty!")
  }
}