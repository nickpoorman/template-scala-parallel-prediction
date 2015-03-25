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

    // FOR SOME REASON THIS WONT WORK (scala.MatchError: null) ==> https://gist.github.com/nickpoorman/5f9fc274fd8a8e02b161
    val userIds = ratingsRDD.map(_.user).distinct()
    // filter out any users that have less than evalk ratings
    val usersWithCounts =
      ratingsRDD
        .map(r => (r.user, (1, Seq[Rating](Rating(r.user, r.item, r.rating)))))
        .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2.union(v2._2)))
        .filter(_._2._1 >= evalK)

    // create evalK folds of ratings
    (0 until evalK).map { idx =>
      // start by getting this fold's ratings for each user
      val fold = usersWithCounts
        .map { userKV =>
          val userRatings = userKV._2._2.zipWithIndex
          val trainingRatings = userRatings.filter(_._2 % evalK != idx).map(_._1)
          val testingRatings = userRatings.filter(_._2 % evalK == idx).map(_._1)
          (trainingRatings, testingRatings) // split the user's ratings into a training set and a testing set
        }
        .reduce((l, r) => (l._1.union(r._1), l._2.union(r._2))) // merge all the testing and training sets into a single testing and training set

      val testingSet = fold._2.map {
        r => (new Query(r.user, r.item), new ActualResult(r.rating))
      }

      (
        new TrainingData(sc.parallelize(fold._1)),
        new EmptyEvaluationInfo(),
        sc.parallelize(testingSet)
      )

    }
  }

}

case class Rating(
  user: String,
  item: String,
  rating: Double) extends Serializable

class TrainingData(
  val ratings: RDD[Rating]) extends Serializable with SanityCheck {
  override def toString = {
    s"ratings: [${ratings.count()}] (${ratings.take(2).toList}...)"
  }

  override def sanityCheck(): Unit = {
    require(!ratings.take(1).isEmpty, s"ratings cannot be empty!")
  }
}