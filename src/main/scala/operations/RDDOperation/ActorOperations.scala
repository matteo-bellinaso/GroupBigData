package operations.RDDOperation

import java.text.SimpleDateFormat

import entity.{Actor, Repo}
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

class ActorOperations[T] {

  def getActorForHour(rdd: RDD[T]) = {
    val rdd1 = groupByTime(rdd)
    val actor = rdd1.aggregateByKey(0)(
      (x,y) => x + 1,
      (x,y) => x + y)
    actor
  }

  def getActorCountForHourAndType(rdd: RDD[T]) = {
    val rdd1 = groupByTypeAndHour(rdd)
    val actor = rdd1.aggregateByKey(0)((x,y) => x + 1, (x,y) => x + y)
    actor
  }

  def getMaxActorForHour(rdd: RDD[T]) = {
    val rdd1 = getActorForHour(rdd)
    val rddMax = rdd1.reduceByKey((x,y) => { if(x > y) x else y})
    rddMax
  }

  def getMinActorForHour(rdd: RDD[T]) = {
    val rdd1 = getActorForHour(rdd)
    val rddMax = rdd1.reduceByKey((x,y) => { if(x < y) x else y})
    rddMax
  }

  private def groupByTime(rdd: RDD[T]) = {
    val mappedRdd = rdd.map {case (_, _, actor: Actor, _, _, time: String, _) => {(actor, time)}}
    val timeRdd = mappedRdd.map {case (actor: Actor, time: String) =>
      val temp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
      val tempo = new DateTime( temp.parse(time))
      (tempo.getMinuteOfHour(), actor)
    }
    timeRdd
  }

  private def groupByTypeAndHour(rdd:  RDD[T]) = {
    val mappedRdd = rdd.map {case (_, tipo: String, actor: Actor, _, _, time: String, _) => {((time,tipo), actor)}}
    val timeRdd = mappedRdd.map {case ((time: String, tipo: String), actor: Actor) =>
      val temp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
      val tempo = new DateTime( temp.parse(time))
      ((tempo.getMinuteOfHour(), tipo), actor)
    }
    timeRdd
  }
}
