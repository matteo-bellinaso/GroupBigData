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


  private def groupByTime(rdd: RDD[T]) = {
    val mappedRdd = rdd.map {case (_, _, actor: Actor, _, _, time: String, _) => {(actor, time)}}
    val timeRdd = mappedRdd.map {case (actor: Actor, time: String) =>
      val temp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
      val tempo = new DateTime( temp.parse(time))
      (tempo.getMinuteOfHour(), actor)
    }
    timeRdd
  }
}
