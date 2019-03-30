package operations.RDDOperation

import entity._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Encoders

class CommitRddOperation[T] {

  // encoder per parsare la classe
  private val commitEncoder = Encoders.product[Commit]

  def getCommitCountFromRDD(rdd: RDD[T]): Long = {
    val rdd1 = rdd.map { case (_, _, _, _, _, _, payload: Payload)=> (payload) }
    val rdd2 = rdd1.filter(x => (x.commits != null && x.commits.size > 0))
    val count = rdd2.aggregate(0)((x,y) => (x + y.commits.size),(x,y) => (x + y))
    count
  }

  def getCommitForActor(rdd: RDD[T]) = {
    val rdd1 = groupPerActor(rdd)
    val reduced = rdd1.reduceByKey((x, y) => (x + y)) //reduceByKey prendi i valori della prima chiave e e li somma alla seconda
    reduced
  }

  def getCommitForActorAndType(rdd: RDD[T]) = {
    val rdd1 = groupPerActorAndType(rdd)
    val reduced  = rdd1.reduceByKey((x,y) => (x+y))
    reduced
  }

  def getMaxCommitPerActor(rdd: RDD[T]) = {
    val rdd1 = groupPerActor(rdd)
    val max = rdd1.reduceByKey((x,y) => {if(x > y) x else y})
    max
  }

  def getMinCommitPerActor(rdd: RDD[T]) = {
    val rdd1 = groupPerActor(rdd)
    val min = rdd1.reduceByKey((x,y) => {if(x < y) x else y})
    min
  }

  private def groupPerActor(rdd: RDD[T]): RDD[(Actor, Int )] = {
    val grouppedRdd = rdd.map{ case (_, _, actor: Actor, _, _, _, payload: Payload) => (actor, payload.commits.size) }
    grouppedRdd
  }

  private def groupPerActorAndType(rdd: RDD[T]): RDD[((Actor,String), Int )] = {
    val grouppedRdd = rdd.map{ case (_, tipo: String, actor: Actor, _, _, _, payload: Payload) => ((actor,tipo), payload.commits.size) }
    grouppedRdd
  }
}
