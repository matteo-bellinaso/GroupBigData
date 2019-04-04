package operations.DataSetOperations

import converter.Converter
import entity.{Actor, Commit, Payload, Repo}
import org.apache.spark.sql.{Dataset, SparkSession}


class CommitDataSetOperation[T](sparkSession: SparkSession) {

  import sparkSession.sqlContext.implicits._

  def getCommitCount(dataSet: Dataset[T]) = {

    val mapped = dataSet.map { case (id: String, tipo: String, actor: Actor, pubblico: Boolean, repo: Repo, created_at: String, payload: Payload)
    => (payload.push_id, payload)
    }.filter(x => x != null)

    val filtered = mapped.filter(x => x._2 != null && x._2.commits != null)
    val finalMap = filtered.map { case (id: BigInt, payload: Payload) => (id, payload.commits.size) }
    var tot = 0
    finalMap.collect.foreach(x => tot = tot + x._2)
    tot
  }

  def getCommitPerActor(dataset: Dataset[T]) = {
    val mapped = dataset.map { case (id: String, tipo: String, actor: Actor, pubblico: Boolean, repo: Repo, created_at: String, payload: Payload)
    => (actor, payload)
    }.filter(x => x != null)
    val filtered = mapped.filter(x => x._2 != null && x._2.commits != null)
    val finalMap = filtered.map { case (actor: Actor, payload: Payload) => (actor, payload.commits.size)}.groupBy( "_1").sum("_2")
    finalMap.as[(Actor, BigInt)]
  }

  def getCommitPerActorAndType(dataset: Dataset[T]) = {
    val mapped = dataset.map { case (id: String, tipo: String, actor: Actor, pubblico: Boolean, repo: Repo, created_at: String, payload: Payload)
    => ((actor,tipo), payload)
    }.filter(x => x != null)
    val filtered = mapped.filter(x => x._2 != null && x._2.commits != null)
    val finalMap = filtered.map { case ((actor: Actor, tipo: String),payload: Payload) => ((actor,tipo), payload.commits.size)}.groupBy( "_1").sum("_2")
    finalMap.as[((Actor, String), BigInt)]
  }

  def getMaxCommitPerHour(dataset: Dataset[T]) = {
    val mapped = dataset.map { case (id: String, tipo: String, actor: Actor, pubblico: Boolean, repo: Repo, created_at: String, payload: Payload)
    => (created_at, payload)
    }.filter(x => x != null)
    val filtered = mapped.filter(x => x._2 != null && x._2.commits != null)
    val time = filtered.withColumn("time", Converter.convertColumnTime($"_1")).as[(String, Payload, String)]
    val finalDS = time.map{case (tempo: String, payload: Payload,  time: String) => (time, payload.commits.size)}
    val group = finalDS.groupBy("_1").sum("_2").as[(String, BigInt)]
    val grouped = group.reduce((x,y) => { if(x._2 > y._2) x else y})
    grouped
    println(s"il massimo è al minuto:   min = ${grouped._1}, n° = ${grouped._2}")
  }

}
