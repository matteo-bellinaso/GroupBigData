package operations.RDDOperation

import entity.{Actor, Payload, Repo}
import org.apache.spark.rdd.RDD

class EventOperations[T] {

  def countEventPerActor(rdd: RDD[T]): RDD[(Actor, Int)] = {

    val countedEventPerActor = groupPerActor(rdd)
      .map { case (actor: Actor, iterable: Iterable[_]) => (actor, iterable.size) }
    /*val collectedRdd= countedEventPerActor.collect
     collectedRdd.foreach{case (actor: Actor,numero)=>println(s"idAttore: ${actor.id} --> eventi: ${numero} ")}*/

    countedEventPerActor

  }

  def countEventPerType(rdd: RDD[T]): RDD[(String, Int)] = {

    val countedEventPerActor = groupByType(rdd)
      .map { case (tipo: String, iterable: Iterable[_]) => (tipo, iterable.size) }
    val collectedRdd = countedEventPerActor.collect
    collectedRdd.foreach { case (tipo: String, numero) => println(s"idAttore: ${tipo} --> eventi: ${numero} ") }

    countedEventPerActor

  }

  def counEventPerTypeAndActor(rdd: RDD[T]): //Unit ={
  RDD[((String, Actor), Int)] = {

    val rddToReturn = groupPerTypeAndActor(rdd).map { case ((tipo: String, actor: Actor), iterable: Iterable[T]) => ((tipo, actor), iterable.size) }

    /*val collectedRdd= rddToReturn.collect
    collectedRdd.foreach{case ((tipo,actor: Actor),numero)=>println(s"idAttore: ${actor.id}, tipo: ${tipo} --> eventi: ${numero} ")}*/

    rddToReturn
  }

  def counEventPerRepo(rdd: RDD[T]): //Unit ={
  RDD[(Repo, Int)] = {

    val rddToReturn = groupByRepo(rdd).map { case (repo: Repo, iterable: Iterable[T]) => (repo, iterable.size) }

    /*val collectedRdd = rddToReturn.collect
    collectedRdd.foreach { case (repo: Repo, numero) => println(s"repo: Repo: ${repo.id}, --> eventi: ${numero} ") }
*/
    rddToReturn
  }


  def countEventPerTypeActorAndRepo(rdd: RDD[T]): //Unit ={
  RDD[((String, Actor, Repo), Int)] = {

    val rddToReturn = groupPerTypeActorAndRepo(rdd).map { case ((actor: Actor, repo: Repo, tipo: String), iterable: Iterable[T]) => ((tipo, actor, repo), iterable.size) }

    val collectedRdd = rddToReturn.collect
    collectedRdd.foreach { case ((tipo, actor: Actor, repo: Repo), numero) => println(s"idAttore: ${actor.id}, tipo: ${tipo}, idRepo ${repo.id} --> eventi: ${numero} ") }

    rddToReturn
  }

  def findActorWithMaxEvents(rdd: RDD[T]): RDD[(Actor, Int)] = {
    val gettedRdd = countEventPerActor(rdd)
    val actorWithMaxEvents = gettedRdd.reduce((x, y) => {
      if (x._2 > y._2) {
        x
      } else y
    })

    val finalRdd = gettedRdd.filter { case (actor: Actor, numero) =>
      numero == actorWithMaxEvents._2
    }

    /* val collectedRdd = finalRdd.collect
     collectedRdd.foreach { case (actor: Actor, numero) => println(s"idAttore: ${actor.id}, --> eventi: ${numero} ") }*/


    finalRdd
  }


  private def groupPerActor(rdd: RDD[T]): RDD[(Actor, scala.Iterable[T])] = {
    val grouppedRdd = rdd.groupBy { case (_, _, actor: Actor, _, _, _, _) => actor }
    grouppedRdd
  }

  private def groupPerTypeAndActor(rdd: RDD[T]): RDD[((String, Actor), scala.Iterable[T])] = {
    val grouppedRdd = rdd.groupBy { case (_, tipo: String, actor: Actor, _, _, _, _) => (tipo, actor) }
    grouppedRdd
  }

  private def groupPerTypeActorAndRepo(rdd: RDD[T]): RDD[((Actor, Repo, String), scala.Iterable[T])] = {
    val grouppedRdd = rdd.groupBy { case (_, tipo: String, actor: Actor, _, repo: Repo, _, _) => (actor, repo, tipo) }
    grouppedRdd
  }

  def groupByType(rdd: RDD[T]): RDD[(String, scala.Iterable[T])] = {
    val grouppedRdd = rdd.groupBy { case (_, tipo: String, _, _, _, _, _) => tipo }
    grouppedRdd
  }

  private def groupByRepo(rdd: RDD[T]): RDD[(Repo, scala.Iterable[T])] = {
    val grouppedRdd = rdd.groupBy { case (_, _, _, _, repo: Repo, _, _) => repo }
    grouppedRdd
  }

  /*
  private def groupPerTypeActorAndRepoAndHour(rdd: RDD[T]): RDD[((Actor, Repo, String, Int), scala.Iterable[T])] = {
    val grouppedRdd = rdd.map { case (_, _, _, _, _, _, created_at: String) =>
      val grouppedRdd = rdd.groupBy { case (_, tipo: String, actor: Actor, _, repo: Repo, _, created_at: String) => (actor, repo, tipo) }
    }*/
  }
