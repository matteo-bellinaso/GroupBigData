package operations.RDDOperation

import java.text.SimpleDateFormat

import entity._
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

class EventRddOperations[T] { //(mainParsed.id, mainParsed.`type`, mainParsed.actor, mainParsed.publico, mainParsed.repo, mainParsed.created_at, mainParsed.payload)

  def actorList(rdd: RDD[T]): RDD[(String, Actor)] = {
    val actorRdd = rdd.map { case (_, _, actor: Actor, _, _, _, _) =>
      ("" + actor.id, actor)
    }.distinct()
    actorRdd
  }

  def repoList(rdd: RDD[T]): RDD[(String, Repo)] = {
    val repoRdd = rdd.map { case (_, _, _, _, repo: Repo, _, _) =>
      ("" + repo.id, repo)
    }.distinct()
    repoRdd
  }

  def typeList(rdd: RDD[T]): RDD[String] = {
    val typeRdd = rdd.map { case (_, tipo: String, _, _, _, _, _) =>
      tipo
    }.distinct()
    typeRdd
  }

  def authorList(rdd: RDD[T]): RDD[(String, Author)] = {
    val commitsRdd = rdd.filter {
      case (_, _, _, _, _, _, payload: Payload) => payload != null && payload.commits != null && payload.commits.nonEmpty
    }.flatMap { case (_, _, _, _, _, _, payload: Payload) =>
      payload.commits
    }


    val authorRdd = commitsRdd.map { case (commit: Commit) => (commit.author.name, commit.author)
    }.distinct()

    authorRdd
  }

  def countActor(rdd: RDD[T]): BigInt = {
    actorList(rdd).count()
  }

  def countRepo(rdd: RDD[T]): BigInt = {
    repoList(rdd).count()
  }

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

    /* val collectedRdd = countedEventPerActor.collect
     collectedRdd.foreach { case (tipo: String, numero) => println(s"idAttore: ${tipo} --> eventi: ${numero} ") }*/

    countedEventPerActor

  }

  def countEventPerTypeAndActor(rdd: RDD[T]): //Unit ={
  RDD[((String, Actor), Int)] = {

    val rddToReturn = groupPerTypeAndActor(rdd).map { case ((tipo: String, actor: Actor), iterable: Iterable[T]) => ((tipo, actor), iterable.size) }

    /*val collectedRdd= rddToReturn.collect
    collectedRdd.foreach{case ((tipo,actor: Actor),numero)=>println(s"idAttore: ${actor.id}, tipo: ${tipo} --> eventi: ${numero} ")}*/

    rddToReturn
  }

  def countEventPerRepo(rdd: RDD[T]): //Unit ={
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

  def findActorWithMinEvents(rdd: RDD[T]): RDD[(Actor, Int)] = {
    val gettedRdd = countEventPerActor(rdd)
    val actorWithMaxEvents = gettedRdd.reduce((x, y) => {
      if (x._2 < y._2) {
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

  def findActorRepoAndHourMinEvents(rdd: RDD[T]): RDD[((Actor, Repo, Int), Int)] = {
    val gettedRdd = groupPerActorRepoAndHour(rdd)
    val mappedRdd = gettedRdd.map { case ((actor: Actor, repo: Repo, ora), iterable: Iterable[T]) => ((actor, repo, ora), iterable.size) }
    val actorRepoAndHourWithMaxEvents = mappedRdd.reduce((x, y) => {
      if (x._2 < y._2) {
        x
      } else y
    })

    val finalRdd = mappedRdd.filter { case (_, numero) =>
      numero == actorRepoAndHourWithMaxEvents._2
    }

    /*val collectedRdd = finalRdd.collect
     collectedRdd.foreach { case ((actor: Actor,repo: Repo,ora:Int), numero) => println(s"idAttore: ${actor.id}, idrepo ${repo.id}, ora ${ora}--> eventi: ${numero} ") }
*/

    finalRdd
  }

  def findActorRepoAndHourMaxEvents(rdd: RDD[T]): RDD[((Actor, Repo, Int), Int)] = {
    val gettedRdd = groupPerActorRepoAndHour(rdd)
    val mappedRdd = gettedRdd.map { case ((actor: Actor, repo: Repo, ora), iterable: Iterable[T]) => ((actor, repo, ora), iterable.size) }
    val actorRepoAndHourWithMaxEvents = mappedRdd.reduce((x, y) => {
      if (x._2 > y._2) {
        x
      } else y
    })

    val finalRdd = mappedRdd.filter { case (_, numero) =>
      numero == actorRepoAndHourWithMaxEvents._2
    }

    /*val collectedRdd = finalRdd.collect
    collectedRdd.foreach { case ((actor: Actor,repo: Repo,ora:Int), numero) => println(s"idAttore: ${actor.id}, idrepo ${repo.id}, ora ${ora}--> eventi: ${numero} ") }
*/

    finalRdd
  }


  private def groupPerActor(rdd: RDD[T]): RDD[(Actor, scala.Iterable[T])] = {
    val grouppedRdd = rdd.groupBy { case (_, _, actor: Actor, _, _, _, _) => actor }
    grouppedRdd
  }

  private def groupPerTypeAndActor(rdd: RDD[T]): RDD[((String, Actor), scala.Iterable[T])]

  = {
    val grouppedRdd = rdd.groupBy { case (_, tipo: String, actor: Actor, _, _, _, _) => (tipo, actor) }
    grouppedRdd
  }

  private def groupPerTypeActorAndRepo(rdd: RDD[T]): RDD[((Actor, Repo, String), scala.Iterable[T])]

  = {
    val grouppedRdd = rdd.groupBy { case (_, tipo: String, actor: Actor, _, repo: Repo, _, _) => (actor, repo, tipo) }
    grouppedRdd
  }

  private def groupByType(rdd: RDD[T]): RDD[(String, scala.Iterable[T])]

  = {
    val grouppedRdd = rdd.groupBy { case (_, tipo: String, _, _, _, _, _) => tipo }
    grouppedRdd
  }


  private def groupPerActorRepoAndHour(rdd: RDD[T])

  = {
    val mappedRdd = rdd.map { case (id: String, tipo: String, actor: Actor, pubblico: Boolean, repo: Repo, created_at: String, payload: Payload) =>
      val input = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
      val date = new DateTime(input.parse(created_at))
      (id, tipo, actor, pubblico, repo, date.getMinuteOfHour(), payload)
    }
    val grouppedRdd = mappedRdd.groupBy { case (_, _, actor: Actor, _, repo: Repo, hours: Int, _) => (actor, repo, hours) }
    grouppedRdd
  }

  private def groupByRepo(rdd: RDD[T]): RDD[(Repo, scala.Iterable[T])]

  = {
    val grouppedRdd = rdd.groupBy { case (_, _, _, _, repo: Repo, _, _) => repo }
    grouppedRdd
  }


}
