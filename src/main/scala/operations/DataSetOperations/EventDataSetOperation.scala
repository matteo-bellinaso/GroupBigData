package operations.DataSetOperations

import java.text.SimpleDateFormat

import entity.{Actor, Payload, Repo}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.joda.time.DateTime


class EventDataSetOperation[T](sparkSession: SparkSession) {

  import sparkSession.sqlContext.implicits._

 /* def countEventPerActor(dataset: Dataset[T]): Dataset[(Actor, Int)] = {

    val countedEventPerActor = groupPerActor(dataset)
      .map { case (actor: Actor, iterable: Iterable[_]) => (actor, iterable.size) }
    /*val collectedDataset= countedEventPerActor.collect
     collectedDataset.foreach{case (actor: Actor,numero)=>println(s"idAttore: ${actor.id} --> eventi: ${numero} ")}*/

    countedEventPerActor

  }

  def countEventPerType(dataset: Dataset[T]): Dataset[(String, Int)] = {

    val countedEventPerActor = groupByType(dataset)
      .map { case (tipo: String, iterable: Iterable[_]) => (tipo, iterable.size) }

    /* val collectedDataset = countedEventPerActor.collect
     collectedDataset.foreach { case (tipo: String, numero) => println(s"idAttore: ${tipo} --> eventi: ${numero} ") }*/

    countedEventPerActor

  }

  def counEventPerTypeAndActor(dataset: Dataset[T]): //Unit ={
  Dataset[((String, Actor), Int)] = {

    val datasetToReturn = groupPerTypeAndActor(dataset).map { case ((tipo: String, actor: Actor), iterable: Iterable[T]) => ((tipo, actor), iterable.size) }

    /*val collectedDataset= datasetToReturn.collect
    collectedDataset.foreach{case ((tipo,actor: Actor),numero)=>println(s"idAttore: ${actor.id}, tipo: ${tipo} --> eventi: ${numero} ")}*/

    datasetToReturn
  }

  def counEventPerRepo(dataset: Dataset[T]): //Unit ={
  Dataset[(Repo, Int)] = {

    val datasetToReturn = groupByRepo(dataset).map { case (repo: Repo, iterable: Iterable[T]) => (repo, iterable.size) }

    /*val collectedDataset = datasetToReturn.collect
    collectedDataset.foreach { case (repo: Repo, numero) => println(s"repo: Repo: ${repo.id}, --> eventi: ${numero} ") }
  */
    datasetToReturn
  }


  def countEventPerTypeActorAndRepo(dataset: Dataset[T]): //Unit ={
  Dataset[((String, Actor, Repo), Int)] = {

    val datasetToReturn = groupPerTypeActorAndRepo(dataset).map { case ((actor: Actor, repo: Repo, tipo: String), iterable: Iterable[T]) => ((tipo, actor, repo), iterable.size) }

    /*val collectedDataset = datasetToReturn.collect
    collectedDataset.foreach { case ((tipo, actor: Actor, repo: Repo), numero) => println(s"idAttore: ${actor.id}, tipo: ${tipo}, idRepo ${repo.id} --> eventi: ${numero} ") }
  */
    datasetToReturn
  }

  def findActorWithMaxEvents(dataset: Dataset[T]): Dataset[(Actor, Int)] = {
    val gettedDataset = countEventPerActor(dataset)
    val actorWithMaxEvents = gettedDataset.reduce((x, y) => {
      if (x._2 > y._2) {
        x
      } else y
    })

    val finalDataset = gettedDataset.filter("numero> " + actorWithMaxEvents._2) /*{ case (actor: Actor, numero) =>
          numero == actorWithMaxEvents._2
      }*/

    /* val collectedDataset = finalDataset.collect
     collectedDataset.foreach { case (actor: Actor, numero) => println(s"idAttore: ${actor.id}, --> eventi: ${numero} ") }*/


    finalDataset
  }

  def findActorWithMinEvents(dataset: Dataset[T]): Dataset[(Actor, Int)] = {
    val gettedDataset = countEventPerActor(dataset)
    val actorWithMaxEvents = gettedDataset.reduce((x, y) => {
      if (x._2 < y._2) {
        x
      } else y
    })

    val finalDataset = gettedDataset.filter("numero > " + actorWithMaxEvents._2)

    /* val collectedDataset = finalDataset.collect
     collectedDataset.foreach { case (actor: Actor, numero) => println(s"idAttore: ${actor.id}, --> eventi: ${numero} ") }*/


    finalDataset
  }

  def findActorRepoAndHourMinEvents(dataset: Dataset[T]): Dataset[((Actor, Repo, Int), Int)] = {

    val gettedDataset = groupPerActorRepoAndHour(dataset)
    val mappedDataset = gettedDataset.map { case ((actor: Actor, repo: Repo, ora), iterable: Iterable[T]) => ((actor, repo, ora), iterable.size) }
    val actorRepoAndHourWithMaxEvents = mappedDataset.reduce((x, y) => {
      if (x._2 < y._2) {
        x
      } else y
    })

    val finalDataset = mappedDataset.filter { case (_, numero) =>
      numero == actorRepoAndHourWithMaxEvents._2
    }

    /*val collectedDataset = finalDataset.collect
     collectedDataset.foreach { case ((actor: Actor,repo: Repo,ora:Int), numero) => println(s"idAttore: ${actor.id}, idrepo ${repo.id}, ora ${ora}--> eventi: ${numero} ") }
  */

    finalDataset
  }

  def findActorRepoAndHourMaxEvents(dataset: Dataset[T]): Dataset[((Actor, Repo, Int), Int)] = {
    val gettedDataset = groupPerActorRepoAndHour(dataset)
    val mappedDataset = gettedDataset.map { case ((actor: Actor, repo: Repo, ora), iterable: Iterable[T]) => ((actor, repo, ora), iterable.size) }
    val actorRepoAndHourWithMaxEvents = mappedDataset.reduce((x, y) => {
      if (x._2 > y._2) {
        x
      } else y
    })

    val finalDataset = mappedDataset.filter { case (_, numero) =>
      numero == actorRepoAndHourWithMaxEvents._2
    }

    /*val collectedDataset = finalDataset.collect
    collectedDataset.foreach { case ((actor: Actor,repo: Repo,ora:Int), numero) => println(s"idAttore: ${actor.id}, idrepo ${repo.id}, ora ${ora}--> eventi: ${numero} ") }
  */

    finalDataset
  }


  private def groupPerActor(dataset: Dataset[T]): Dataset[(Actor, scala.Iterable[T])] = {
    val grouppedDataset = //dataset.groupBy { case (_, _, actor: Actor, _, _, _, _) => actor }
      dataset.groupBy("actor")
    //dataset.gr
    grouppedDataset.
  }

  private def groupPerTypeAndActor(dataset: Dataset[T]): Dataset[((String, Actor), scala.Iterable[T])] = {
    val grouppedDataset = dataset.groupBy { case (_, tipo: String, actor: Actor, _, _, _, _) => (tipo, actor) }
    grouppedDataset
  }

  private def groupPerTypeActorAndRepo(dataset: Dataset[T]): Dataset[((Actor, Repo, String), scala.Iterable[T])] = {
    val grouppedDataset = dataset.groupBy { case (_, tipo: String, actor: Actor, _, repo: Repo, _, _) => (actor, repo, tipo) }
    grouppedDataset
  }

  private def groupByType(dataset: Dataset[T]): Dataset[(String, scala.Iterable[T])] = {
    val grouppedDataset = dataset.groupBy { case (_, tipo: String, _, _, _, _, _) => tipo }
    grouppedDataset
  }


  private def groupPerActorRepoAndHour(dataset: Dataset[T]) = {
    val mappedDataset = dataset.map { case (id: String, tipo: String, actor: Actor, pubblico: Boolean, repo: Repo, created_at: String, payload: Payload) =>
      val input = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
      val date = new DateTime(input.parse(created_at))
      (id, tipo, actor, pubblico, repo, date.getMinuteOfHour(), payload)
    }
    val grouppedDataset = mappedDataset.groupBy { case (_, _, actor: Actor, _, repo: Repo, hours: Int, _) => (actor, repo, hours) }
    grouppedDataset
  }

  private def groupByRepo(dataset: Dataset[T]): Dataset[(Repo, scala.Iterable[T])] = {
    val grouppedDataset = dataset.groupBy { case (_, _, _, _, repo: Repo, _, _) => repo }
    grouppedDataset
  }*/
}






