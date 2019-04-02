package operations.DataSetOperations

import java.text.SimpleDateFormat

import converter.Converter
import entity.{Actor, Payload, Repo}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.joda.time.DateTime


class EventDataSetOperation[T](sparkSession: SparkSession) {

  import sparkSession.sqlContext.implicits._

  def countEventPerActor(dataset: Dataset[T]): Dataset[(Actor, BigInt)] = {

    val grouppedDataframe = dataset.groupBy("_3").count()
    //actor
    val grouppedDataset = grouppedDataframe.as[(Actor, BigInt)]

    /* val collectedDataset= grouppedDataset.collect
      collectedDataset.foreach{case (actor: Actor,numero)=>println(s"idAttore: ${actor.id} --> eventi: ${numero} ")}
 */
    grouppedDataset

  }

  def countEventPerType(dataset: Dataset[T]): Dataset[(String, BigInt)] = {

    val grouppedDataframe = dataset.groupBy("_2").count()
    //type
    val grouppedDataset = grouppedDataframe.as[(String, BigInt)]

    /*  val collectedDataset = grouppedDataset.collect
      collectedDataset.foreach { case (tipo: String, numero) => println(s"idAttore: ${tipo} --> eventi: ${numero} ") }*/

    grouppedDataset

  }

  def counEventPerTypeAndActor(dataset: Dataset[T]): Dataset[((String, Actor), BigInt)] = {

    val grouppedDataframe = dataset.groupBy("_2", "_3").count() //actor,type

    val grouppedDataset = grouppedDataframe.as[(String, Actor, BigInt)]

    /*val collectedDataset= grouppedDataset.collect
    collectedDataset.foreach{case (tipo,actor: Actor,numero)=>println(s"idAttore: ${actor.id}, tipo: ${tipo} --> eventi: ${numero} ")}
*/
    grouppedDataset.map { case (tipo, actor: Actor, numero) => ((tipo, actor), numero) }
  }

  def counEventPerRepo(dataset: Dataset[T]): //Unit ={
  Dataset[(Repo, BigInt)] = {

    val grouppedDataframe = dataset.groupBy("_5").count()
    //repo
    val grouppedDataset = grouppedDataframe.as[(Repo, BigInt)]

    /*val collectedDataset = grouppedDataset.collect
    collectedDataset.foreach { case (repo: Repo, numero) => println(s"repo: Repo: ${repo.id}, --> eventi: ${numero} ") }*/

    grouppedDataset
  }


  def countEventPerTypeActorAndRepo(dataset: Dataset[T]): Dataset[((Actor, Repo, String), BigInt)] = {

    val grouppedDataframe = dataset.groupBy("_3", "_5", "_2").count() //actor,repo,type
    val grouppedDataset = grouppedDataframe.as[(Actor, Repo, String, BigInt)]

    val collectedDataset = grouppedDataset.collect
    collectedDataset.foreach { case (actor: Actor, repo: Repo, tipo, numero) => println(s"idAttore: ${actor.id}, tipo: ${tipo}, idRepo ${repo.id} --> eventi: ${numero} ") }

    grouppedDataset.map { case (actor: Actor, repo: Repo, tipo, numero) => ((actor, repo, tipo), numero) }
  }

  def findActorWithMaxEvents(dataset: Dataset[T]): Dataset[(Actor, BigInt)] = {
    val gettedDataset = countEventPerActor(dataset)
    val actorWithMaxEvents = gettedDataset.reduce((x, y) => {
      if (x._2 > y._2) {
        x
      } else y
    })

    val finalDataset = gettedDataset.filter("count = " + actorWithMaxEvents._2) /*{ case (actor: Actor, numero) =>
          numero == actorWithMaxEvents._2
      }*/

    val collectedDataset = finalDataset.collect
    collectedDataset.foreach { case (actor: Actor, numero) => println(s"idAttore: ${actor.id}, --> eventi: ${numero} ") }


    finalDataset
  }

  def findActorWithMinEvents(dataset: Dataset[T]): Dataset[(Actor, BigInt)] = {
    val gettedDataset = countEventPerActor(dataset)
    val actorWithMaxEvents = gettedDataset.reduce((x, y) => {
      if (x._2 < y._2) {
        x
      } else y
    })

    val finalDataset = gettedDataset.filter("count = " + actorWithMaxEvents._2)

    val collectedDataset = finalDataset.collect
    collectedDataset.foreach { case (actor: Actor, numero) => println(s"idAttore: ${actor.id}, --> eventi: ${numero} ") }


    finalDataset
  }

  def findActorRepoAndHourMinEvents(dataset: Dataset[T]): Dataset[((Actor, Repo, BigInt), BigInt)] = {

    val gettedDataset = groupPerActorRepoAndHour(dataset)

    val actorRepoAndHourWithMaxEvents = gettedDataset.reduce((x, y) => {
      if (x._2 < y._2) {
        x
      } else y
    })

    val finalDataset = gettedDataset.filter("_2 = " + actorRepoAndHourWithMaxEvents._2)

    val collectedDataset = finalDataset.collect
    collectedDataset.foreach { case ((actor: Actor, repo: Repo, ora: BigInt), numero) => println(s"idAttore: ${actor.id}, idrepo ${repo.id}, ora ${ora}--> eventi: ${numero} ") }


    finalDataset
  }

  def findActorRepoAndHourMaxEvents(dataset: Dataset[T]): Dataset[((Actor, Repo, BigInt), BigInt)] = {
    val gettedDataset = groupPerActorRepoAndHour(dataset)
    val actorRepoAndHourWithMaxEvents = gettedDataset.reduce((x, y) => {
      if (x._2 > y._2) {
        x
      } else y
    })

    val finalDataset = gettedDataset.filter("_2 = " + actorRepoAndHourWithMaxEvents._2)

    val collectedDataset = finalDataset.collect
    collectedDataset.foreach { case ((actor: Actor, repo: Repo, ora: BigInt), numero) => println(s"idAttore: ${actor.id}, idrepo ${repo.id}, ora ${ora}--> eventi: ${numero} ") }


    finalDataset
  }


  private def groupPerActorRepoAndHour(dataset: Dataset[T]): Dataset[((Actor, Repo, BigInt), BigInt)] = {
    val mappedDataset = dataset.map { case (id: String, tipo: String, actor: Actor, pubblico: Boolean, repo: Repo, created_at: String, payload: Payload) =>
      val input = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
      val date = new DateTime(input.parse(created_at))
      (id, tipo, actor, pubblico, repo, date.getMinuteOfHour(), payload)
    }
    val grouppedDataframe = mappedDataset.groupBy("_3", "_5", "_6").count() //actor,repo,hours

    grouppedDataframe.as[(Actor, Repo, BigInt, BigInt)].map{case ( actor: Actor, repo: Repo,ora: BigInt, numero:BigInt) => ((actor,repo,ora),numero)}
  }

}






