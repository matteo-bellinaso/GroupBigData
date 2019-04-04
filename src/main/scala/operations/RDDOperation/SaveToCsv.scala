package operations.RDDOperation

import entity._
import operations.DataSetOperations.EventDataSetOperation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import proprierties.SaveCSV

object SaveToCsv {

  type T = (String, String, Actor, Boolean, Repo, String, Payload)

  private val actorRddOperation = new ActorRddOperation[T]

  private val eventRddOperations = new EventRddOperations[T]

  //private val eventDsOperations= new EventDataSetOperation[T]()

  private val commitRDD = new CommitRddOperation[T]

  def csvActorList(rdd: RDD[T]) = {
    SaveCSV.saveActorCsv(eventRddOperations.actorList(rdd))
  }

  def csvRepoList(rdd: RDD[T]) = {
    SaveCSV.saveRepoCsv(eventRddOperations.repoList(rdd).filter(x => x._1 != null))
  }

  /* def typeList(rdd: RDD[T]) = {
     val typeRdd = rdd.map { case (_, tipo: String, _, _, _, _, _) =>
       tipo
     }.distinct()
     typeRdd
   }*/

  def csvAuthorList(rdd: RDD[T]) = {
    SaveCSV.saveAuthorCsv(eventRddOperations.authorList(rdd).filter(x => x._1 != null))
  }

  def csvCountEventPerActor(rdd: RDD[T]) = {
    SaveCSV.saveCountEventPerActorCsv(eventRddOperations.countEventPerActor(rdd).filter(x => x._1 != null))
  }

  def csvCountEventPerType(rdd: RDD[T]) = {
    SaveCSV.saveCountEventPerTypeCsv(eventRddOperations.countEventPerType(rdd).filter(x => x._1 != null))
  }

  def csvCommitPerActor(rdd: RDD[T]) = {
    SaveCSV.saveCommitForActor(commitRDD.getCommitForActor(rdd).filter(x => x._1 != null))
  }

  def csvCommitPerActorType(rdd: RDD[T]) = {
    SaveCSV.saveCommitForActorAndType(commitRDD.getCommitForActorAndType(rdd).filter(x => x._1 != null))
  }

  def csvCommitPerHour(rdd: RDD[T]) = {
    SaveCSV.saveCommitForHour(commitRDD.getMaxCommitPerHour(rdd).filter(x => x._1 != null))
  }

  /*
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

      /*val collectedRdd = rddToReturn.collect
      collectedRdd.foreach { case ((tipo, actor: Actor, repo: Repo), numero) => println(s"idAttore: ${actor.id}, tipo: ${tipo}, idRepo ${repo.id} --> eventi: ${numero} ") }
  */
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


  def csvCountEventPerTypeAndActor(rdd: RDD[T]) = {
    SaveCSV.saveCountEventPerTypeAndActorCsv(eventRddOperations.countEventPerTypeAndActor(rdd).filter(x => x._1._1 != null && x._1._2 != null))
    }

    def findActorWithMinEvents(rdd: RDD[T]): RDD[(Actor, Int)] = {
      val gettedRdd = countEventPerActor(rdd)
      val actorWithMaxEvents = gettedRdd.reduce((x, y) => {
        if (x._2 < y._2) {
          x
        } else y
      })

  def csvCountEventPerRepo(rdd: RDD[T]) = {
    SaveCSV.saveCountEventPerRepoCsv(eventRddOperations.countEventPerRepo(rdd).filter(x => x._1 != null))
  }

  def csvCountEventPerTypeActorAndRepo(rdd: RDD[T]) = {
    SaveCSV.saveCountEventTypeActorAndRepoCsv(eventRddOperations.countEventPerTypeActorAndRepo(rdd).filter(x => x._1._1 != null && x._1._2 != null && x._1._3 != null))
    }

    def csvFindActorWithMaxEvents(rdd: RDD[T]) = {
      SaveCSV.saveActorWithMaxEventCsv(eventRddOperations.findActorWithMaxEvents(rdd).filter(x => x._1 != null))
  }

  def csvFindActorWithMinEvents(rdd: RDD[T]) = {
    SaveCSV.saveActorWithMaxEventCsv(eventRddOperations.findActorWithMinEvents(rdd).filter(x => x._1 != null))
    }

    def csvFindActorRepoAndHourWithMaxEvents(rdd: RDD[T]) = {
      SaveCSV.saveActorRepoAndHourWithMaxEventsCsv(eventRddOperations.findActorRepoAndHourMaxEvents(rdd).filter(x => x._1._1 != null && x._1._2 != null ))
  }

  def csvFindActorRepoAndHourWithMinEvents(rdd: RDD[T]) = {
    SaveCSV.saveActorRepoAndHourWithMinEventsCsv(eventRddOperations.findActorRepoAndHourMinEvents(rdd).filter(x => x._1._1 != null && x._1._2 != null ))
    }

 /* def csvCountEventPerTypeActorAndRepoDs(ds: Dataset[T]) = {
    SaveCSV.saveCountEventTypeActorAndRepoCsv(eventRddOperations.countEventPerTypeActorAndRepo(ds).filter(x => x._1._1 != null && x._1._2 != null && x._1._3 != null))
  }*/

}
