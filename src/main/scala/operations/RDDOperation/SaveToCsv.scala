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

  def csvCountEventPerTypeAndActor(rdd: RDD[T]) = {
    SaveCSV.saveCountEventPerTypeAndActorCsv(eventRddOperations.countEventPerTypeAndActor(rdd).filter(x => x._1._1 != null && x._1._2 != null))
  }


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
