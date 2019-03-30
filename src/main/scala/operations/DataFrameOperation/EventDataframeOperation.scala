package operations.DataFrameOperation

import converter.Converter
import entity.{Actor, Repo}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

class EventDataframeOperation(sc: SparkContext) {

  val hiveContext = new HiveContext(sc)

  import hiveContext.implicits._

  def countEventPerActor(dataFrame: DataFrame) = {
    val selectedFields = dataFrame.select($"id", $"actor.id".alias("idAttore"))
    val grouppedPerActor = selectedFields.groupBy("idAttore")
    //grouppedPerActor.count().show(100000)
    grouppedPerActor.count()
  }

  def countEventPerType(dataFrame: DataFrame) = {
    val selectedFields = dataFrame.select($"id", $"type")
    val grouppedPerType = selectedFields.groupBy("type")
    // grouppedPerType.count().show(100000)
    grouppedPerType.count().as("conteggio")
  }

  def counEventPerTypeAndActor(dataFrame: DataFrame) = {
    val selectedFields = dataFrame.select($"id", $"type", $"actor.id".alias("idAttore"))
    val grouppedPerTypeAndActor = selectedFields.groupBy("type", "idAttore")
    //grouppedPerTypeAndActor.count().as("conteggio").show(100000)
    grouppedPerTypeAndActor.count()
  }

  def countEventPerTypeActorAndRepo(dataFrame: DataFrame) = {
    val selectedFields = dataFrame.select($"id", $"type", $"actor.id".alias("idAttore"), $"repo.id".alias("idRepo"))
    val grouppedPerTypeActorAndRepo = selectedFields.groupBy("idAttore", "idRepo", "type")
    // grouppedPerTypeActorAndRepo.count().show()
    grouppedPerTypeActorAndRepo.count()
  }

  def findActorWithMaxEvents(dataFrame: DataFrame) = {

    val gettedDF = countEventPerActor(dataFrame)

    val count = gettedDF.agg(max("count").alias("conteggio"))

    val idAttoreDf = count.join(gettedDF, count("conteggio") === gettedDF("count")).select("idAttore", "conteggio")

    // idAttoreDf.show()

    idAttoreDf
  }

  def findActorWithMinEvents(dataFrame: DataFrame) = {

    val gettedDF = countEventPerActor(dataFrame)

    val count = gettedDF.agg(min("count").alias("conteggio"))

    val idAttoreDf = count.join(gettedDF, count("conteggio") === gettedDF("count")).select("idAttore", "conteggio")

    // idAttoreDf.show()

    idAttoreDf
  }

  def findActorRepoAndHourMinEvents(dataFrame: DataFrame) = {
    val dfWithTime = dataFrame.withColumn("time", Converter.convertColumnTime($"created_at"))
    val selectedDf = dfWithTime.select($"actor.id".alias("idAttore"), $"repo.id".alias("idRepo"), $"time")
    val grouppedDf = selectedDf.groupBy("idAttore", "idRepo", "time").count()
    val minimo = grouppedDf.agg(min("count").alias("conteggio"))

    val grouppedDfWithCount = minimo.join(grouppedDf, minimo("conteggio") === grouppedDf("count")).select("idAttore","idRepo","time", "conteggio")
    //grouppedDfWithCount.show(5000)
    grouppedDfWithCount
  }

  def findActorRepoAndHourMaxEvents(dataFrame: DataFrame) = {
    val dfWithTime = dataFrame.withColumn("time", Converter.convertColumnTime($"created_at"))
    val selectedDf = dfWithTime.select($"actor.id".alias("idAttore"), $"repo.id".alias("idRepo"), $"time")
    val grouppedDf = selectedDf.groupBy("idAttore", "idRepo", "time").count()
    val massimo = grouppedDf.agg(max("count").alias("conteggio"))

    val grouppedDfWithCount = massimo.join(grouppedDf, massimo("conteggio") === grouppedDf("count")).select("idAttore","idRepo","time", "conteggio")
    grouppedDfWithCount.show()
    grouppedDfWithCount
  }
}
