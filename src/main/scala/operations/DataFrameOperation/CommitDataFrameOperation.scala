package operations.DataFrameOperation

import converter.Converter
import entity.{Author, Commit}
import io.netty.util.Version
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{max, _}

import scala.collection.mutable

class CommitDataFrameOperation(sc: SparkContext) {

  val hc = new HiveContext(sc)

  import hc.implicits._


  def getCommitCountFromDF(dataFrame: DataFrame) = {
    val dataFrameCommit = dataFrame.select("payload.commits")
    val filteredNullDF = dataFrameCommit.filter("commits is not null")
    val sizeCommit = filteredNullDF.withColumn("size", size($"commits")).agg(sum("size").alias("somma"))
    //sizeCommit.show()
    sizeCommit
  }

  def getCommitCountPerActor(dataframe: DataFrame) = {
    val df = dataframe.select($"actor", $"payload.commits")
    val filtered = df.filter("commits is not null")
    val nCommit = filtered.withColumn("size", size($"commits"))
    val finalDF = nCommit.groupBy($"actor").agg(sum("size").as("tot"))
    finalDF
  }

  def getCommitCountPerTypeAndActor(dataframe: DataFrame) = {
    val df = dataframe.select($"actor", $"type", $"payload.commits")
    val filtered = df.filter("commits is not null")
    val nCommit = filtered.withColumn("size", size($"commits"))
    val finalDF = nCommit.groupBy($"actor", $"type").agg(sum("size").as("tot"))
    finalDF
  }

  def getMaxCommitForHour(dataFrame: DataFrame) = {
    val df = dataFrame.select($"created_at", $"payload.commits")
    val filtered = df.filter("commits is not null")
    val time = filtered.withColumn("time", Converter.convertColumnTime($"created_at")).withColumn("size", size($"commits"))
    val finalDF = time.groupBy("time").agg(sum("size").as("tot"))

    val joined = finalDF.join(finalDF, finalDF("tot") === max("tot")).select("*")
    joined

  }

  def getMaxCommitForRepo(dataframe: DataFrame) = {
    val df = dataframe.select($"repo", $"payload.commits")
    val filtered = df.filter("commits is not null")
    val nCommit = filtered.withColumn("size", size($"commits"))
    val grouped = nCommit.groupBy($"repo").agg(sum("size") as ("tot"))
    val maxi = grouped.agg(max($"tot").alias("max"))

    val joined = grouped.join(maxi, grouped("tot") === max("max")).select("*")
    joined
  }




}
