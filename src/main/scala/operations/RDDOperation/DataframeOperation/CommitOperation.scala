package operations.RDDOperation.DataframeOperation


import converter.Converter
import entity.{Author, Commit}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Encoders, types}

import scala.collection.mutable.ListBuffer

class CommitOperation(sc: SparkContext) {

  val hc = new HiveContext(sc)

  import hc.implicits._

  def getCommitCountFromDF(dataframe: DataFrame, con: SparkConf) = {
    //Struttura author
    val authorStruct = new StructType().add("name", StringType, true).add("email", StringType, true)

    // Struttura Commit
    val struct = StructType(Seq(StructField("sha", StringType, true), StructField("author", authorStruct, true),
      StructField("message", StringType, true), StructField("distinct", BooleanType, true), StructField("url", StringType, true)  ) )


    val payload = dataframe.select($"payload.commits")
    val rdd = payload.rdd
    val dfFinal = Converter.createDF(con, rdd, struct)
    dfFinal
  }



}
