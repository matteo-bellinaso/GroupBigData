package operations.DataFrameOperation

import converter.Converter
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

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


}
