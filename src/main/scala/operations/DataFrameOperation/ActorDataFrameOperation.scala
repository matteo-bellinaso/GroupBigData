package operations.DataFrameOperation

import converter.Converter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


class ActorDataFrameOperation(sparkSession: SparkSession) {


  import sparkSession.implicits._

  def getActorActiveForHour(dataframe: DataFrame) = {
    val time = dataframe.withColumn("time", Converter.convertColumnTime($"created_at"))
    time.select($"actor", $"time").groupBy("time").count()
  }

  def getActorActiveForTypeAndHour(dataframe: DataFrame) = {
    val time =  dataframe.withColumn("time", Converter.convertColumnTime($"created_at"))
    time.select($"actor", $"type", $"time").groupBy($"type", $"time").count()
  }


  def getMaxActorActiveForHour(dataframe: DataFrame) = {
    val tot = getActorActiveForHour(dataframe).agg(max($"count")).withColumnRenamed("max(count)", "countMax").as("tot")
    val getted = getActorActiveForHour(dataframe).select("*").as("all")
    val joined = getted.join(tot, col("all.count") === col("tot.countMax"))
    joined.select("time", "countMax")
  }

  def getMinActorActiveForHour(dataframe: DataFrame) = {
    val tot = getActorActiveForHour(dataframe).agg(min($"count")).withColumnRenamed("min(count)", "countMin").as("tot")
    val getted = getActorActiveForHour(dataframe).select("*").as("all")
    val joined = getted.join(tot, col("all.count") === col("tot.countMin"))
    joined.select("time", "countMin")
  }
}
