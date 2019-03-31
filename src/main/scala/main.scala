import converter.Converter
import entity.{Actor, Payload, Repo}
import fileUtilities.{FileDownloader, FileExtractor}
import operations.DataFrameOperation.{CommitDataFrameOperation, EventDataframeOperation}
import operations.DataSetOperations.EventDataSetOperation
import operations.RDDOperation.{CommitRddOperation, EventRddOperations}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import properties.{ApplicationConfig, SparkConfig}
import utility.{Paths, PropertyEnum}

object main {
  def main(args: Array[String]): Unit = {

    ApplicationConfig.init(Paths.applicationConfigPath)

    SparkConfig.init(Paths.sparkConfigPath)


    val sparkSession = SparkConfig.instance().setSparkSession()


    /* val filename = new FileDownloader().downloadWithRedirect("http://data.githubarchive.org/2018-03-01-0.json.gz")
     val fileExtractor = new FileExtractor
     val path = fileExtractor.extract(
       ApplicationConfig.instance().getProperty(PropertyEnum.downloadLocation) + filename,
       ApplicationConfig.instance().getProperty(PropertyEnum.jsonLocation) + cutExtensionFromFilename(filename) + "-" + System.currentTimeMillis() + ".json")

     println(path)*/
    val path = "/Users/davidebelvedere/Documents/SparkLynx/GroupBigData3/src/archive/JSONFiles/2018-03-01-0-1554043115539.json"

    val dataFrameFromJson = Converter.convertJSONToDSToUse(path, sparkSession)
    //val eventOperation = new EventRddOperations[(String, String, Actor, Boolean, Repo, String, Payload)]()
    // val commitOp = new CommitRddOperation[(String, String, Actor, Boolean, Repo, String, Payload)]
    val eventOp = new EventDataSetOperation[(String, String, Actor, Boolean, Repo, String, Payload)](sparkSession)
    //val commitOp = new CommitDataFrameOperation(sparkContext)
    // println(s"ciao${commitOp.getCommitCountFromRDD(dataFrameFromJson)}")
    eventOp.findActorRepoAndHourMaxEvents(dataFrameFromJson)

  }

  def cutExtensionFromFilename(filename: String): String = {
    val splittedFileName = filename.split("\\.")
    splittedFileName(0)
  }

}
