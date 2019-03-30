import converter.Converter
import entity.{Actor, Payload, Repo}
import operations.DataFrameOperation.EventDataframeOperation
import operations.RDDOperation.EventRddOperations
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import properties.{ApplicationConfig, SparkConfig}
import utility.Paths

object main {
  def main(args: Array[String]): Unit = {

    ApplicationConfig.init(Paths.applicationConfigPath)

    SparkConfig.init(Paths.sparkConfigPath)

    val contex = SparkConfig.instance().setSparkConfiguration()

    val config = SparkConfig.instance()

    val sparkContext = new SparkContext(contex)

    /*  val filename = new FileDownloader().downloadWithRedirect("http://data.githubarchive.org/2018-03-01-0.json.gz")
      val fileExtractor = new FileExtractor
      val path = fileExtractor.extract(
        ApplicationConfig.instance().getProperty(PropertyEnum.downloadLocation) + filename,
        ApplicationConfig.instance().getProperty(PropertyEnum.jsonLocation) + cutExtensionFromFilename(filename) + "-" + System.currentTimeMillis() + ".json")*/

    val path = "/Users/davidebelvedere/Documents/SparkLynx/GroupBigData3/src/archive/JSONFiles/2018-03-01-0-1553603303171.json"

    val dataFrameFromJson = Converter.ConvertJSONToRDD(path, contex)
    val eventOperation = new EventRddOperations[(String, String, Actor, Boolean, Repo, String, Payload)]()

    eventOperation.findActorRepoAndHourMaxEvents(dataFrameFromJson)

  }

  def cutExtensionFromFilename(filename: String): String = {
    val splittedFileName = filename.split("\\.")
    splittedFileName(0)
  }

}
