import converter.Converter
import entity.{Actor, Payload, Repo}
import operations.DataFrameOperation.{CommitDataFrameOperation, EventDataframeOperation}
import operations.RDDOperation.{CommitRddOperation, EventRddOperations}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import properties.{ApplicationConfig, SparkConfig}
import utility.Paths

object main {
  def main(args: Array[String]): Unit = {

    ApplicationConfig.init(Paths.applicationConfigPath)

    SparkConfig.init(Paths.sparkConfigPath)

    val contex = SparkConfig.instance()

    val config = SparkConfig.instance()


    /*  val filename = new FileDownloader().downloadWithRedirect("http://data.githubarchive.org/2018-03-01-0.json.gz")
      val fileExtractor = new FileExtractor
      val path = fileExtractor.extract(
        ApplicationConfig.instance().getProperty(PropertyEnum.downloadLocation) + filename,
        ApplicationConfig.instance().getProperty(PropertyEnum.jsonLocation) + cutExtensionFromFilename(filename) + "-" + System.currentTimeMillis() + ".json")*/

    val path = "/Users/matteobellinaso/Desktop/lynx_accademy/BigData/GroupBigData/downloadJson/2018-03-01-0-1553853054114.json"


    val commitRDD = new CommitRddOperation[(String, String, Actor, Boolean, Repo, String, Payload)]



  }

  def cutExtensionFromFilename(filename: String): String = {
    val splittedFileName = filename.split("\\.")
    splittedFileName(0)
  }

}
