import converter.{Converter, SaveCsvDataFrame}
import entity.{Actor, Payload, Repo}
import operations.DataFrameOperation.{CommitDataFrameOperation, EventDataframeOperation}
import operations.RDDOperation.{CommitRddOperation, EventRddOperations}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import properties.{ApplicationConfig, SparkConfig}
import utility.{Paths, PropertyEnum}

object main {
  def main(args: Array[String]): Unit = {

    ApplicationConfig.init(Paths.applicationConfigPath)

    SparkConfig.init(Paths.sparkConfigPath)

    val config = SparkConfig.instance().setSparkSession()


    val dataFrameCsv = new SaveCsvDataFrame(ApplicationConfig.instance().getProperty(PropertyEnum.csvLocation))


    /*  val filename = new FileDownloader().downloadWithRedirect("http://data.githubarchive.org/2018-03-01-0.json.gz")
      val fileExtractor = new FileExtractor
      val path = fileExtractor.extract(
        ApplicationConfig.instance().getProperty(PropertyEnum.downloadLocation) + filename,
        ApplicationConfig.instance().getProperty(PropertyEnum.jsonLocation) + cutExtensionFromFilename(filename) + "-" + System.currentTimeMillis() + ".json")*/

    val path = "/Users/davidebelvedere/Documents/SparkLynx/GroupBigData3/src/archive/JSONFiles/2018-03-01-0-1554043115539.json"


    val commitRDD = new CommitRddOperation[(String, String, Actor, Boolean, Repo, String, Payload)]

    val eventDF = new EventDataframeOperation(config.sparkContext)

    val dataframeFromJson = Converter.convertJSONToDF(path, config)


    val df = eventDF.countEventPerActor(dataframeFromJson)

    // dataFrameCsv.save(df)


  }

  def cutExtensionFromFilename(filename: String): String = {
    val splittedFileName = filename.split("\\.")
    splittedFileName(0)
  }

}
