import converter.Converter
import operations.RDDOperation.SaveToCsv
import properties.{ApplicationConfig, SparkConfig}
import utility.Paths

object FinalMain {

  def main(args: Array[String]): Unit = {

    ApplicationConfig.init(Paths.applicationConfigPath)

    SparkConfig.init(Paths.sparkConfigPath)


    val sparkSession = SparkConfig.instance().setSparkSession()


    /*  val filename = new FileDownloader().downloadWithRedirect("http://data.githubarchive.org/2018-03-01-0.json.gz")
      val fileExtractor = new FileExtractor
      val path = fileExtractor.extract(
        ApplicationConfig.instance().getProperty(PropertyEnum.downloadLocation) + filename,
        ApplicationConfig.instance().getProperty(PropertyEnum.jsonLocation) + Converter.cutExtensionFromFilename(filename) + "-" + System.currentTimeMillis() + ".json")*/

    val path = "/Users/davidebelvedere/Documents/SparkLynx/GroupBigData3/src/archive/JSONFiles/2018-03-01-0-1554043115539.json"

    val rddFromJson = Converter.convertJSONToRDD(path, sparkSession)

    SaveToCsv.csvActorList(rddFromJson)

   // SaveToCsv.csvRepoList(rddFromJson)

    //SaveToCsv.csvAuthorList(rddFromJson)



  }


}
