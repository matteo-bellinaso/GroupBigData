import entity.{Actor, Payload, Repo}
import fileUtilities.{FileDownloader, FileExtractor}
import operations.RDDOperation.EventOperations
import properties.{ApplicationConfig, SparkConfig}
import proprierties.Converter
import utility.{Paths, PropertyEnum}

object main {
  def main(args: Array[String]): Unit = {

    ApplicationConfig.init(Paths.applicationConfigPath)

    SparkConfig.init(Paths.sparkConfigPath)

    val contex = SparkConfig.instance().setSparkConfiguration()

    val config  = SparkConfig.instance()

  /*  val filename = new FileDownloader().downloadWithRedirect("http://data.githubarchive.org/2018-03-01-0.json.gz")
    val fileExtractor = new FileExtractor
    val path = fileExtractor.extract(
      ApplicationConfig.instance().getProperty(PropertyEnum.downloadLocation) + filename,
      ApplicationConfig.instance().getProperty(PropertyEnum.jsonLocation) + cutExtensionFromFilename(filename) + "-" + System.currentTimeMillis() + ".json")*/

 //   val strunzDF = Converter.ConvertJSONToDS(path , contex)

    val path ="/Users/davidebelvedere/Documents/SparkLynx/GroupBigData2/src/archive/JSONFiles/2018-03-01-0-1553603303171.json"

    val rddFromJson = Converter.ConvertJSONToRDD(path, contex)
    val  eventOperation= new EventOperations[(String, String, Actor, Boolean, Repo, String, Payload)]()
   // eventOperation.countEventPerActor(rddFromJson)
    eventOperation.countEventPerType(rddFromJson)

  }

  def cutExtensionFromFilename(filename: String): String = {
    val splittedFileName = filename.split("\\.")
    splittedFileName(0)
  }

}
