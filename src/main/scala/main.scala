
import entity.{Actor, MainParsed, Payload, Repo}
import fileUtilities.{FileDownloader, FileExtractor}
import operations.RDDOperation.{ActorOperations, CommitOperations}
import org.apache.spark.rdd.RDD
import properties.{ApplicationConfig, SparkConfig}
import proprierties.{Converter, SaveCSV}
import utility.{Paths, PropertyEnum}

object main {
  def main(args: Array[String]): Unit = {
    ApplicationConfig.init(Paths.applicationConfigPath)

    SparkConfig.init(Paths.sparkConfigPath)

    val contex = SparkConfig.instance().setSparkConfiguration()


    val config = SparkConfig.instance()

    val filename = new FileDownloader().downloadWithRedirect("http://data.githubarchive.org/2018-03-01-0.json.gz")


    val fileExtractor = new FileExtractor
    val path = fileExtractor.extract(
      ApplicationConfig.instance().getProperty(PropertyEnum.downloadLocation) + filename,
      ApplicationConfig.instance().getProperty(PropertyEnum.jsonLocation) + cutExtensionFromFilename(filename) + "-" + System.currentTimeMillis() + ".json")


    val strunzDS = Converter.ConvertJSONToDS(path, contex)

    val strunzRDD = Converter.ConvertJSONToRDD(path, contex)

    /*
    val actorDF = ActorThings.getActorDS(strunzDF)

    val actorRDD = ActorThings.getActorRDD(strunzRDD)

    SaveCSV.saveActorCsv(actorRDD)

    val commitService = new CommitOperations[(String, String, Actor, Boolean, Repo, String, Payload)];

    val actorService = new ActorOperations[(String, String, Actor, Boolean, Repo, String, Payload)]
      */
  }

  def cutExtensionFromFilename(filename: String): String = {
    val splittedFileName = filename.split("\\.")
    splittedFileName(0)
  }

}
