import entity.Actor
import fileUtilities.{FileDownloader, FileExtractor}
import org.apache.commons.configuration.ConfigurationFactory
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import proprierties.Converter
import persistence.ConnectionProvider
import properties.{ApplicationConfig, SparkConfig}
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

    val strunzDF = Converter.ConvertJSONToDS(path, contex)

    val strunzRDD = Converter.ConvertJSONToRDD(path, contex)

    val actorDF = strunzDF.select("actor.*").dropDuplicates("id")

    actorDF.write.csv(PropertyEnum.csvLocation + "actor")
  }

  def cutExtensionFromFilename(filename: String): String = {
    val splittedFileName = filename.split("\\.")
    splittedFileName(0)
  }


}
