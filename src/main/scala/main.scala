
import fileUtilities.{FileDownloader, FileExtractor}
import properties.{ApplicationConfig, SparkConfig}
import utility.{Paths, PropertyEnum}

object main {
  def main(args: Array[String]): Unit = {
    ApplicationConfig.init(Paths.applicationConfigPath)

    SparkConfig.init(Paths.sparkConfigPath)
    SparkConfig.instance().setSparkConfiguration()


    //"https://srv-file1.gofile.io/download/e5xxGs/57b9b04d902588405c3d4c6022e151ee/2018-03-01-0.json.gz"
    val filename = new FileDownloader().downloadWithRedirect("http://data.githubarchive.org/2018-03-01-0.json.gz")


    val fileExtractor = new FileExtractor
    fileExtractor.extract(
      ApplicationConfig.instance().getProperty(PropertyEnum.downloadLocation) + filename,
      ApplicationConfig.instance().getProperty(PropertyEnum.jsonLocation) + cutExtensionFromFilename(filename) + "-" + System.currentTimeMillis() + ".json")
  }

  def cutExtensionFromFilename(filename: String): String = {
    val splittedFileName = filename.split("\\.")
    splittedFileName(0)
  }

}
