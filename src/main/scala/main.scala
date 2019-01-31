import org.apache.commons.configuration.ConfigurationFactory
import proprierties.SetConfig
import persistence.ConnectionProvider

object main {
  def main(args: Array[String]): Unit = {

    SetConfig.setSparkConfiguration()

    val fileDownloader = new FileDownloader
    fileDownloader.download("https://srv-file1.gofile.io/download/e5xxGs/57b9b04d902588405c3d4c6022e151ee/2018-03-01-0.json.gz", "Download.gz")

    val fileExtractor = new FileExtractor
    fileExtractor.extract("Download.gz", "Output.json")
  }


}
