import org.apache.commons.configuration.ConfigurationFactory

object main {
  def main(args: Array[String]): Unit = {

    val fileDownloader = new FileDownloader
    fileDownloader.download("https://srv-file1.gofile.io/download/e5xxGs/57b9b04d902588405c3d4c6022e151ee/2018-03-01-0.json.gz", "Download.gz")//salva in download.gz il file scaricato

    val fileExtractor = new FileExtractor
    fileExtractor.extract("Download.gz", "Output.json")
    // SetConfig.setSparkConfiguration()
    fileExtractor.extract("Download.gz", "Output.json")//decomprime e salva in output.json il file scaricato prima
  }


}
