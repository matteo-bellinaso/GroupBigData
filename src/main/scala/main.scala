import entity.{Actor, Payload, Repo}
import fileUtilities.{FileDownloader, FileExtractor}
import operations.RDDOperation.{CommitOperations, EventOperations}
import properties.{ApplicationConfig, SparkConfig}
import converter.Converter
import operations.RDDOperation.DataframeOperation.{ActorOperation, CommitOperation}
import org.apache.spark.SparkContext
import utility.{Paths, PropertyEnum}

object main {
  def main(args: Array[String]): Unit = {

    ApplicationConfig.init(Paths.applicationConfigPath)

    SparkConfig.init(Paths.sparkConfigPath)

    val contex = SparkConfig.instance().setSparkConfiguration()

    val config = SparkConfig.instance()
    val sc = new SparkContext(contex)

    /*  val filename = new FileDownloader().downloadWithRedirect("http://data.githubarchive.org/2018-03-01-0.json.gz")
      val fileExtractor = new FileExtractor
      val path = fileExtractor.extract(
        ApplicationConfig.instance().getProperty(PropertyEnum.downloadLocation) + filename,
        ApplicationConfig.instance().getProperty(PropertyEnum.jsonLocation) + cutExtensionFromFilename(filename) + "-" + System.currentTimeMillis() + ".json")*/

    //   val strunzDF = Converter.ConvertJSONToDS(path , contex)

    val path = "/Users/matteobellinaso/Desktop/lynx_accademy/BigData/GroupBigData/downloadJson/2018-03-01-0-1553853054114.json"

    val dataframeFromJson = Converter.ConvertJSONToDF(path, contex)
    val rddFromJson = Converter.ConvertJSONToRDD(path, contex)

    val eventOperation = new CommitOperations[(String, String, Actor, Boolean, Repo, String, Payload)]
    // eventOperation.countEventPerActor(rddFromJson)

    //44527


    val rdd = rddFromJson.map {case (_,_, _, _, _, _, payload: Payload) => (payload.commits)}

    val commitOperation = new CommitOperation(sc)

    val df = commitOperation.getCommitCountFromDF(dataframeFromJson, contex)

    df.show()
    /*
    val dfCommitOperation = new ActorOperation(sc)

    val count = dfCommitOperation.getMaxActorActiveForHour(dataframeFromJson);

    count.show()
*/

  }

  def cutExtensionFromFilename(filename: String): String = {
    val splittedFileName = filename.split("\\.")
    splittedFileName(0)
  }

}
