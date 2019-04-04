import converter.Converter
import entity.{Actor, Payload, Repo}
import operations.DataFrameOperation.EventDataframeOperation
import operations.DataSetOperations.EventDataSetOperation
import operations.RDDOperation.EventRddOperations
import operations.SaveToPostgres
import org.apache.spark.sql.SparkSession
import properties.{ApplicationConfig, SparkConfig}
import utility.Paths

object FinalMain {

  def main(args: Array[String]): Unit = {

    // val path2= Paths.applicationConfigPath
    ApplicationConfig.init(Paths.applicationConfigPath)

    SparkConfig.init(Paths.sparkConfigPath)


    implicit val sparkSession: SparkSession = SparkConfig.instance().setSparkSession()


    /*val filename = new FileDownloader().downloadWithRedirect("http://data.githubarchive.org/" + ApplicationConfig.instance().getProperty(PropertyEnum.date) + "-" + ApplicationConfig.instance().getProperty(PropertyEnum.hour) + ".json.gz")
    val fileExtractor = new FileExtractor
    val path = fileExtractor.extract(
      ApplicationConfig.instance().getProperty(PropertyEnum.downloadLocation) + filename,
      ApplicationConfig.instance().getProperty(PropertyEnum.jsonLocation) + Converter.cutExtensionFromFilename(filename) + "-" + System.currentTimeMillis() + ".json")*/

    val path = "/Users/davidebelvedere/Documents/SparkLynx/GroupBigData3/src/archive/JSONFiles/2018-03-01-0-1554223122222.json"

    val rddFromJson = Converter.convertJSONToRDD(path, sparkSession)

    val dataframeFromJson = Converter.convertJSONToDF(path, sparkSession)

    val dsFromJson = Converter.convertJSONToDSToUse(path, sparkSession)

    val eventDsOp = new EventDataSetOperation[(String, String, Actor, Boolean, Repo, String, Payload)]

    val eventDfOp = new EventDataframeOperation

    val eventRDDOp = new EventRddOperations[(String, String, Actor, Boolean, Repo, String, Payload)]

    //eventDfOp.authorList(dataframeFromJson)

    // eventRDDOp.countEventPerTypeActorAndRepo(rddFromJson)

    //SaveToCsv.csvActorList(rddFromJson)

    //SaveToCsv.csvRepoList(rddFromJson)

    // SaveToCsv.csvAuthorList(rddFromJson)

    // SaveToCsv.csvCountEventPerActor(rddFromJson)
    //SaveToCsv.csvCountEventPerType(rddFromJson)

    //SaveToCsv.csvCountEventPerTypeAndActor(rddFromJson)

    //SaveToCsv.csvCountEventPerRepo(rddFromJson)

    //SaveToCsv.csvCountEventPerTypeActorAndRepo(rddFromJson)
    //SaveToCsv.csvFindActorWithMaxEvents(rddFromJson)
    // SaveToCsv.csvFindActorWithMinEvents(rddFromJson)
    //SaveToCsv.csvFindActorRepoAndHourWithMaxEvents(rddFromJson)
    // SaveToCsv.csvFindActorRepoAndHourWithMinEvents(rddFromJson)

    /*Salvataggio su postgres

    SaveToPostgres.saveActorOnPostgres(eventDfOp.actorList(dataframeFromJson))
    SaveToPostgres.saveAuthorOnPostgres(eventDfOp.authorList(dataframeFromJson))
    SaveToPostgres.saveRepoOnPostgres(eventDfOp.repoList(dataframeFromJson))
    SaveToPostgres.saveTypeOnPostgres(eventDfOp.typeList(dataframeFromJson))

    */

  }


}
