import entity.{Actor, MainParsed}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import persistence.dao.ActorDaoImpl
import properties.SparkConfig

object ActorThings {

  def getActorDS(dataset: Dataset[MainParsed]): Dataset[Row] = {
    val actorDS = dataset.select("actor.*").dropDuplicates("id")
    actorDS
  }

  def getActorCountDS(dataset: Dataset[MainParsed]): Long = {
    val countDS = dataset.select("actor.*").dropDuplicates("id").count()
    countDS
  }

  def getActorRDD(rdd: RDD[MainParsed]): RDD[(String, Actor)] = {
    val rddActor =  rdd.map( x => (x.id , x.actor))
    val rddGrouped = rddActor.groupByKey()
    val rddFinal = rddGrouped.map(x => (x._1, x._2.last))
    saveOnDBActorRDD(rddFinal)
    rddActor
  }


  private def saveOnDBActorRDD(rdd: RDD[(String, Actor)]) = {
    val actorDao = new ActorDaoImpl()
    rdd.collect.foreach(x => {
      val actorScala = x._2
      val actorJava = new persistence.entity.Actor(actorScala.id.longValue(), actorScala.login,actorScala.display_login, actorScala.gravatar_id, actorScala.url, actorScala.avatar_url);
      actorDao.writeActor(actorJava);
    })
  }



}
