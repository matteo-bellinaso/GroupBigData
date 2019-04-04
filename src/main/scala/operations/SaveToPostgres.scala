package operations

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode}
import properties.ApplicationConfig
import utility.PropertyEnum

object SaveToPostgres {
  val prop = new Properties()

  prop.setProperty("user", ApplicationConfig.instance().getProperty(PropertyEnum.user))
  prop.setProperty("password", ApplicationConfig.instance().getProperty(PropertyEnum.password))
  prop.put("driver", ApplicationConfig.instance().getProperty(PropertyEnum.driver))

  def saveActorOnPostgres(df: DataFrame) = {
    df.select("actor.*").write.mode(SaveMode.Overwrite)
      .jdbc(ApplicationConfig.instance().getProperty(PropertyEnum.dbUrl), "actor", prop)
  }

  def saveRepoOnPostgres(df: DataFrame) = {
    df.select("repo.*").write.mode(SaveMode.Overwrite)
      .jdbc(ApplicationConfig.instance().getProperty(PropertyEnum.dbUrl), "repo", prop)
  }

  def saveAuthorOnPostgres(df: DataFrame) = {
    df.select("author.*").write.mode(SaveMode.Overwrite)
      .jdbc(ApplicationConfig.instance().getProperty(PropertyEnum.dbUrl), "author", prop)
  }

  def saveTypeOnPostgres(df: DataFrame) = {
    df.show()
    df.write.mode(SaveMode.Overwrite)
      .jdbc(ApplicationConfig.instance().getProperty(PropertyEnum.dbUrl), "type", prop)
  }






}
