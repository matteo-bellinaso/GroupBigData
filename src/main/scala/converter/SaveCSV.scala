package proprierties

import java.io.{BufferedWriter, FileWriter}
import java.util

import au.com.bytecode.opencsv.CSVWriter
import entity.{Actor, Author, Repo}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import properties.ApplicationConfig
import utility.PropertyEnum


object SaveCSV {


  def saveActorCsv(rdd: RDD[(String, Actor)]) = {

    println(s"dio porco:  ${ApplicationConfig.instance().getProperty(PropertyEnum.csvLocation)}")
    val out = new BufferedWriter(new FileWriter(ApplicationConfig.instance().getProperty(PropertyEnum.csvLocation) + "actor.csv"))
    val writer = new CSVWriter(out)
    val actorSchema = Array("id", "login", "display_login", "gravatar_id", "url", "avatar_url")

    val finalArr: java.util.List[Array[String]] = new util.ArrayList()
    finalArr.add(actorSchema)

    rdd.collect.foreach { case (_, row) => {
      val arr = Array(row.id.toString(), row.login, row.display_login, row.gravatar_id, row.url, row.avatar_url)
      finalArr.add(arr)
    }
    }
    writer.writeAll(finalArr)
    out.close()
  }

  def saveAuthorCsv(rdd: RDD[(String, Author)]) = {
    rdd.collect.foreach(z => println(s"name ${println(z._1)}"))
    val out = new BufferedWriter(new FileWriter(ApplicationConfig.instance().getProperty(PropertyEnum.csvLocation) + "author.csv"))
    val writer = new CSVWriter(out)
    val authorSchema = Array("name", "email")

    val finalArr: java.util.List[Array[String]] = new util.ArrayList()
    finalArr.add(authorSchema)

    rdd.collect.foreach { case (_, row) => {
      val arr = Array(row.name, row.email)
      finalArr.add(arr)
    }
    }
    writer.writeAll(finalArr)
    out.close()
  }

  def saveRepoCsv(rdd: RDD[(String, Repo)]) = {

    val out = new BufferedWriter(new FileWriter(ApplicationConfig.instance().getProperty(PropertyEnum.csvLocation) + "repo.csv"))
    val writer = new CSVWriter(out)
    val repoSchema = Array("id", "name", "url")

    val finalArr: java.util.List[Array[String]] = new util.ArrayList()
    finalArr.add(repoSchema)

    rdd.collect.foreach { case (_, row) => {
      val arr = Array(row.id.toString(), row.name, row.url)
      finalArr.add(arr)
    }
    }
    writer.writeAll(finalArr)
    out.close()
  }

  def saveCountEventPerActorCsv(rdd: RDD[(Actor, Int)]): Unit = {
    val out = new BufferedWriter(new FileWriter(ApplicationConfig.instance().getProperty(PropertyEnum.csvLocation) + "actorCount.csv"))
    saveActorCount(out,rdd)
  }

  def saveCountEventPerTypeCsv(rdd: RDD[(String, Int)]): Unit = {
    val out = new BufferedWriter(new FileWriter(ApplicationConfig.instance().getProperty(PropertyEnum.csvLocation) + "typeCount.csv"))
    val writer = new CSVWriter(out)
    val actorSchema = Array("type", "count")

    val finalArr: java.util.List[Array[String]] = new util.ArrayList()
    finalArr.add(actorSchema)

    rdd.collect.foreach { case (tipo, count) => {
      val arr = Array(tipo, count.toString)
      finalArr.add(arr)
    }
    }
    writer.writeAll(finalArr)
    out.close()
  }

  def saveCountEventPerTypeAndActorCsv(rdd: RDD[((String, Actor), Int)]): Unit = {
    val out = new BufferedWriter(new FileWriter(ApplicationConfig.instance().getProperty(PropertyEnum.csvLocation) + "typeAndActorCount.csv"))
    val writer = new CSVWriter(out)
    val actorSchema = Array("(type", "(id", "login", "display_login", "gravatar_id", "url", "avatar_url))", "count"
    )

    val finalArr: java.util.List[Array[String]] = new util.ArrayList()
    finalArr.add(actorSchema)

    rdd.collect.foreach { case ((tipo, actor: Actor), count) => {
      val arr = Array("(" + tipo, "(" + actor.id.toString(), actor.login, actor.display_login, actor.gravatar_id, actor.url, actor.avatar_url + "))", count.toString)
      finalArr.add(arr)
    }
    }
    writer.writeAll(finalArr)
    out.close()
  }

  def saveCountEventPerRepoCsv(rdd: RDD[(Repo, Int)]): Unit = {
    val out = new BufferedWriter(new FileWriter(ApplicationConfig.instance().getProperty(PropertyEnum.csvLocation) + "repoCount.csv"))
    val writer = new CSVWriter(out)
    val actorSchema = Array("(id", "name", "url)", "count")

    val finalArr: java.util.List[Array[String]] = new util.ArrayList()
    finalArr.add(actorSchema)

    rdd.collect.foreach { case (repo: Repo, count) => {
      val arr = Array("(" + repo.id.toString(), repo.name, repo.url + ")", count.toString)
      finalArr.add(arr)
    }
    }
    writer.writeAll(finalArr)
    out.close()
  }

  def saveCountEventTypeActorAndRepoCsv(rdd: RDD[((String, Actor, Repo), Int)]) = {
    val out = new BufferedWriter(new FileWriter(ApplicationConfig.instance().getProperty(PropertyEnum.csvLocation) + "actorRepoAndTypeCount.csv"))
    val writer = new CSVWriter(out)
    val actorSchema = Array("(type", "(id", "login", "display_login", "gravatar_id", "url", "avatar_url)", "(id", "name", "url))", "count")

    val finalArr: java.util.List[Array[String]] = new util.ArrayList()
    finalArr.add(actorSchema)

    rdd.collect.foreach { case ((tipo: String, actor: Actor, repo: Repo), count) => {
      val arr = Array("(" + tipo, "(" + actor.id.toString(), actor.login, actor.display_login, actor.gravatar_id, actor.url, actor.avatar_url + ")", "(" + repo.id.toString(), repo.name, repo.url + "))", count.toString)
      finalArr.add(arr)
    }
    }
    writer.writeAll(finalArr)
    out.close()
  }

  def saveActorWithMaxEventCsv(rdd: RDD[(Actor, Int)]) = {
    val out = new BufferedWriter(new FileWriter(ApplicationConfig.instance().getProperty(PropertyEnum.csvLocation) + "actorMaxEvents.csv"))
    saveActorCount(out,rdd)
  }

  def saveActorWithMinEventCsv(rdd: RDD[(Actor, Int)]) = {
    val out = new BufferedWriter(new FileWriter(ApplicationConfig.instance().getProperty(PropertyEnum.csvLocation) + "actorMinEvents.csv"))
    saveActorCount(out,rdd)
  }

  def saveActorRepoAndHourWithMaxEventsCsv(rdd:RDD[((Actor, Repo, Int), Int)])={
    val out = new BufferedWriter(new FileWriter(ApplicationConfig.instance().getProperty(PropertyEnum.csvLocation) + "actorRepoAndHourMaxCount.csv"))
    val writer = new CSVWriter(out)
    val actorSchema = Array( "((id", "login", "display_login", "gravatar_id", "url", "avatar_url)", "(id", "name", "url)","hour)", "count")

    val finalArr: java.util.List[Array[String]] = new util.ArrayList()
    finalArr.add(actorSchema)

    rdd.collect.foreach { case (( actor: Actor, repo: Repo,hour), count) => {
      val arr = Array("((" + actor.id.toString(), actor.login, actor.display_login, actor.gravatar_id, actor.url, actor.avatar_url + ")", "(" + repo.id.toString(), repo.name, repo.url + ")",hour+")", count.toString)
      finalArr.add(arr)
    }
    }
    writer.writeAll(finalArr)
    out.close()
  }

  def saveActorRepoAndHourWithMinEventsCsv(rdd:RDD[((Actor, Repo, Int), Int)])={
    val out = new BufferedWriter(new FileWriter(ApplicationConfig.instance().getProperty(PropertyEnum.csvLocation) + "actorRepoAndHourMinCount.csv"))
    val writer = new CSVWriter(out)
    val actorSchema = Array( "((id", "login", "display_login", "gravatar_id", "url", "avatar_url)", "(id", "name", "url)","hour)", "count")

    val finalArr: java.util.List[Array[String]] = new util.ArrayList()
    finalArr.add(actorSchema)

    rdd.collect.foreach { case (( actor: Actor, repo: Repo,hour), count) => {
      val arr = Array("((" + actor.id.toString(), actor.login, actor.display_login, actor.gravatar_id, actor.url, actor.avatar_url + ")", "(" + repo.id.toString(), repo.name, repo.url + ")",hour+")", count.toString)
      finalArr.add(arr)
    }
    }
    writer.writeAll(finalArr)
    out.close()
  }

  def saveCountEventTypeActorAndRepoDsToCsv(ds: Dataset[((String, Actor, Repo), Int)]) = {
    val out = new BufferedWriter(new FileWriter(ApplicationConfig.instance().getProperty(PropertyEnum.csvLocation) + "repoCount.csv"))
    val writer = new CSVWriter(out)
    val actorSchema = Array("(type", "(id", "login", "display_login", "gravatar_id", "url", "avatar_url)", "(id", "name", "url))", "count")

    val finalArr: java.util.List[Array[String]] = new util.ArrayList()
    finalArr.add(actorSchema)

    ds.collect.foreach { case ((tipo: String, actor: Actor, repo: Repo), count) => {
      val arr = Array("(" + tipo, "(" + actor.id.toString(), actor.login, actor.display_login, actor.gravatar_id, actor.url, actor.avatar_url + ")", "(" + repo.id.toString(), repo.name, repo.url + "))", count.toString)
      finalArr.add(arr)
    }
    }
    writer.writeAll(finalArr)
    out.close()
  }


  private def saveActorCount(out: BufferedWriter, rdd: RDD[(Actor, Int)]) = {
    val writer = new CSVWriter(out)
    val actorSchema = Array("(id", "login", "display_login", "gravatar_id", "url", "avatar_url)", "count")

    val finalArr: java.util.List[Array[String]] = new util.ArrayList()
    finalArr.add(actorSchema)

    rdd.collect.foreach { case (actor: Actor, count) => {
      val arr = Array("(" + actor.id.toString(), actor.login, actor.display_login, actor.gravatar_id, actor.url, actor.avatar_url + ")", count.toString)
      finalArr.add(arr)
    }
    }
    writer.writeAll(finalArr)
    out.close()
  }
}
