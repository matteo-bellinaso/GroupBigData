package proprierties

import utility.PropertyEnum
import au.com.bytecode.opencsv.CSVWriter
import java.io.BufferedWriter
import java.io.FileWriter
import java.util

import entity.{Author, Repo, Actor}
import org.apache.spark.rdd.RDD
import properties.ApplicationConfig


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
    /*val out = new BufferedWriter(new FileWriter(ApplicationConfig.instance().getProperty(PropertyEnum.csvLocation) + "author.csv"))
    val writer = new CSVWriter(out)
    val authorSchema = Array("name", "email")

    val finalArr: java.util.List[Array[String]] = new util.ArrayList()
    finalArr.add( authorSchema)

    rdd.collect.foreach{ case (_ , row)=>{
      val arr = Array(row.name, row.email)
      finalArr.add(arr)
    }
    }
    writer.writeAll(finalArr)
    out.close()*/
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
    val writer = new CSVWriter(out)
    val actorSchema = Array("(id", "login", "display_login", "gravatar_id", "url", "avatar_url)", "count")

    val finalArr: java.util.List[Array[String]] = new util.ArrayList()
    finalArr.add(actorSchema)

    rdd.collect.foreach { case (actor, count) => {
      val arr = Array("(" + actor.id.toString(), actor.login, actor.display_login, actor.gravatar_id, actor.url, actor.avatar_url + ")", count.toString)
      finalArr.add(arr)
    }
    }
    writer.writeAll(finalArr)
    out.close()
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

}
